package e2e

// Contract retirement (unitdb/retire): the contract's primary client opens
// one persistent retirement by a generation; every node first refuses the
// contract's clients, credentials, publishes, subscriptions and relays, then
// closes its connections and removes its subscriptions, then deletes its
// messages, replicas, hints, dedup ids, session logs and revocation records;
// the retirement completes once every reachable node confirmed an empty
// scan. A node that was down, or the process crashed, finishes it when it is
// back; a repeated request opens no second retirement; another contract and
// ordinary traffic are untouched.

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/unit-io/unitdb/server/internal/message/security"
	"github.com/unit-io/unitdb/server/internal/types"
)

// retireStatus sends a unitdb/retire request on c and decodes its answer.
func retireStatus(t *testing.T, c *client, req types.RetireRequest) types.RetireResponse {
	t.Helper()
	answer := request(t, c, "retire", req)
	var resp types.RetireResponse
	if err := json.Unmarshal(answer, &resp); err != nil {
		t.Fatalf("retire answer %s: %v", answer, err)
	}
	return resp
}

// retireAdmin connects as the contract's primary client, the only id a
// retired contract keeps, opens the retirement with confirm, and returns its
// first answer and the connection for a follow-up.
func retireAdmin(t *testing.T, addr string, contract uint32) (*client, types.RetireResponse) {
	t.Helper()
	c, err := connectTo(t, addr, connectOpts{clientID: primaryClientID(contract)})
	if err != nil {
		t.Fatal(err)
	}
	resp := retireStatus(t, c, types.RetireRequest{Confirm: true})
	if resp.Status != 200 {
		t.Fatalf("retire: status %d (%s)", resp.Status, answerJSON(t, resp))
	}
	return c, resp
}

func answerJSON(t *testing.T, v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// adminQuery opens an admin connection of the retired contract and reads its
// retirement status. A failed attempt returns an empty answer for the
// polling loop to retry: the connection it replaces may be closed in the
// drain at the same moment.
func adminQuery(t *testing.T, addr string, contract uint32) types.RetireResponse {
	t.Helper()
	c, err := connectTo(t, addr, connectOpts{clientID: primaryClientID(contract)})
	if err != nil {
		return types.RetireResponse{}
	}
	defer c.close()
	b, err := json.Marshal(types.RetireRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.publish(0, "unitdb/retire", b, ""); err != nil {
		return types.RetireResponse{}
	}
	m, ok := c.waitTopic("unitdb/retire", 3*time.Second)
	if !ok {
		return types.RetireResponse{}
	}
	var resp types.RetireResponse
	if err := json.Unmarshal(m.Payload, &resp); err != nil {
		t.Fatalf("retire answer %s: %v", m.Payload, err)
	}
	return resp
}

// waitRetired polls until the retirement is done with no remaining resources
// on any node, or fails.
func waitRetired(t *testing.T, addr string, contract uint32) types.RetireResponse {
	t.Helper()
	var resp types.RetireResponse
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		resp = adminQuery(t, addr, contract)
		clean := len(resp.Remaining) == 0
		if clean {
			for _, n := range resp.Nodes {
				if len(n.Remaining) != 0 {
					clean = false
				}
			}
		}
		if resp.Done && clean {
			return resp
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("the retirement did not complete; last status: %s", answerJSON(t, resp))
	return resp
}

// TestRetireCluster retires a contract across a three-node cluster: it is
// barred everywhere, its primary keeps only unitdb/retire, its data is purged
// on every node, the retirement completes with an empty scan, a repeated
// request is the same generation, and another contract keeps serving.
func TestRetireCluster(t *testing.T) {
	c := startCluster(t, names...)
	if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	waitCapabilities()
	one, two, three := c.node("one"), c.node("two"), c.node("three")
	contract := uint32(0x7e5d0e01)
	other := contract + 1

	// A topic owned by each node, with a delivered message and a
	// subscription of the contract on each.
	topicOf := map[string]string{}
	user := newClientID(contract)
	for _, n := range c.nodes {
		topic := topicOwnedBy(n.name, contract, "groups.retire", names...)
		topicOf[n.name] = topic
		if !delivers(t, n, n, "u@e2e.test", contract, topic) {
			t.Fatalf("control: a message does not reach %s", n.name)
		}
	}
	// A reliable subscription, whose unanswered notification leaves a
	// session log for the purge.
	key := topicKey(contract, topicOf["two"], security.AllowReadWrite)
	s, err := connectTo(t, two.tcpAddr, connectOpts{clientID: user, sessKey: nextSess()})
	if err != nil {
		t.Fatal(err)
	}
	sid, _ := s.subscribe(1, keyed(key, topicOf["two"]))
	s.waitAck(sid, 3*time.Second)
	// The publisher on the same topic.
	p, err := connectTo(t, one.tcpAddr, connectOpts{clientID: user})
	if err != nil {
		t.Fatal(err)
	}
	p.publish(1, keyed(key, topicOf["two"]), encodePayload(1, "reliable"), "")
	time.Sleep(300 * time.Millisecond)

	// Open the retirement on one; the answer names the generation and the
	// nodes still to confirm.
	admin, first := retireAdmin(t, one.tcpAddr, contract)
	if first.Generation == 0 || first.Contract != contract || first.Done {
		t.Fatalf("first retire answer: %+v", first)
	}
	gen := first.Generation
	admin.close()

	// Within a moment every node refuses new connections, keys, publishes
	// and subscriptions of the contract.
	for _, n := range c.nodes {
		eventually(t, 10*time.Second, "a new client id is refused on "+n.name, func() bool {
			return !connects(t, n.server, newClientID(contract))
		})
	}
	if connects(t, two.server, user) {
		t.Fatal("an id issued before still connects after the retirement")
	}
	// The primary id still connects, for the retire entry alone.
	if !connects(t, three.server, primaryClientID(contract)) {
		t.Fatal("the primary id can no longer connect to read the retirement")
	}

	// Completion: every node through the purge, with nothing left.
	done := waitRetired(t, one.tcpAddr, contract)
	if done.Generation != gen {
		t.Errorf("generation changed: %d then %d", gen, done.Generation)
	}
	for _, n := range done.Nodes {
		if !n.Reachable || n.Phase != 3 || n.State != types.RetireDone || len(n.Remaining) != 0 {
			t.Errorf("node %s did not finish an empty purge: %+v", n.Node, n)
		}
	}

	// A repeated request opens no second retirement and reports the same one.
	again := adminQuery(t, one.tcpAddr, contract)
	if again.Generation != gen || !again.Done {
		t.Errorf("repeated retire: %+v", again)
	}

	// The retired primary's connection serves no data plane: keygen is
	// refused, and so is a publish.
	adm, err := connectTo(t, three.tcpAddr, connectOpts{clientID: primaryClientID(contract)})
	if err != nil {
		t.Fatal(err)
	}
	defer adm.close()
	if _, err := adm.publish(0, "unitdb/keygen", []byte(`[{"topic":"`+topicOf["three"]+`","type":"rw"}]`), ""); err != nil {
		t.Fatal(err)
	}
	if st := errorStatus(t, adm); st != types.ErrContractRetired.Status {
		t.Errorf("keygen for a retired contract: status %d", st)
	}
	if _, err := adm.publish(0, keyed(topicKey(contract, topicOf["three"], security.AllowReadWrite), topicOf["three"]), []byte("x"), ""); err != nil {
		t.Fatal(err)
	}
	if st := errorStatus(t, adm); st != types.ErrContractRetired.Status {
		t.Errorf("publish for a retired contract: status %d", st)
	}

	// Another contract is untouched: it connects and delivers on every node.
	for _, n := range c.nodes {
		topic := topicOwnedBy(n.name, other, "groups.other", names...)
		if !delivers(t, n, n, "v@e2e.test", other, topic) {
			t.Errorf("another contract stopped serving on %s", n.name)
		}
		if !connects(t, n.server, newClientID(other)) {
			t.Errorf("another contract's client was refused on %s", n.name)
		}
	}
}

// TestRetireStandalone retires a contract on a standalone server, kills the
// process right after the answer, and checks that on restart the contract is
// still barred, the purge resumes and completes, and the data stays gone.
func TestRetireStandalone(t *testing.T) {
	s := startServer(t)
	contract := uint32(0x7e5d0e02)
	topic := "groups.retire.standalone"
	if !keyedDelivers(t, s, newClientID(contract), topicKey(contract, topic, security.AllowReadWrite), topic) {
		t.Fatal("control: the contract serves before its retirement")
	}
	a, err := connectTo(t, s.tcpAddr, connectOpts{clientID: primaryClientID(contract)})
	if err != nil {
		t.Fatal(err)
	}
	first := retireStatus(t, a, types.RetireRequest{Confirm: true})
	if first.Generation == 0 {
		t.Fatalf("retire answer: %+v", first)
	}
	a.close()
	// SIGKILL, then restart: the resumption survives a crash.
	if err := s.restart(); err != nil {
		t.Fatalf("restart: %v\nlogs:\n%s", err, s.logs.String())
	}
	if connects(t, s, newClientID(contract)) {
		t.Fatal("the retired contract connects after a crash and restart")
	}
	// The primary still connects, for the retire entry alone.
	if !connects(t, s, primaryClientID(contract)) {
		t.Fatal("the primary id can no longer connect to read the retirement")
	}
	done := waitRetired(t, s.tcpAddr, contract)
	if !done.Done || done.Nodes[0].Phase != 3 || len(done.Nodes[0].Remaining) != 0 {
		t.Fatalf("the retirement did not resume to a clean finish: %+v", done)
	}
	if !keyedDelivers(t, s, newClientID(contract+1), topicKey(contract+1, topic, security.AllowReadWrite), topic) {
		t.Error("another contract stopped serving after the restart")
	}
}

// TestRetireOfflineNode retires a contract while a node is down: the
// reachable nodes complete their retirement and name the down node; the node
// enforces and purges when it is back and then confirms.
func TestRetireOfflineNode(t *testing.T) {
	c := startCluster(t, names...)
	if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	waitCapabilities()
	one, two, three := c.node("one"), c.node("two"), c.node("three")
	contract := uint32(0x7e5d0e03)
	topicThree := topicOwnedBy("three", contract, "groups.retire.offline", names...)
	if !delivers(t, three, two, "w@e2e.test", contract, topicThree) {
		t.Fatal("control: a message does not reach the node that will go down")
	}
	three.stop()
	admin, first := retireAdmin(t, one.tcpAddr, contract)
	if first.Generation == 0 {
		t.Fatalf("retire answer: %+v", first)
	}
	admin.close()

	// The reachable nodes finish; three is named unreachable.
	var resp types.RetireResponse
	eventually(t, 20*time.Second, "the reachable nodes complete the retirement", func() bool {
		resp = adminQuery(t, two.tcpAddr, contract)
		if !resp.Done {
			return false
		}
		var blocked bool
		for _, n := range resp.Nodes {
			if n.Node == "three" {
				blocked = !n.Reachable
			} else if !n.Reachable || n.Phase != 3 {
				return false
			}
		}
		return blocked
	})
	if !connects(t, two.server, newClientID(contract)) {
		// Expected: the reachable nodes barred the contract.
	} else {
		t.Error("a client connects to a reachable node after the retirement")
	}

	// Bring the down node back: it enforces at once and completes its purge.
	if err := three.start(); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "the returning node refuses the contract", func() bool {
		return !connects(t, three.server, newClientID(contract))
	})
	eventually(t, 30*time.Second, "the returning node confirms an empty purge", func() bool {
		d := adminQuery(t, one.tcpAddr, contract)
		for _, n := range d.Nodes {
			if n.Node == "three" {
				return n.Reachable && n.Phase == 3 && len(n.Remaining) == 0
			}
		}
		return false
	})
	// Its held data never serves again.
	if keyedDelivers(t, three.server, newClientID(contract), topicKey(contract, topicThree, security.AllowReadWrite), topicThree) {
		t.Error("the returning node serves the retired contract's topic")
	}
}

// TestRetireMixedCluster retires a contract in a cluster with a node that
// does not support retirements: the capable nodes bar, drain and purge, but
// the retirement cannot complete while the old node is reachable, and the
// status names it as the blocker. When the old node is the client's node, it
// still serves (it was never told); that is the documented rolling-deploy
// limit.
func TestRetireMixedCluster(t *testing.T) {
	// Nodes of an earlier version that does not know the retire capability.
	older := []string{"UNITDB_CLUSTER_CAPS=replicate,deliver,sessions,resync,service,v2keys,revocations"}
	c := startClusterWith(t, clusterOpts{env: map[string][]string{"one": older}}, names...)
	if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	waitCapabilities()
	one, two := c.node("one"), c.node("two")
	contract := uint32(0x7e5d0e05)

	// Opened from a capable node.
	admin, first := retireAdmin(t, two.tcpAddr, contract)
	if first.Status != 200 {
		t.Fatalf("retire: %d", first.Status)
	}
	admin.close()

	// The capable nodes eventually refuse everything of the contract.
	for _, n := range []*clusterNode{two, c.node("three")} {
		eventually(t, 15*time.Second, n.name+" refuses the contract", func() bool {
			return !connects(t, n.server, newClientID(contract))
		})
	}

	// The capable nodes purge; the old node blocks completion.
	blockerOK := false
	deadline := time.Now().Add(25 * time.Second)
	var last types.RetireResponse
	for time.Now().Before(deadline) {
		st := adminQuery(t, two.tcpAddr, contract)
		last = st
		if st.Done {
			t.Fatalf("the retirement completed with an incapable node: %s", answerJSON(t, st))
		}
		phases := map[string]int{}
		for _, n := range st.Nodes {
			phases[n.Node] = n.Phase
			if n.Node == "one" && n.Reachable && n.State == "unsupported" {
				blockerOK = true
			}
		}
		if phases["two"] == 3 && phases["three"] == 3 {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Logf("last status: %s", answerJSON(t, last))
	if !blockerOK {
		t.Error("the incapable node was not named as the blocker")
	}
	// Give completion a few ticks it must not take.
	time.Sleep(2 * time.Second)
	if st := adminQuery(t, two.tcpAddr, contract); st.Done {
		t.Error("the retirement completed despite the reachable old node")
	}

	// The old node was never sent the retirement: its clients still connect.
	// Upgrading it lets the cluster finish; that is the rolling-deploy path.
	if !connects(t, one.server, newClientID(contract)) {
		t.Error("the old node refused a contract it was never told retired")
	}
}

// TestRetireUnauthorized checks that only the contract's primary client can
// open a retirement, and an unknown one cannot be queried.
func TestRetireUnauthorized(t *testing.T) {
	s := startServer(t)
	contract := uint32(0x7e5d0e04)
	user, err := connectTo(t, s.tcpAddr, connectOpts{clientID: newClientID(contract)})
	if err != nil {
		t.Fatal(err)
	}
	if st := retireStatus(t, user, types.RetireRequest{Confirm: true}).Status; st != types.ErrForbidden.Status {
		t.Errorf("a secondary client retires its contract: status %d", st)
	}
	if st := retireStatus(t, user, types.RetireRequest{}).Status; st != types.ErrForbidden.Status {
		t.Errorf("a secondary client reads a retirement: status %d", st)
	}
	// Status of a retirement that does not exist, queried by the primary, is
	// not found.
	primary, err := connectTo(t, s.tcpAddr, connectOpts{clientID: primaryClientID(contract)})
	if err != nil {
		t.Fatal(err)
	}
	if st := retireStatus(t, primary, types.RetireRequest{}).Status; st != types.ErrNotFound.Status {
		t.Errorf("an unknown retirement's status: %d", st)
	}
	if st := retireStatus(t, primary, types.RetireRequest{Confirm: true}).Status; st != 200 {
		t.Errorf("the primary client cannot retire its contract: status %d", st)
	}
	waitRetired(t, s.tcpAddr, contract)
}
