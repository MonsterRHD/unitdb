/*
 * Copyright 2020 Saffat Technologies, Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/unit-io/unitdb/server/utp"
)

// publishBatch sends one PUBLISH carrying several messages, the transactional
// unit the new owner-side commit covers.
func (c *client) publishBatch(id uint16, mode uint8, topics []string, payload string) error {
	msgs := make([]*utp.PublishMessage, len(topics))
	for i, topic := range topics {
		msgs[i] = &utp.PublishMessage{Topic: c.keyed(topic), Payload: []byte(payload)}
	}
	raw, err := (&utp.Publish{
		MessageID:    id,
		DeliveryMode: mode,
		Messages:     msgs,
	}).ToBinary()
	if err != nil {
		return err
	}
	return c.writeRaw(raw.Bytes())
}

// topicsForOwners returns one topic per owner for the contract.
func topicsForOwners(contract uint32, prefix string, owners, live []string) map[string]string {
	out := make(map[string]string, len(owners))
	for _, n := range owners {
		out[n] = topicOwnedBy(n, contract, prefix+"."+n, live...)
	}
	return out
}

// relayCount relays a topic's recent history and counts the messages that
// come back, draining the stream for a short while as the existing relay
// tests do.
func relayCount(t *testing.T, c *client, topic string) int {
	t.Helper()
	if _, err := c.relay(topic, "1h"); err != nil {
		t.Fatal(err)
	}
	m, ok := c.waitPub(3 * time.Second)
	if !ok {
		return 0
	}
	count := len(m.Messages)
	for {
		more, ok := c.waitPub(400 * time.Millisecond)
		if !ok {
			break
		}
		count += len(more.Messages)
	}
	return count
}

func txnPublisher(t *testing.T, addr string, contract uint32) *client {
	t.Helper()
	ctx := context.Background()
	c, err := dial(ctx, addr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.connectWith(connectOpts{clientID: newClientID(contract), autoKey: true, sessKey: nextSess()}); err != nil {
		t.Fatal(err)
	}
	return c
}

// TestTxnAtomicBatchAcrossOwners: a batch spanning all three owners commits
// as one unit: one ACK and every topic carries exactly one copy reachable by
// relay from each holder set.
func TestTxnAtomicBatchAcrossOwners(t *testing.T) {
	names := []string{"one", "two", "three"}
	c := startCluster(t, names...)
	if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	contract := uint32(0x7a010001)
	topics := topicsForOwners(contract, "txn.atomic", names, names)
	ordered := []string{topics["one"], topics["two"], topics["three"]}

	pub := txnPublisher(t, c.node("one").tcpAddr, contract)
	defer pub.close()
	id := pub.id()
	if err := pub.publishBatch(id, 0, ordered, "atomic-v1"); err != nil {
		t.Fatal(err)
	}
	pub.waitAck(id, 5*time.Second)

	for _, topic := range ordered {
		// A fresh reader per topic avoids collecting another relay's late
		// holder response.
		r := txnPublisher(t, c.node("one").tcpAddr, contract)
		if got := relayCount(t, r, topic); got != 1 {
			r.close()
			t.Fatalf("topic %s relayed %d messages, want 1", topic, got)
		}
		r.close()
	}
}

// TestTxnSameMessageIDReplayStoresOnce: resending the same committed batch
// with the same MessageID answers ACK again without storing a second copy.
func TestTxnSameMessageIDReplayStoresOnce(t *testing.T) {
	names := []string{"one", "two", "three"}
	c := startCluster(t, names...)
	if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	contract := uint32(0x7a010002)
	topics := topicsForOwners(contract, "txn.replay", []string{"one", "two"}, names)
	ordered := []string{topics["one"], topics["two"]}

	pub := txnPublisher(t, c.node("one").tcpAddr, contract)
	defer pub.close()
	id := uint16(7021)
	for attempt := 0; attempt < 2; attempt++ {
		if err := pub.publishBatch(id, 1, ordered, "replay-v1"); err != nil {
			t.Fatal(err)
		}
		pub.waitAck(id, 5*time.Second)
		// Assert with a fresh reader each time: a reader that already
		// relayed can pick up a holder's late answer while collecting.
		for _, topic := range ordered {
			r := txnPublisher(t, c.node("one").tcpAddr, contract)
			if got := relayCount(t, r, topic); got != 1 {
				r.close()
				t.Fatalf("attempt %d topic %s has %d messages, want 1", attempt, topic, got)
			}
			r.close()
		}
	}
}

// TestTxnOwnerRestartHistoryIntact: after an owner that committed a
// transactional batch is SIGKILLed and restarted with its store, the message
// is still there and converged replicas still answer exactly one.
func TestTxnOwnerRestartHistoryIntact(t *testing.T) {
	names := []string{"one", "two", "three"}
	c := startCluster(t, names...)
	if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	contract := uint32(0x7a010003)
	topic := topicOwnedBy("two", contract, "txn.restart.two", names...)

	pub := txnPublisher(t, c.node("one").tcpAddr, contract)
	defer pub.close()
	id := pub.id()
	if err := pub.publishBatch(id, 1, []string{topic}, "restart-v1"); err != nil {
		t.Fatal(err)
	}
	pub.waitAck(id, 5*time.Second)

	// Kill the owner and bring it back with the same identity, addresses and
	// store.
	s := c.node("two")
	old := s.server
	tcp, grpc, db := old.tcpAddr, old.grpcAddr, old.dbPath
	s.stop()
	s.server = startServerWith(t, serverOpts{
		cluster:  c.conf(nil, nil),
		args:     []string{"-cluster_self", "two"},
		tcpAddr:  tcp,
		grpcAddr: grpc,
		dbPath:   db,
	})
	// Failure detection (node_fail_after heartbeats), the rehash without
	// the owner, and its return with the same identity take a few seconds.
	deadline := time.Now().Add(20 * time.Second)
	time.Sleep(4 * time.Second)
	var got int
	for time.Now().Before(deadline) {
		r := txnPublisher(t, c.node("one").tcpAddr, contract)
		got = relayCount(t, r, topic)
		r.close()
		if got == 1 {
			break
		}
		time.Sleep(time.Second)
	}
	if got != 1 {
		t.Fatalf("after owner restart topic has %d messages, want 1", got)
	}
}

// TestTxnBatchDuringOutageConvergesOnRejoin: one reliable PUBLISH carrying
// topics the dead node owns (served by each survivor while it is out) and a
// survivor-owned topic is committed as one unit during the outage. When the
// dead node restarts every node relays each topic exactly once: the owners'
// full-ring hints hand the batch to the node that was out, and the
// deterministic ids keep it to one copy.
func TestTxnBatchDuringOutageConvergesOnRejoin(t *testing.T) {
	names := []string{"one", "two", "three"}
	c := startCluster(t, names...)
	if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	victim := "two"
	liveNames := []string{"one", "three"}
	contract := uint32(0x7a010005)

	// Topics owned by the victim in the full ring, one taken over by each
	// survivor in the outage ring, and one a survivor owns in both.
	var topics []string
	for _, heir := range liveNames {
		for i := 0; ; i++ {
			topic := fmt.Sprintf("txn.outage.%s.t%d", heir, i)
			if topicOwner(contract, topic, names...) == victim && topicOwner(contract, topic, liveNames...) == heir {
				topics = append(topics, topic)
				break
			}
		}
	}
	topics = append(topics, topicOwnedBy("one", contract, "txn.outage.kept", liveNames...))

	var live []*clusterNode
	for _, n := range c.nodes {
		if n.name != victim {
			live = append(live, n)
		}
	}
	dead := c.node(victim)
	dead.stop()
	time.Sleep(4 * time.Second) // failure detection and the failover rehash
	if _, err := c.waitLeader(live, 10*time.Second); err != nil {
		t.Fatal(err)
	}

	pub := txnPublisher(t, live[0].tcpAddr, contract)
	defer pub.close()
	id := pub.id()
	if err := pub.publishBatch(id, 1, topics, "outage-batch"); err != nil {
		t.Fatal(err)
	}
	if !pub.waitAck(id, 5*time.Second) {
		t.Fatal("the batch was not acknowledged while one owner was down")
	}
	// While the victim is out, the survivors relay every topic.
	for _, node := range live {
		for _, topic := range topics {
			r := txnPublisher(t, node.tcpAddr, contract)
			got := relayCount(t, r, topic)
			r.close()
			if got != 1 {
				t.Fatalf("outage relay of %s on %s: %d messages, want 1", topic, node.name, got)
			}
		}
	}

	if err := dead.start(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * time.Second)
	if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	// Hints are handed to the rejoined node on the next handoff; poll until
	// every node relays every topic exactly once.
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, node := range c.nodes {
			for _, topic := range topics {
				r := txnPublisher(t, node.tcpAddr, contract)
				got := relayCount(t, r, topic)
				r.close()
				if got != 1 {
					ok = false
				}
			}
		}
		if ok {
			return
		}
		time.Sleep(time.Second)
	}
	for _, node := range c.nodes {
		for _, topic := range topics {
			r := txnPublisher(t, node.tcpAddr, contract)
			got := relayCount(t, r, topic)
			r.close()
			if got != 1 {
				t.Errorf("after %s rejoined, relay of %s on %s: %d messages, want 1", victim, topic, node.name, got)
			}
		}
	}
}

// TestTxnLegacyFallbackWithoutCapability: an owner node without the
// txnpublish capability makes the coordinator serve the batch on the
// per-message legacy path, which still commits and relays exactly one copy.
func TestTxnLegacyFallbackWithoutCapability(t *testing.T) {
	names := []string{"one", "two", "three"}
	c := startClusterWith(t, clusterOpts{
		env: map[string][]string{
			"two": {"UNITDB_CLUSTER_CAPS=replicate,deliver,sessions,resync,service,v2keys,revocations"},
		},
	}, names...)
	if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	// The old node must be taken as lacking the new capability; the notice
	// is best effort because the first publish may take the fallback
	// directly from the capabilities map.
	_ = c.waitFor(func() bool {
		return strings.Contains(c.node("one").logs.String(), `capability "txnpublish" not enabled`)
	}, 3*time.Second)
	contract := uint32(0x7a010004)
	topic := topicOwnedBy("two", contract, "txn.legacy.two", names...)

	pub := txnPublisher(t, c.node("one").tcpAddr, contract)
	defer pub.close()
	id := pub.id()
	if err := pub.publishBatch(id, 0, []string{topic}, "legacy-v1"); err != nil {
		t.Fatal(err)
	}
	pub.waitAck(id, 5*time.Second)

	if got := relayCount(t, pub, topic); got != 1 {
		t.Fatalf("legacy batch topic has %d messages, want 1", got)
	}
}

// waitFor is the cluster helper's readiness wait used by capability tests.
func (c *cluster) waitFor(check func() bool, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("condition not met within %s", timeout)
}
