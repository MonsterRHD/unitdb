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

package internal

import (
	"errors"
	"net"
	"net/rpc"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/unit-io/unitdb/server/internal/store"
)

var txnRPCPort int32 = 43000

// txnRPCCluster builds two cluster objects, node-a and node-b, on one ring,
// with a real net/rpc connection from a to b serving b's Cluster methods.
// Both share the package test store here; namespaced txn records do not
// collide. The caller owns the returned clusters' store effects.
func txnRPCCluster(t *testing.T, capB []string) (ca, cb *Cluster, cleanup func()) {
	t.Helper()
	port := atomic.AddInt32(&txnRPCPort, 1)
	addr := "127.0.0.1:" + strconv.Itoa(int(port))
	l, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	cb = &Cluster{thisNodeName: "node-b", replicas: 2}
	srv := rpc.NewServer()
	if err := srv.RegisterName("Cluster", cb); err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go srv.ServeConn(c)
		}
	}()

	n := &ClusterNode{name: "node-b", address: addr, connected: true, done: make(chan bool, 1)}
	endpoint, watched, err := n.dial()
	if err != nil {
		t.Fatal(err)
	}
	n.endpoint = endpoint
	n.caps = peerCapabilities{known: &NodeCapabilities{Version: clusterProtocolVersion, Capabilities: capB}}

	ca = &Cluster{
		thisNodeName: "node-a",
		replicas:     2,
		nodes:        map[string]*ClusterNode{"node-b": n},
	}
	_ = watched

	ring := newRing(latestRingVersion, []string{"node-a", "node-b"})
	for _, c := range []*Cluster{ca, cb} {
		c.ringMu.Lock()
		c.ring = ring
		c.fullRing = ring
		c.ringVersion = latestRingVersion
		c.ringNodes = []string{"node-a", "node-b"}
		c.ringMu.Unlock()
	}
	return ca, cb, func() {
		endpoint.Close()
		watched.Close()
		l.Close()
	}
}

func txnOwnerReq(cl *Cluster, batch string, contract uint32, topics []string, wait bool) *TxnPrepareReq {
	entries := make([]TxnEntry, len(topics))
	for i, topic := range topics {
		entries[i] = TxnEntry{Topic: topic, Payload: []byte("p" + strconv.Itoa(i)), Ttl: "", ReplicaID: replicaID(batch, i)}
	}
	return &TxnPrepareReq{
		Node:         "node-a",
		BatchID:      batch,
		Contract:     contract,
		Origin:       ClusterSess{Node: "node-a", ConnID: 1, SessID: 2},
		MessageID:    1,
		DeliveryMode: 1,
		ContentHash:  []byte("hash-" + batch),
		RingVersion:  latestRingVersion,
		PinOwner:     "node-a",
		WaitReplica:  wait,
		Entries:      entries,
	}
}

// TestClusterTxnOwnerPrepareCommitIdempotent: an owner stages, fences,
// commits once and answers re-prepare/re-commit idempotently.
func TestClusterTxnOwnerPrepareCommitIdempotent(t *testing.T) {
	ca, _, cleanup := txnRPCCluster(t, allCapabilities)
	defer cleanup()
	const contract = uint32(0x77030001)
	topic := topicOwnedBy(ca, contract, "node-a")
	const batch = "77030001.2.1"
	req := txnOwnerReq(ca, batch, contract, []string{topic}, false)

	var resp TxnPrepareResp
	if err := ca.TxnPrepare(req, &resp); err != nil || resp.State != 1 || resp.Moved {
		t.Fatalf("prepare: %+v err=%v", resp, err)
	}
	if st, _ := store.Txn.State(batch); st.State != store.TxnPrepared {
		t.Fatalf("state after prepare=%d", st.State)
	}
	// Idempotent re-prepare.
	if err := ca.TxnPrepare(req, &resp); err != nil || resp.State != 1 {
		t.Fatalf("re-prepare: %+v err=%v", resp, err)
	}
	// Commit.
	var cresp TxnCommitResp
	if err := ca.TxnCommit(&TxnCommitReq{Node: "node-a", BatchID: batch, PinOwner: "node-a"}, &cresp); err != nil || !cresp.Committed || cresp.Moved {
		t.Fatalf("commit: %+v err=%v", cresp, err)
	}
	assertHistoryLen(t, contract, topic, 1)
	// Re-commit stores nothing again.
	if err := ca.TxnCommit(&TxnCommitReq{Node: "node-a", BatchID: batch, PinOwner: "node-a"}, &cresp); err != nil || !cresp.Committed {
		t.Fatalf("re-commit: %+v err=%v", cresp, err)
	}
	assertHistoryLen(t, contract, topic, 1)
	// A committed batch cannot be aborted.
	var aresp TxnAbortResp
	if err := ca.TxnAbort(&TxnAbortReq{BatchID: batch, Reason: "x"}, &aresp); err != nil || !aresp.Committed {
		t.Fatalf("abort committed: %+v err=%v", aresp, err)
	}
}

// TestClusterTxnPrepareFenceRejectsMoved: prepare on a non-owner is a move
// without staging anything.
func TestClusterTxnPrepareFenceRejectsMoved(t *testing.T) {
	ca, _, cleanup := txnRPCCluster(t, allCapabilities)
	defer cleanup()
	const contract = uint32(0x77030002)
	topic := topicOwnedBy(ca, contract, "node-b")
	const batch = "77030002.2.2"
	req := txnOwnerReq(ca, batch, contract, []string{topic}, false)

	var resp TxnPrepareResp
	if err := ca.TxnPrepare(req, &resp); err != nil {
		t.Fatal(err)
	}
	if !resp.Moved || resp.Owner != "node-b" {
		t.Fatalf("expect moved to node-b, got %+v", resp)
	}
	if st, _ := store.Txn.State(batch); st.State != store.TxnUnknown {
		t.Fatalf("a moved prepare staged: %d", st.State)
	}
	assertHistoryLen(t, contract, topic, 0)
}

// TestClusterTxnContentMismatch: reusing a batch id with other content fails.
func TestClusterTxnContentMismatch(t *testing.T) {
	ca, _, cleanup := txnRPCCluster(t, allCapabilities)
	defer cleanup()
	const contract = uint32(0x77030003)
	topic := topicOwnedBy(ca, contract, "node-a")
	const batch = "77030003.2.3"
	req := txnOwnerReq(ca, batch, contract, []string{topic}, false)
	var resp TxnPrepareResp
	if err := ca.TxnPrepare(req, &resp); err != nil {
		t.Fatal(err)
	}
	req.ContentHash = []byte("different")
	if err := ca.TxnPrepare(req, &resp); !errors.Is(err, errTxnMismatch) {
		t.Fatalf("mismatch err=%v, want errTxnMismatch", err)
	}
}

// TestClusterTxnReplicaStageAndCommit: a reliable batch stages a hidden
// replica copy at prepare, promotes it at commit, and never repeats it.
func TestClusterTxnReplicaStageAndCommit(t *testing.T) {
	ca, cb, cleanup := txnRPCCluster(t, allCapabilities)
	defer cleanup()
	const contract = uint32(0x77030004)
	topic := topicOwnedBy(ca, contract, "node-a")
	const batch = "77030004.2.4"
	req := txnOwnerReq(ca, batch, contract, []string{topic}, true)

	var resp TxnPrepareResp
	if err := ca.TxnPrepare(req, &resp); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	// The replica staged it invisibly.
	if st, _ := store.Txn.ReplicaState(batch); st.State != store.TxnPrepared {
		t.Fatalf("replica state=%d, want prepared", st.State)
	}
	// Hidden while prepared: no replica-namespace copy yet.
	entries, _ := store.Message.History(contract, topic)
	if len(entries) != 0 {
		t.Fatalf("replica copy visible before commit: %d", len(entries))
	}
	var cresp TxnCommitResp
	if err := ca.TxnCommit(&TxnCommitReq{BatchID: batch, PinOwner: "node-a"}, &cresp); err != nil || !cresp.Committed {
		t.Fatalf("commit: %+v err=%v", cresp, err)
	}
	// The replica promoted it and recorded the deterministic id.
	if st, _ := store.Txn.ReplicaState(batch); st.State != store.TxnCommitted {
		t.Fatalf("replica state=%d, want committed", st.State)
	}
	// Re-commit does not duplicate the replica copy.
	if err := ca.TxnCommit(&TxnCommitReq{BatchID: batch, PinOwner: "node-a"}, &cresp); err != nil {
		t.Fatal(err)
	}
	_ = cb
}

// TestClusterTxnNewOwnerResolvesCommittedOldOwner: a new owner that receives
// a prepare pinned at the previous (live) owner learns it committed and
// answers "already committed" without staging its own copy.
func TestClusterTxnNewOwnerResolvesCommittedOldOwner(t *testing.T) {
	ca, _, cleanup := txnRPCCluster(t, allCapabilities)
	defer cleanup()
	const contract = uint32(0x77030005)
	topic := topicOwnedBy(ca, contract, "node-a")
	const batch = "77030005.2.5"

	// Seed the batch as committed "on node-b" through its own handler.
	oldReq := txnOwnerReq(ca, batch, contract, []string{topic}, false)
	oldReq.Node = "node-b"
	oldReq.PinOwner = "node-b"
	oldTopic := topicOwnedBy(ca, contract, "node-b")
	oldReq.Entries[0].Topic = oldTopic
	var oldResp TxnPrepareResp
	if err := ca.nodes["node-b"].endpoint.Call("Cluster.TxnPrepare", oldReq, &oldResp); err != nil {
		t.Fatal(err)
	}
	if oldResp.State != 1 {
		t.Fatalf("old owner prepare: %+v", oldResp)
	}
	var oldCommit TxnCommitResp
	if err := ca.nodes["node-b"].endpoint.Call("Cluster.TxnCommit", &TxnCommitReq{Node: "node-b", BatchID: batch, PinOwner: "node-b"}, &oldCommit); err != nil {
		t.Fatal(err)
	}

	// node-a receives the retry pinned at node-b; the topic currently pins
	// node-a, so node-a is the new owner and asks node-b first.
	newReq := txnOwnerReq(ca, batch, contract, []string{topic}, false)
	newReq.PinOwner = "node-b"
	var newResp TxnPrepareResp
	if err := ca.TxnPrepare(newReq, &newResp); err != nil {
		t.Fatal(err)
	}
	if newResp.State != 2 {
		t.Fatalf("new owner learned state=%d, want 2 (committed)", newResp.State)
	}
}

// TestClusterTxnCapabilityFallback: an owner without capTxnPublish drives the
// coordinator participant to the legacy path.
func TestClusterTxnCapabilityFallback(t *testing.T) {
	ca, _, cleanup := txnRPCCluster(t, nil) // node-b reports no capabilities
	defer cleanup()
	const contract = uint32(0x77030006)
	topic := topicOwnedBy(ca, contract, "node-b")
	const batch = "77030006.2.6"
	msgs := []*validatedMsg{txnValidated(topic, "x")}
	msgs[0].index = 0
	p, err := newRemoteTxnParticipant(remoteParticipantParams{
		svc: Globals.Service, cluster: ca,
		origin:       store.TxnOrigin{Node: "node-a"},
		batchID:      batch,
		hash:         []byte("h"),
		contract:     contract,
		messageID:    6,
		deliveryMode: 1,
		ringVersion:  latestRingVersion,
		waitReplica:  false,
		node:         "node-b",
		msgs:         msgs,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.prepare(); !errors.Is(err, errTxnUseLegacy) {
		t.Fatalf("prepare on an incapable owner: %v, want errTxnUseLegacy", err)
	}
}

// TestClusterTxnPrepareWithoutCapableReplicaDegrades: a reliable batch whose
// only other holder cannot take a staged copy still prepares and commits on
// the owner, as a put outside a transaction does - it is never refused.
func TestClusterTxnPrepareWithoutCapableReplicaDegrades(t *testing.T) {
	ca, _, cleanup := txnRPCCluster(t, nil) // node-b advertises no capabilities
	defer cleanup()
	const contract = uint32(0x77030008)
	topic := topicOwnedBy(ca, contract, "node-a")
	const batch = "77030008.2.8"
	req := txnOwnerReq(ca, batch, contract, []string{topic}, true)

	var resp TxnPrepareResp
	if err := ca.TxnPrepare(req, &resp); err != nil || resp.State != 1 {
		t.Fatalf("prepare with no capable replica: %+v err=%v", resp, err)
	}
	st, _ := store.Txn.State(batch)
	if st.State != store.TxnPrepared || st.ChosenReplica != "" {
		t.Fatalf("owner staging: state=%d chosenReplica=%q, want prepared with no replica", st.State, st.ChosenReplica)
	}
	var cresp TxnCommitResp
	if err := ca.TxnCommit(&TxnCommitReq{BatchID: batch, PinOwner: "node-a"}, &cresp); err != nil || !cresp.Committed {
		t.Fatalf("commit: %+v err=%v", cresp, err)
	}
	assertHistoryLen(t, contract, topic, 1)
}

// TestClusterTxnReplicaCommitDedupsDelivered: when regular replication (or a
// hint handoff) puts the deterministic id into the replica namespace before
// the staged copy is promoted, the commit only unstages it and never writes a
// second copy.
func TestClusterTxnReplicaCommitDedupsDelivered(t *testing.T) {
	ca, cb, cleanup := txnRPCCluster(t, allCapabilities)
	defer cleanup()
	const contract = uint32(0x77030009)
	topic := topicOwnedBy(ca, contract, "node-a")
	const batch = "77030009.2.9"
	rid := replicaID(batch, 0)
	payload := []byte("replica-once")

	var prepResp TxnReplicaResp
	if err := cb.TxnReplicaPrepare(&TxnReplicaReq{
		Node:        "node-a",
		BatchID:     batch,
		Contract:    contract,
		ContentHash: []byte("h9"),
		Entries:     []TxnEntry{{Topic: topic, Payload: payload, ReplicaID: rid}},
	}, &prepResp); err != nil || !prepResp.Prepared {
		t.Fatalf("replica prepare: %+v err=%v", prepResp, err)
	}
	if entries, _ := store.Message.History(contract, topic); len(entries) != 0 {
		t.Fatalf("a staged copy is visible before commit: %d", len(entries))
	}

	// Regular replication delivers the same id while the batch is prepared.
	var unused bool
	if err := cb.Replicate(&ReplicateReq{Node: "node-a", Entries: []ReplicaEntry{
		{ID: rid, Contract: contract, Topic: topic, Payload: payload},
	}}, &unused); err != nil {
		t.Fatal(err)
	}
	if entries, _ := store.Message.History(contract, topic); len(entries) != 1 {
		t.Fatalf("replicated entries=%d, want 1", len(entries))
	}

	// Commit promotes nothing a second time.
	var commitResp TxnReplicaResp
	if err := cb.TxnReplicaCommit(&TxnReplicaReq{Node: "node-a", BatchID: batch, Contract: contract}, &commitResp); err != nil || !commitResp.Committed {
		t.Fatalf("replica commit: %+v err=%v", commitResp, err)
	}
	if entries, _ := store.Message.History(contract, topic); len(entries) != 1 {
		t.Fatalf("entries after commit=%d, want exactly 1", len(entries))
	}
	if st, _ := store.Txn.ReplicaState(batch); st.State != store.TxnCommitted {
		t.Fatalf("replica state=%d, want committed", st.State)
	}
}

// TestClusterTxnReplicaCommitPartiallyDelivered: a two-entry batch of which
// only one entry already arrived through replication is still staged in full;
// the commit promotes just the missing entry, and each topic ends with one
// copy.
func TestClusterTxnReplicaCommitPartiallyDelivered(t *testing.T) {
	ca, cb, cleanup := txnRPCCluster(t, allCapabilities)
	defer cleanup()
	const contract = uint32(0x7703000a)
	topic1 := topicOwnedBy(ca, contract, "node-a")
	topic2 := anotherTopicOwnedBy(ca, contract, "node-a", topic1)
	const batch = "7703000a.2.a"
	rid1, rid2 := replicaID(batch, 0), replicaID(batch, 1)
	payload := []byte("p")

	var prepResp TxnReplicaResp
	if err := cb.TxnReplicaPrepare(&TxnReplicaReq{
		Node:        "node-a",
		BatchID:     batch,
		Contract:    contract,
		ContentHash: []byte("ha"),
		Entries: []TxnEntry{
			{Topic: topic1, Payload: payload, ReplicaID: rid1},
			{Topic: topic2, Payload: payload, ReplicaID: rid2},
		},
	}, &prepResp); err != nil || !prepResp.Prepared {
		t.Fatalf("replica prepare: %+v err=%v", prepResp, err)
	}

	// Only entry 1 arrives the regular way before the commit.
	var unused bool
	if err := cb.Replicate(&ReplicateReq{Node: "node-a", Entries: []ReplicaEntry{
		{ID: rid2, Contract: contract, Topic: topic2, Payload: payload},
	}}, &unused); err != nil {
		t.Fatal(err)
	}

	var commitResp TxnReplicaResp
	if err := cb.TxnReplicaCommit(&TxnReplicaReq{Node: "node-a", BatchID: batch, Contract: contract}, &commitResp); err != nil || !commitResp.Committed {
		t.Fatalf("replica commit: %+v err=%v", commitResp, err)
	}
	if entries, _ := store.Message.History(contract, topic1); len(entries) != 1 {
		t.Fatalf("promoted-only entry: %d copies, want 1", len(entries))
	}
	if entries, _ := store.Message.History(contract, topic2); len(entries) != 1 {
		t.Fatalf("pre-delivered entry: %d copies, want 1", len(entries))
	}
}

// anotherTopicOwnedBy returns another topic owned by owner, different from
// notIt.
func anotherTopicOwnedBy(cl *Cluster, contract uint32, owner, notIt string) string {
	for {
		topic := txnTopicName()
		if topic == notIt {
			continue
		}
		if cl.getRing().Get(topicRingKey(contract, topic)) == owner {
			return topic
		}
	}
}
