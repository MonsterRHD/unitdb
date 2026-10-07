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

package store_test

import (
	"bytes"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/unit-io/unitdb/server/internal/store"
)

// txnContract is the contract the transaction tests stage batches under.
const txnContract uint32 = 0x775a0001

// newTxnStore opens a fresh unsealed store in a temp directory, closed when
// the test ends.
func newTxnStore(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	store.ForgetTopicsForTest()
	if err := store.Open(dir, storeConf, false); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if store.IsOpen() {
			store.Close()
		}
	})
	return dir
}

// reopenTxnStore closes the store and opens the same directory again, as a
// restarted server does.
func reopenTxnStore(t *testing.T, dir string) {
	t.Helper()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store.ForgetTopicsForTest()
	if err := store.Open(dir, storeConf, false); err != nil {
		t.Fatal(err)
	}
}

func txnBatch(batchID string, topics ...string) *store.TxnRecord {
	msgs := make([]store.TxnMessage, len(topics))
	for i, topic := range topics {
		msgs[i] = store.TxnMessage{Topic: topic, Ttl: ""}
	}
	return &store.TxnRecord{
		BatchID:  batchID,
		Contract: txnContract,
		Origin: store.TxnOrigin{
			Node:   "node-a",
			SessID: 42,
		},
		MessageID:   7,
		ContentHash: []byte("hash-of-" + batchID),
		Messages:    msgs,
	}
}

func txnPayloads(n int, marker string) [][]byte {
	payloads := make([][]byte, n)
	for i := range payloads {
		b := bytes.Repeat([]byte{byte('a' + i%26)}, 8)
		payloads[i] = append(b, []byte(marker)...)
	}
	return payloads
}

// stagedRead reads the one record a participant staged for message i of a
// batch, through the exported hidden topic.
func stagedRead(t *testing.T, topic string) []byte {
	t.Helper()
	raw, err := store.GetForTest(txnContract, topic)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != 1 {
		t.Fatalf("staged topic %q has %d records, want 1", topic, len(raw))
	}
	return raw[0]
}

func assertHistory(t *testing.T, topic string, want [][]byte) {
	t.Helper()
	msgs, err := store.Message.GetAll(txnContract, topic, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != len(want) {
		t.Fatalf("GetAll(%q): got %d messages, want %d", topic, len(msgs), len(want))
	}
	for i, w := range want {
		if !bytes.Equal(msgs[i].Payload, w) {
			t.Fatalf("GetAll(%q)[%d] = %q, want %q", topic, i, msgs[i].Payload, w)
		}
	}
	entries, err := store.Message.History(txnContract, topic)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != len(want) {
		t.Fatalf("History(%q): got %d, want %d", topic, len(entries), len(want))
	}
}

// TestTxnStageIsInvisible: a staged batch is nowhere a client can read, and
// staging it again is idempotent.
func TestTxnStageIsInvisible(t *testing.T) {
	newTxnStore(t)
	const batch = "100.42.7"
	topics := []string{"teams.alpha", "teams.beta"}
	rec := txnBatch(batch, topics...)
	payloads := txnPayloads(2, "stage")
	staged, err := store.Txn.Stage(rec, payloads)
	if err != nil {
		t.Fatal(err)
	}
	for _, topic := range topics {
		assertHistory(t, topic, nil)
	}
	for _, ref := range store.Message.Topics() {
		if ref.Contract == txnContract {
			t.Fatalf("staged batch indexed topic %v", ref)
		}
	}
	// The staged bytes are there, hidden.
	for i := range topics {
		stagedRead(t, store.TxnStageTopicForTest(batch, i))
	}

	state, err := store.Txn.State(batch)
	if err != nil {
		t.Fatal(err)
	}
	if state.State != store.TxnPrepared {
		t.Fatalf("state = %d, want prepared", state.State)
	}

	// Re-stage with the same content: no new allocation, same record.
	again, err := store.Txn.Stage(txnBatch(batch, topics...), payloads)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(again.Messages[0].ID, staged.Messages[0].ID) {
		t.Fatal("idempotent re-stage allocated new ids")
	}
	// A different request under the same batch id is a conflict.
	conflict := txnBatch(batch, topics...)
	conflict.ContentHash = []byte("other")
	if _, err := store.Txn.Stage(conflict, payloads); !errors.Is(err, store.ErrTxnConflict) {
		t.Fatalf("re-stage with another content: %v, want ErrTxnConflict", err)
	}
}

// TestTxnCommitPromotesOnce: commit makes every message visible once and is
// idempotent; staging is removed afterwards.
func TestTxnCommitPromotesOnce(t *testing.T) {
	newTxnStore(t)
	const batch = "100.42.8"
	topics := []string{"teams.alpha", "teams.beta", "teams.gamma"}
	rec := txnBatch(batch, topics...)
	payloads := txnPayloads(3, "commit")
	if _, err := store.Txn.Stage(rec, payloads); err != nil {
		t.Fatal(err)
	}
	committed, _, _, err := store.Txn.Commit(batch)
	if err != nil {
		t.Fatal(err)
	}
	if committed.State != store.TxnCommitted {
		t.Fatalf("commit state = %d", committed.State)
	}
	for i, topic := range topics {
		assertHistory(t, topic, [][]byte{payloads[i]})
	}

	// Committing again is a no-op: still one message per topic.
	if _, _, _, err := store.Txn.Commit(batch); err != nil {
		t.Fatal(err)
	}
	for i, topic := range topics {
		assertHistory(t, topic, [][]byte{payloads[i]})
	}
	// Staging records were dropped.
	raw, err := store.GetForTest(txnContract, store.TxnStageTopicForTest(batch, 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != 0 {
		t.Fatalf("staging left after commit: %d records", len(raw))
	}
	// A committed batch cannot be aborted.
	if _, err := store.Txn.Abort(batch, "test"); !errors.Is(err, store.ErrTxnCommitted) {
		t.Fatalf("abort committed: %v, want ErrTxnCommitted", err)
	}
}

// TestTxnAbortLeavesNothing: an aborted batch is invisible on both real and
// staging topics, and aborting is idempotent; the same batch may be staged
// again as a fresh attempt.
func TestTxnAbortLeavesNothing(t *testing.T) {
	newTxnStore(t)
	const batch = "100.42.9"
	topics := []string{"teams.alpha", "teams.beta"}
	payloads := txnPayloads(2, "abort")
	if _, err := store.Txn.Stage(txnBatch(batch, topics...), payloads); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Txn.Abort(batch, "a participant failed"); err != nil {
		t.Fatal(err)
	}
	for _, topic := range topics {
		assertHistory(t, topic, nil)
	}
	if raw, err := store.GetForTest(txnContract, store.TxnStageTopicForTest(batch, 0)); err != nil || len(raw) != 0 {
		t.Fatalf("staging left after abort: %v, %d", err, len(raw))
	}
	state, err := store.Txn.State(batch)
	if err != nil {
		t.Fatal(err)
	}
	if state.State != store.TxnAborted || state.Reason != "a participant failed" {
		t.Fatalf("state = %+v, want aborted with reason", state)
	}
	// Idempotent.
	if _, err := store.Txn.Abort(batch, "again"); err != nil {
		t.Fatal(err)
	}
	// The batch retried with the same content can be staged and committed.
	if _, err := store.Txn.Stage(txnBatch(batch, topics...), payloads); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := store.Txn.Commit(batch); err != nil {
		t.Fatal(err)
	}
	for i, topic := range topics {
		assertHistory(t, topic, [][]byte{payloads[i]})
	}
}

// TestTxnCommitUnknown: a batch never staged cannot be committed.
func TestTxnCommitUnknown(t *testing.T) {
	newTxnStore(t)
	if _, _, _, err := store.Txn.Commit("1.1.1"); !errors.Is(err, store.ErrTxnUnknown) {
		t.Fatalf("commit unknown batch: %v, want ErrTxnUnknown", err)
	}
	// Aborting one is a no-op without error.
	if rec, err := store.Txn.Abort("1.1.2", "none"); err != nil || rec.State != store.TxnAborted {
		t.Fatalf("abort unknown batch: %+v, %v", rec, err)
	}
}

// TestTxnRecoverAcrossRestart: prepared, committed and aborted batches are
// enumerated when the store is reopened, and a prepared batch can be finished
// after the restart.
func TestTxnRecoverAcrossRestart(t *testing.T) {
	dir := newTxnStore(t)

	prepared := txnBatch("100.42.10", "recover.prepared")
	if _, err := store.Txn.Stage(prepared, [][]byte{[]byte("p")}); err != nil {
		t.Fatal(err)
	}
	committed := txnBatch("100.42.11", "recover.committed")
	if _, err := store.Txn.Stage(committed, [][]byte{[]byte("c")}); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := store.Txn.Commit("100.42.11"); err != nil {
		t.Fatal(err)
	}
	aborted := txnBatch("100.42.12", "recover.aborted")
	if _, err := store.Txn.Stage(aborted, [][]byte{[]byte("a")}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Txn.Abort("100.42.12", "test"); err != nil {
		t.Fatal(err)
	}

	reopenTxnStore(t, dir)
	owners, replicas, decisions, err := store.Txn.Recover()
	if err != nil {
		t.Fatal(err)
	}
	states := map[string]store.TxnState{}
	for _, rec := range owners {
		states[rec.BatchID] = rec.State
	}
	if states["100.42.10"] != store.TxnPrepared || states["100.42.11"] != store.TxnCommitted || states["100.42.12"] != store.TxnAborted {
		t.Fatalf("recovered states = %v", states)
	}
	if len(replicas) != 0 || len(decisions) != 0 {
		t.Fatalf("unexpected replica/decision records: %d %d", len(replicas), len(decisions))
	}

	// Finish the prepared batch after restart.
	if _, _, _, err := store.Txn.Commit("100.42.10"); err != nil {
		t.Fatal(err)
	}
	assertHistory(t, "recover.prepared", [][]byte{[]byte("p")})
	// The earlier committed batch is still one message and stays idempotent.
	assertHistory(t, "recover.committed", [][]byte{[]byte("c")})
	if _, _, _, err := store.Txn.Commit("100.42.11"); err != nil {
		t.Fatal(err)
	}
	assertHistory(t, "recover.committed", [][]byte{[]byte("c")})
}

// TestTxnAllocatedIdsDoNotCollide: ids allocated across many staged batches
// are all distinct, and normal auto-id puts keep working beside the
// reservations; committed batches hold exactly one message each.
func TestTxnAllocatedIdsDoNotCollide(t *testing.T) {
	newTxnStore(t)
	seen := map[string]bool{}
	const batches = 100
	for b := 0; b < batches; b++ {
		id := "100.42." + strconv.Itoa(100+b)
		topics := []string{"collide." + strconv.Itoa(b) + ".a", "collide." + strconv.Itoa(b) + ".b", "collide." + strconv.Itoa(b) + ".c"}
		rec := txnBatch(id, topics...)
		payloads := txnPayloads(3, "x")
		staged, err := store.Txn.Stage(rec, payloads)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range staged.Messages {
			if seen[string(m.ID)] || seen[string(m.StageID)] {
				t.Fatal("a transaction allocated an id twice")
			}
			seen[string(m.ID)] = true
			seen[string(m.StageID)] = true
		}
		if _, _, _, err := store.Txn.Commit(id); err != nil {
			t.Fatal(err)
		}
	}
	for b := 0; b < batches; b++ {
		for _, suffix := range []string{"a", "b", "c"} {
			topic := "collide." + strconv.Itoa(b) + "." + suffix
			msgs, err := store.Message.GetAll(txnContract, topic, "")
			if err != nil {
				t.Fatal(err)
			}
			if len(msgs) != 1 {
				t.Fatalf("%s: %d messages, want 1", topic, len(msgs))
			}
		}
	}
	// A normal auto-id put still works after the reservations.
	if err := store.Message.Put(txnContract, "collide.after", []byte("z"), ""); err != nil {
		t.Fatal(err)
	}
	assertHistory(t, "collide.after", [][]byte{[]byte("z")})
}

// TestTxnReplicaStageCommitAbort: a replica participant's staging is hidden,
// its commit lands in the replica namespace exactly once and records the
// deterministic replication id, and abort clears it.
func TestTxnReplicaStageCommitAbort(t *testing.T) {
	newTxnStore(t)
	const batch = "100.42.20"
	topic := "replica.topic"
	rec := txnBatch(batch, topic)
	rec.WaitReplica = true
	rec.Messages[0].ReplicaID = "node-a/1/2"
	payload := []byte("replica-payload")
	if _, err := store.Txn.StageReplica(rec, [][]byte{payload}); err != nil {
		t.Fatal(err)
	}
	// Hidden while prepared: the replica namespace is read by GetAll too.
	assertHistory(t, topic, nil)
	stagedRead(t, store.TxnReplicaStageTopicForTest(batch, 0))

	if _, _, err := store.Txn.CommitReplica(batch, nil); err != nil {
		t.Fatal(err)
	}
	assertHistory(t, topic, [][]byte{payload})
	if _, _, err := store.Txn.CommitReplica(batch, nil); err != nil {
		t.Fatal(err)
	}
	assertHistory(t, topic, [][]byte{payload})

	// A second batch aborts and leaves nothing.
	const batch2 = "100.42.21"
	rec2 := txnBatch(batch2, topic+".two")
	if _, err := store.Txn.StageReplica(rec2, [][]byte{payload}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Txn.AbortReplica(batch2, "test"); err != nil {
		t.Fatal(err)
	}
	assertHistory(t, topic+".two", nil)
}

// TestTxnDecision: the coordinator's decision records carry the payloads and
// survive a restart; the latest state wins.
func TestTxnDecision(t *testing.T) {
	dir := newTxnStore(t)
	const batch = "100.42.30"
	rec := txnBatch(batch, "decision.topic")
	rec.Messages[0].Payload = []byte("decision-payload")
	rec.State = store.TxnPrepared
	if err := store.Txn.SaveDecision(rec); err != nil {
		t.Fatal(err)
	}
	got, err := store.Txn.Decision(batch)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != store.TxnPrepared || string(got.Messages[0].Payload) != "decision-payload" {
		t.Fatalf("decision = %+v", got)
	}
	rec.Seq = got.Seq + 1
	rec.State = store.TxnCommitted
	if err := store.Txn.SaveDecision(rec); err != nil {
		t.Fatal(err)
	}
	got, err = store.Txn.Decision(batch)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != store.TxnCommitted {
		t.Fatalf("decision state = %d, want committed", got.State)
	}

	reopenTxnStore(t, dir)
	_, _, decisions, err := store.Txn.Recover()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, d := range decisions {
		if d.BatchID == batch {
			found = true
			if d.State != store.TxnCommitted || string(d.Messages[0].Payload) != "decision-payload" {
				t.Fatalf("recovered decision = %+v", d)
			}
		}
	}
	if !found {
		t.Fatal("committed decision not recovered")
	}
}

// TestTxnStageTTLExpires: staging and registry of a short-ttl batch expire,
// so an abandoned transaction does not outlive its messages.
func TestTxnStageTTLExpires(t *testing.T) {
	dir := newTxnStore(t)
	const batch = "100.42.40"
	rec := txnBatch(batch, "ttl.topic")
	rec.Messages[0].Ttl = "1s"
	if _, err := store.Txn.Stage(rec, [][]byte{[]byte("short")}); err != nil {
		t.Fatal(err)
	}
	if state, _ := store.Txn.State(batch); state.State != store.TxnPrepared {
		t.Fatalf("state = %d", state.State)
	}
	// Memdb time blocks are a second; wait past the entry's expiry and its
	// block, then reopen: the expired batch is no longer anywhere.
	time.Sleep(2500 * time.Millisecond)
	reopenTxnStore(t, dir)
	if state, _ := store.Txn.State(batch); state.State != store.TxnUnknown {
		t.Fatalf("state after ttl = %d, want unknown", state.State)
	}
	owners, _, _, err := store.Txn.Recover()
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range owners {
		if r.BatchID == batch {
			t.Fatal("expired batch still recovered")
		}
	}
}

// TestTxnStagedRecordsAreSealed: with encryption at rest on, staged and
// registry records are sealed on disk, and the batch still commits correctly.
func TestTxnStagedRecordsAreSealed(t *testing.T) {
	newStore(t, 0, ring(0), true)
	const batch = "100.42.50"
	topic := "sealed.topic"
	payload := []byte("sealed-transaction-payload")
	rec := txnBatch(batch, topic)
	if _, err := store.Txn.Stage(rec, [][]byte{payload}); err != nil {
		t.Fatal(err)
	}
	if raw, err := store.GetRawForTest(txnContract, store.TxnStageTopicForTest(batch, 0)); err != nil || len(raw) != 1 || bytes.Contains(raw[0], payload) {
		t.Fatalf("staged payload not sealed: %d records, err %v", len(raw), err)
	}
	if raw, err := store.GetRawForTest(store.SysContract, store.TxnRegistryTopicForTest(batch)); err != nil || len(raw) != 1 || bytes.Contains(raw[0], []byte(batch)) {
		t.Fatalf("registry record not sealed: %d records, err %v", len(raw), err)
	}
	if _, _, _, err := store.Txn.Commit(batch); err != nil {
		t.Fatal(err)
	}
	assertHistory(t, topic, [][]byte{payload})
}
