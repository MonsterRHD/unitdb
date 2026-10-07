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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/unit-io/unitdb/server/internal/message/security"
	"github.com/unit-io/unitdb/server/internal/pkg/uid"
	"github.com/unit-io/unitdb/server/internal/store"
	"github.com/unit-io/unitdb/server/internal/types"
	"github.com/unit-io/unitdb/server/utp"
)

var txnTestTopic int32 = 4000

func txnTopicName() string {
	n := atomic.AddInt32(&txnTestTopic, 1)
	return "txn.test.topic." + itoa32(n)
}

func itoa32(n int32) string {
	if n == 0 {
		return "0"
	}
	var b [12]byte
	pos := len(b)
	for n > 0 {
		pos--
		b[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(b[pos:])
}

// txnConn builds a publisher connection for direct coordinator calls, with a
// fresh id of a given contract and distinct session ids.
func txnConn(contract uint32) *_Conn {
	id, err := uid.MintClientID(contract, false)
	if err != nil {
		panic(err)
	}
	return &_Conn{
		service:  Globals.Service,
		clientID: id,
		connID:   uid.NewLID(),
		sessID:   uid.NewLID(),
	}
}

func txnValidated(topic, payload string) *validatedMsg {
	pm := &utp.PublishMessage{Topic: topic, Payload: []byte(payload), Ttl: ""}
	return &validatedMsg{msg: pm, topic: security.ParseKey(topic)}
}

func assertHistoryLen(t *testing.T, contract uint32, topic string, want int) {
	t.Helper()
	msgs, err := store.Message.GetAll(contract, topic, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != want {
		t.Fatalf("GetAll(%q): %d messages, want %d", topic, len(msgs), want)
	}
}

func meterCounts() (int64, int64, int64, int64) {
	m := Globals.Service.meter
	return m.InMsgs.Count(), m.InBytes.Count(), m.OutMsgs.Count(), m.OutBytes.Count()
}

// TestTxnLocalCommit: a standalone multi-message batch commits all topics,
// answers without a legacy fallback, meters the batch once at commit, and
// replays as an idempotent no-op.
func TestTxnLocalCommit(t *testing.T) {
	c := txnConn(0x77010001)
	topic1, topic2 := txnTopicName(), txnTopicName()
	pub := &utp.Publish{MessageID: 101, DeliveryMode: 0}
	msgs := []*validatedMsg{txnValidated(topic1, "p1"), txnValidated(topic2, "p2")}

	in0, bytes0, out0, _ := meterCounts()
	legacy, terr := c.runBatchPublish(pub, msgs)
	if legacy || terr != nil {
		t.Fatalf("runBatchPublish: legacy=%v err=%v", legacy, terr)
	}
	assertHistoryLen(t, c.clientID.Contract(), topic1, 1)
	assertHistoryLen(t, c.clientID.Contract(), topic2, 1)
	if got := Globals.Service.meter.InMsgs.Count() - in0; got != 2 {
		t.Fatalf("InMsgs delta=%d, want 2", got)
	}
	if got := Globals.Service.meter.InBytes.Count() - bytes0; got != int64(len("p1")+len("p2")) {
		t.Fatalf("InBytes delta=%d, want %d", got, len("p1")+len("p2"))
	}
	if got := Globals.Service.meter.OutMsgs.Count() - out0; got != 0 {
		t.Fatalf("OutMsgs delta without subscribers=%d, want 0", got)
	}

	// The durable state is committed.
	dec, err := store.Txn.Decision(batchID(c.clientID.Contract(), c.sessID, 101))
	if err != nil || dec.State != store.TxnCommitted {
		t.Fatalf("decision=%+v err=%v", dec, err)
	}
	if rec, err := store.Txn.State(batchID(c.clientID.Contract(), c.sessID, 101)); err != nil || rec.State != store.TxnCommitted {
		t.Fatalf("participant state=%d err=%v", rec.State, err)
	}

	// Replaying the same batch stores and meters nothing again.
	in1, bytes1 := Globals.Service.meter.InMsgs.Count(), Globals.Service.meter.InBytes.Count()
	if legacy, terr := c.runBatchPublish(pub, msgs); legacy || terr != nil {
		t.Fatalf("replay: legacy=%v err=%v", legacy, terr)
	}
	assertHistoryLen(t, c.clientID.Contract(), topic1, 1)
	assertHistoryLen(t, c.clientID.Contract(), topic2, 1)
	if Globals.Service.meter.InMsgs.Count() != in1 || Globals.Service.meter.InBytes.Count() != bytes1 {
		t.Fatal("a committed replay incremented meters")
	}
}

// TestTxnConcurrentSameBatch: two concurrent attempts of one batch commit
// exactly once.
func TestTxnConcurrentSameBatch(t *testing.T) {
	c := txnConn(0x77010002)
	topic := txnTopicName()
	pub := &utp.Publish{MessageID: 202, DeliveryMode: 0}
	msgs := []*validatedMsg{txnValidated(topic, "once")}

	var wg sync.WaitGroup
	errs := make(chan *types.Error, 2)
	legacies := make(chan bool, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l, e := c.runBatchPublish(pub, msgs)
			legacies <- l
			errs <- e
		}()
	}
	wg.Wait()
	close(errs)
	close(legacies)
	for e := range errs {
		if e != nil {
			t.Fatalf("concurrent commit error: %v", e)
		}
	}
	for l := range legacies {
		if l {
			t.Fatal("concurrent attempt took the legacy path")
		}
	}
	assertHistoryLen(t, c.clientID.Contract(), topic, 1)
}

// TestTxnDifferentSessionSameID: distinct sessions reusing a MessageID are
// independent batches (the existing per-connection id boundary).
func TestTxnDifferentSessionsSameID(t *testing.T) {
	contract := uint32(0x77010003)
	c1, c2 := txnConn(contract), txnConn(contract)
	topic := txnTopicName()
	msgs1 := []*validatedMsg{txnValidated(topic, "from-one")}
	msgs2 := []*validatedMsg{txnValidated(topic, "from-two")}
	pub := &utp.Publish{MessageID: 303, DeliveryMode: 0}
	if _, terr := c1.runBatchPublish(pub, msgs1); terr != nil {
		t.Fatal(terr)
	}
	if _, terr := c2.runBatchPublish(pub, msgs2); terr != nil {
		t.Fatal(terr)
	}
	assertHistoryLen(t, contract, topic, 2)
}

// TestTxnContentHashConflict: a committed batch id reused with different
// content is refused.
func TestTxnContentHashConflict(t *testing.T) {
	c := txnConn(0x77010004)
	topic1, topic2 := txnTopicName(), txnTopicName()
	pub := &utp.Publish{MessageID: 404, DeliveryMode: 0}
	if _, terr := c.runBatchPublish(pub, []*validatedMsg{txnValidated(topic1, "orig")}); terr != nil {
		t.Fatal(terr)
	}
	_, terr := c.runBatchPublish(pub, []*validatedMsg{txnValidated(topic2, "other-content")})
	if terr != types.ErrBadRequest {
		t.Fatalf("conflict err=%v, want ErrBadRequest", terr)
	}
	assertHistoryLen(t, c.clientID.Contract(), topic1, 1)
	assertHistoryLen(t, c.clientID.Contract(), topic2, 0)
}

// fakeTxnParticipant is an injectable participant for coordinator tests.
type fakeTxnParticipant struct {
	node          string
	prepareErr    *types.Error
	prepareErrAny error
	commitErrs    []*types.Error
	mu            sync.Mutex
	prepares      int
	commits       int
	aborts        int
}

func (f *fakeTxnParticipant) nodeName() string { return f.node }
func (f *fakeTxnParticipant) prepare() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.prepares++
	// Return an untyped nil on success: returning the *types.Error field
	// directly would hand the coordinator a typed-nil error interface.
	if f.prepareErrAny != nil {
		return f.prepareErrAny
	}
	if f.prepareErr != nil {
		return f.prepareErr
	}
	return nil
}
func (f *fakeTxnParticipant) commit() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	i := f.commits
	f.commits++
	if i < len(f.commitErrs) {
		return f.commitErrs[i]
	}
	return nil
}
func (f *fakeTxnParticipant) abort(string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.aborts++
}

// withTxnCluster builds a minimal two-node cluster and installs the remote
// participant seam for the test. The process-wide Globals.Cluster is not
// touched: tests pin the returned cluster on their publisher connection via
// its testCluster seam, so no background goroutine ever sees a swapped
// global.
func withTxnCluster(t *testing.T, fakes map[string]*fakeTxnParticipant) (*Cluster, func()) {
	t.Helper()
	cl := &Cluster{thisNodeName: "node-a", replicas: 2}
	ring := newRing(latestRingVersion, []string{"node-a", "node-b"})
	cl.ringMu.Lock()
	cl.ring = ring
	cl.fullRing = ring
	cl.ringVersion = latestRingVersion
	cl.ringMu.Unlock()

	prevSeam := newRemoteTxnParticipant
	newRemoteTxnParticipant = func(p remoteParticipantParams) (txnParticipant, error) {
		f, ok := fakes[p.node]
		if !ok {
			t.Errorf("unexpected remote participant %q", p.node)
			return nil, nil
		}
		return f, nil
	}
	return cl, func() {
		newRemoteTxnParticipant = prevSeam
	}
}

// txnConnOn returns a publisher connection planning against the given test
// cluster.
func txnConnOn(contract uint32, cl *Cluster) *_Conn {
	c := txnConn(contract)
	c.testCluster = cl
	return c
}

// topicOwnedBy returns a topic the test ring places on owner.
func topicOwnedBy(cl *Cluster, contract uint32, owner string) string {
	for {
		topic := txnTopicName()
		if cl.getRing().Get(topicRingKey(contract, topic)) == owner {
			return topic
		}
	}
}

// TestTxnPrepareFailureAbortsAll: when the remote participant cannot prepare,
// the local participant is aborted, nothing is stored or metered and the
// decision is aborted.
func TestTxnPrepareFailureAbortsAll(t *testing.T) {
	fake := &fakeTxnParticipant{node: "node-b", prepareErr: types.ErrServerError}
	cl, restore := withTxnCluster(t, map[string]*fakeTxnParticipant{"node-b": fake})
	defer restore()

	c := txnConnOn(0x77010005, cl)
	localTopic := topicOwnedBy(cl, c.clientID.Contract(), "node-a")
	remoteTopic := topicOwnedBy(cl, c.clientID.Contract(), "node-b")
	pub := &utp.Publish{MessageID: 505, DeliveryMode: 1}
	msgs := []*validatedMsg{txnValidated(localTopic, "l"), txnValidated(remoteTopic, "r")}

	in0, _, out0, _ := meterCounts()
	legacy, terr := c.runBatchPublish(pub, msgs)
	if legacy || terr != types.ErrServerError {
		t.Fatalf("prepare failure: legacy=%v err=%v", legacy, terr)
	}
	assertHistoryLen(t, c.clientID.Contract(), localTopic, 0)
	assertHistoryLen(t, c.clientID.Contract(), remoteTopic, 0)
	if Globals.Service.meter.InMsgs.Count() != in0 || Globals.Service.meter.OutMsgs.Count() != out0 {
		t.Fatal("an aborted batch was metered")
	}
	if fake.aborts != 1 {
		t.Fatalf("the failing participant was aborted %d times, want 1", fake.aborts)
	}
	// The local participant prepared and was told to abort.
	rec, err := store.Txn.State(batchID(c.clientID.Contract(), c.sessID, 505))
	if err != nil || rec.State != store.TxnAborted {
		t.Fatalf("local participant state=%d err=%v", rec.State, err)
	}
	dec, err := store.Txn.Decision(batchID(c.clientID.Contract(), c.sessID, 505))
	if err != nil || dec.State != store.TxnAborted {
		t.Fatalf("decision state=%d err=%v", dec.State, err)
	}
}

// TestTxnCommitFailureRetries: a participant whose first commit fails is
// committed by the retry of the same batch, once.
func TestTxnCommitFailureRetries(t *testing.T) {
	fake := &fakeTxnParticipant{node: "node-b", commitErrs: []*types.Error{types.ErrServerError}}
	cl, restore := withTxnCluster(t, map[string]*fakeTxnParticipant{"node-b": fake})
	defer restore()

	c := txnConnOn(0x77010006, cl)
	localTopic := topicOwnedBy(cl, c.clientID.Contract(), "node-a")
	remoteTopic := topicOwnedBy(cl, c.clientID.Contract(), "node-b")
	pub := &utp.Publish{MessageID: 606, DeliveryMode: 1}
	msgs := []*validatedMsg{txnValidated(localTopic, "l"), txnValidated(remoteTopic, "r")}

	if _, terr := c.runBatchPublish(pub, msgs); terr != types.ErrServerError {
		t.Fatalf("first attempt: %v", terr)
	}
	// The local participant committed despite the remote failure.
	assertHistoryLen(t, c.clientID.Contract(), localTopic, 1)

	// The client retries the same batch: the committed decision is resolved
	// and the remote participant's second commit succeeds.
	if _, terr := c.runBatchPublish(pub, msgs); terr != nil {
		t.Fatalf("retry: %v", terr)
	}
	fake.mu.Lock()
	if fake.commits != 2 {
		t.Fatalf("remote commit calls=%d, want 2", fake.commits)
	}
	if fake.prepares != 1 {
		t.Fatalf("remote prepare calls=%d, want 1", fake.prepares)
	}
	fake.mu.Unlock()
	restore()
	probe := connectedClient(t, newClientID(t), true)
	probe.subscribe(9001, "probe.topic.x", 0)
}

// TestTxnLocalCommitFansOut: a committed batch with a local subscriber
// delivers once, and the idempotent replay delivers nothing again.
func TestTxnLocalCommitFanout(t *testing.T) {
	contract := uint32(0x77010007)
	raw, err := uid.MintClientID(contract, false)
	if err != nil {
		t.Fatal(err)
	}
	text, err := Globals.Service.keys.SealClientID(raw, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	sub := connectedClient(t, text, true)
	defer sub.send(&utp.Disconnect{})
	topic := txnTopicName()
	sub.subscribe(7001, topic, 0)

	c := txnConn(contract)
	c.clientID = raw
	pub := &utp.Publish{MessageID: 707, DeliveryMode: 0}
	msgs := []*validatedMsg{txnValidated(topic, "delivered-once")}
	if _, terr := c.runBatchPublish(pub, msgs); terr != nil {
		t.Fatal(terr)
	}
	m := sub.waitFor("committed message delivery", isPublishOn(topic)).(*utp.Publish)
	if string(m.Messages[0].Payload) != "delivered-once" {
		t.Fatalf("payload=%q", m.Messages[0].Payload)
	}
	if _, terr := c.runBatchPublish(pub, msgs); terr != nil {
		t.Fatal(terr)
	}
	sub.expectNone("a second delivery of the committed batch", isPublishOn(topic), 400*time.Millisecond)
}

// TestTxnLegacyParticipantFallbackIsSticky: a prepare that finds an owner
// that cannot take transactions aborts whatever staged, marks the batch
// durably as legacy and asks for the per-message path; a retry of the same
// id takes that path without staging again, and a different content is
// refused.
func TestTxnLegacyParticipantFallbackIsSticky(t *testing.T) {
	fake := &fakeTxnParticipant{node: "node-b", prepareErrAny: errTxnUseLegacy}
	cl, restore := withTxnCluster(t, map[string]*fakeTxnParticipant{"node-b": fake})
	defer restore()

	c := txnConnOn(0x77010008, cl)
	localTopic := topicOwnedBy(cl, c.clientID.Contract(), "node-a")
	remoteTopic := topicOwnedBy(cl, c.clientID.Contract(), "node-b")
	pub := &utp.Publish{MessageID: 808, DeliveryMode: 1}
	msgs := []*validatedMsg{txnValidated(localTopic, "l"), txnValidated(remoteTopic, "r")}

	legacy, terr := c.runBatchPublish(pub, msgs)
	if !legacy || terr != nil {
		t.Fatalf("first attempt: legacy=%v err=%v, want the legacy path", legacy, terr)
	}
	// Nothing was made visible, and both participants were abandoned.
	assertHistoryLen(t, c.clientID.Contract(), localTopic, 0)
	assertHistoryLen(t, c.clientID.Contract(), remoteTopic, 0)
	fake.mu.Lock()
	if fake.aborts != 1 {
		t.Fatalf("the legacy participant was aborted %d times, want 1", fake.aborts)
	}
	fake.mu.Unlock()
	dec, err := store.Txn.Decision(batchID(c.clientID.Contract(), c.sessID, 808))
	if err != nil || dec.State != store.TxnAborted || dec.Reason != txnDecisionLegacy {
		t.Fatalf("decision=%+v err=%v, want an aborted legacy decision", dec, err)
	}

	// The retry takes the legacy path before any participant is built.
	legacy, terr = c.runBatchPublish(pub, msgs)
	if !legacy || terr != nil {
		t.Fatalf("retry: legacy=%v err=%v", legacy, terr)
	}
	fake.mu.Lock()
	if fake.prepares != 1 {
		t.Fatalf("the legacy owner was prepared %d times, want 1", fake.prepares)
	}
	fake.mu.Unlock()

	// A same-id retry with other content is rejected against the decision.
	other := []*validatedMsg{txnValidated(localTopic, "l2"), txnValidated(remoteTopic, "different")}
	if _, terr := c.runBatchPublish(pub, other); terr != types.ErrBadRequest {
		t.Fatalf("a changed legacy batch: %v, want ErrBadRequest", terr)
	}
}
