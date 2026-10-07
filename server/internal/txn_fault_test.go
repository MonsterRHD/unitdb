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
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/unit-io/unitdb/server/internal/store"
	"github.com/unit-io/unitdb/server/utp"
)

// TestTxnStageFailureAborts: a durable staging failure aborts the batch with
// nothing visible or metered.
func TestTxnStageFailureAborts(t *testing.T) {
	c := txnConn(0x77060001)
	topic := txnTopicName()
	pub := &utp.Publish{MessageID: 6001, DeliveryMode: 0}
	msgs := []*validatedMsg{txnValidated(topic, "x")}

	prev := txnStoreStage
	txnStoreStage = func(rec *store.TxnRecord, payloads [][]byte) (*store.TxnRecord, error) {
		return nil, errors.New("disk unavailable")
	}
	defer func() { txnStoreStage = prev }()

	in0, _, out0, _ := meterCounts()
	if _, terr := c.runBatchPublish(pub, msgs); terr == nil {
		t.Fatal("a staging failure must fail the batch")
	}
	if got := Globals.Service.meter.InMsgs.Count() - in0; got != 0 {
		t.Fatalf("InMsgs delta=%d, want 0", got)
	}
	if got := Globals.Service.meter.OutMsgs.Count() - out0; got != 0 {
		t.Fatalf("OutMsgs delta=%d, want 0", got)
	}
	assertHistoryLen(t, c.clientID.Contract(), topic, 0)
}

// TestTxnCommitFailureThenRetry: a commit failure after the durable decision
// leaves history empty for the attempt; once the store recovers, the retried
// batch resolves to committed with exactly one stored copy.
func TestTxnCommitFailureThenRetry(t *testing.T) {
	c := txnConn(0x77060002)
	topic := txnTopicName()
	pub := &utp.Publish{MessageID: 6002, DeliveryMode: 0}
	msgs := []*validatedMsg{txnValidated(topic, "once")}

	var fail atomic.Bool
	fail.Store(true)
	prev := txnStoreCommit
	txnStoreCommit = func(batchID string) (store.TxnRecord, [][]byte, bool, error) {
		if fail.Load() {
			return store.TxnRecord{}, nil, false, errors.New("disk unavailable")
		}
		return prev(batchID)
	}
	defer func() { txnStoreCommit = prev }()

	if _, terr := c.runBatchPublish(pub, msgs); terr == nil {
		t.Fatal("a commit failure must fail the attempt")
	}
	assertHistoryLen(t, c.clientID.Contract(), topic, 0)

	fail.Store(false)
	if _, terr := c.runBatchPublish(pub, msgs); terr != nil {
		t.Fatalf("retry after recovery: %v", terr)
	}
	assertHistoryLen(t, c.clientID.Contract(), topic, 1)
	// Further replays do not add copies.
	if _, terr := c.runBatchPublish(pub, msgs); terr != nil {
		t.Fatal(terr)
	}
	assertHistoryLen(t, c.clientID.Contract(), topic, 1)
}

// TestTxnConcurrentStress: many batches concurrently commit exactly once and
// leave exactly one message per topic; goroutines fan out but settle.
func TestTxnConcurrentStress(t *testing.T) {
	const n = 50
	var wg sync.WaitGroup
	errs := make(chan error, n)
	conns := make([]*_Conn, n)
	topics := make([]string, n)
	startGoroutines := runtime.NumGoroutine()
	for i := 0; i < n; i++ {
		conns[i] = txnConn(0x77060003)
		topics[i] = txnTopicName()
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			pub := &utp.Publish{MessageID: uint16(6100 + i), DeliveryMode: 0}
			msgs := []*validatedMsg{
				txnValidated(topics[i], "a"),
				txnValidated(txnTopicName(), "b"),
			}
			// Make both messages land in the same batch's history check:
			// use the first topic for the assertion; the second topic is
			// unique too.
			if _, terr := conns[i].runBatchPublish(pub, msgs); terr != nil {
				errs <- errors.New(terr.Message)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent batch failed: %v", err)
	}
	for i := 0; i < n; i++ {
		assertHistoryLen(t, conns[i].clientID.Contract(), topics[i], 1)
	}
	Globals.Service.inflight.Wait()
	// Fan-out goroutines all settle: no steady growth left behind.
	if got := runtime.NumGoroutine(); got > startGoroutines+n*2+8 {
		t.Fatalf("goroutines leaked: before=%d after=%d", startGoroutines, got)
	}
}
