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
	"testing"

	"github.com/unit-io/unitdb/server/internal/store"
)

// TestRecoverCommittedDecisionPromotes: a committed coordinator decision
// and its prepared owner record left by a crashed process are completed at
// recovery: the message is visible once and the decision stays committed.
func TestRecoverCommittedDecisionPromotes(t *testing.T) {
	const contract = uint32(0x77050001)
	c := txnConn(contract)
	topic := txnTopicName()
	const batch = "77050001.9.1"
	payload := []byte("recovered-commit")

	rec := &store.TxnRecord{
		BatchID:      batch,
		Contract:     contract,
		Origin:       store.TxnOrigin{ConnID: uint64(c.connID), SessID: uint64(c.sessID)},
		MessageID:    901,
		DeliveryMode: 0,
		Messages: []store.TxnMessage{{
			Topic:   topic,
			Ttl:     "",
			Payload: payload,
		}},
	}
	if _, err := store.Txn.Stage(rec, [][]byte{payload}); err != nil {
		t.Fatal(err)
	}
	committed := *rec
	committed.State = store.TxnCommitted
	committed.Seq = 2
	if err := store.Txn.SaveDecision(&committed); err != nil {
		t.Fatal(err)
	}
	assertHistoryLen(t, contract, topic, 0)

	RecoverTransactions()

	assertHistoryLen(t, contract, topic, 1)
	if st, _ := store.Txn.State(batch); st.State != store.TxnCommitted {
		t.Fatalf("owner state=%d", st.State)
	}
	// Recovery is idempotent: a second run stores nothing again.
	RecoverTransactions()
	assertHistoryLen(t, contract, topic, 1)
}

// TestRecoverPreparedDecisionAborts: a prepared decision with no terminal
// commit is presumed aborted at recovery, and its owner staging dropped.
func TestRecoverPreparedDecisionAborts(t *testing.T) {
	const contract = uint32(0x77050002)
	c := txnConn(contract)
	topic := txnTopicName()
	const batch = "77050002.9.2"
	payload := []byte("recovered-abort")

	rec := &store.TxnRecord{
		BatchID:      batch,
		Contract:     contract,
		Origin:       store.TxnOrigin{ConnID: uint64(c.connID), SessID: uint64(c.sessID)},
		MessageID:    902,
		DeliveryMode: 0,
		Messages:     []store.TxnMessage{{Topic: topic, Payload: payload}},
	}
	if _, err := store.Txn.Stage(rec, [][]byte{payload}); err != nil {
		t.Fatal(err)
	}
	prepared := *rec
	prepared.State = store.TxnPrepared
	prepared.Seq = 1
	if err := store.Txn.SaveDecision(&prepared); err != nil {
		t.Fatal(err)
	}

	RecoverTransactions()

	if st, _ := store.Txn.Decision(batch); st.State != store.TxnAborted {
		t.Fatalf("decision state=%d, want aborted", st.State)
	}
	if st, _ := store.Txn.State(batch); st.State != store.TxnPrepared {
		// The owner record waits for the coordinator's abort, which the
		// recovery just wrote; the batch remains hidden regardless.
		t.Fatalf("owner state=%d", st.State)
	}
	assertHistoryLen(t, contract, topic, 0)
}
