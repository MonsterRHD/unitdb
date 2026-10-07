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
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	lp "github.com/unit-io/unitdb/server/internal/net"
	"github.com/unit-io/unitdb/server/internal/pkg/uid"
	"github.com/unit-io/unitdb/server/internal/types"
	"github.com/unit-io/unitdb/server/utp"
)

// txnWireClient connects an insecure client of a fresh contract and returns
// it with its raw id, for assertions against the store.
func txnWireClient(t *testing.T) (*testClient, uint32) {
	t.Helper()
	contract := uint32(0x77040000 + int(atomic.AddInt32(&txnTestTopic, 1))%1000)
	raw, err := uid.MintClientID(contract, false)
	if err != nil {
		t.Fatal(err)
	}
	text, err := Globals.Service.keys.SealClientID(raw, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return connectedClient(t, text, true), contract
}

func txnWireAck(t *testing.T, c *testClient, id uint16) {
	t.Helper()
	c.waitFor("publish acknowledge", isAck(utp.PUBLISH, id))
}

func txnWireError(t *testing.T, c *testClient, id uint16) *types.Error {
	t.Helper()
	m := c.waitFor("error notification", isPublishOn("unitdb/error/"))
	var e types.Error
	if err := json.Unmarshal(payloadOf(m), &e); err != nil {
		t.Fatal(err)
	}
	if e.ID != int(id) {
		t.Fatalf("error notification id=%d, want %d", e.ID, id)
	}
	return &e
}

// TestOnPublishValidatesBeforeSideEffects: one reserved topic in a
// multi-message publish fails the whole batch: error instead of ACK, and the
// good topic was never stored.
func TestOnPublishValidatesBeforeSideEffects(t *testing.T) {
	c, contract := txnWireClient(t)
	defer c.send(&utp.Disconnect{})
	good, bad := txnTopicName(), "$sys.blocked."+txnTopicName()
	id := uint16(8001)
	c.send(&utp.Publish{MessageID: id, Messages: []*utp.PublishMessage{
		{Topic: good, Payload: []byte("good")},
		{Topic: bad, Payload: []byte("bad")},
	}})

	e := txnWireError(t, c, id)
	if e.Status != types.ErrForbidden.Status {
		t.Fatalf("error status=%d want %d", e.Status, types.ErrForbidden.Status)
	}
	c.expectNone("acknowledge after a validation failure", isAck(utp.PUBLISH, id), 400*time.Millisecond)
	assertHistoryLen(t, contract, good, 0)
}

// TestOnPublishSpecialAndNormalOneAck: a batch with a keygen request and a
// normal message serves the keygen response, stores the message, and returns
// exactly one ACK.
func TestOnPublishSpecialAndNormalOneAck(t *testing.T) {
	c, contract := txnWireClient(t)
	defer c.send(&utp.Disconnect{})
	topic := txnTopicName()
	id := uint16(8002)
	keygenReq, _ := json.Marshal([]types.KeyGenRequest{{Topic: topic, Type: "rw"}})
	c.send(&utp.Publish{MessageID: id, Messages: []*utp.PublishMessage{
		{Topic: "unitdb/keygen", Payload: keygenReq},
		{Topic: topic, Payload: []byte("mixed")},
	}})

	m := c.waitFor("keygen response", isPublishOn("unitdb/keygen"))
	var resp []types.KeyGenResponse
	if err := json.Unmarshal(payloadOf(m), &resp); err != nil || len(resp) != 1 || resp[0].Key == "" {
		t.Fatalf("keygen response: %q err %v", string(payloadOf(m)), err)
	}
	txnWireAck(t, c, id)
	assertHistoryLen(t, contract, topic, 1)
	// Exactly one ACK: no second one queued.
	c.expectNone("a second ACK", isAck(utp.PUBLISH, id), 300*time.Millisecond)
}

// TestOnPublishMultiMessageWireCommit: multiple normal topics in one publish
// commit together and get one ACK over the wire.
func TestOnPublishMultiMessageWireCommit(t *testing.T) {
	c, contract := txnWireClient(t)
	defer c.send(&utp.Disconnect{})
	t1, t2, t3 := txnTopicName(), txnTopicName(), txnTopicName()
	id := uint16(8003)
	c.send(&utp.Publish{MessageID: id, DeliveryMode: 0, Messages: []*utp.PublishMessage{
		{Topic: t1, Payload: []byte("a")},
		{Topic: t2, Payload: []byte("b")},
		{Topic: t3, Payload: []byte("c")},
	}})
	txnWireAck(t, c, id)
	assertHistoryLen(t, contract, t1, 1)
	assertHistoryLen(t, contract, t2, 1)
	assertHistoryLen(t, contract, t3, 1)

	// Re-sending the same batch id with the same content is the idempotent
	// success: one ACK and still one stored copy each.
	c.send(&utp.Publish{MessageID: id, DeliveryMode: 0, Messages: []*utp.PublishMessage{
		{Topic: t1, Payload: []byte("a")},
		{Topic: t2, Payload: []byte("b")},
		{Topic: t3, Payload: []byte("c")},
	}})
	txnWireAck(t, c, id)
	assertHistoryLen(t, contract, t1, 1)
	assertHistoryLen(t, contract, t2, 1)
	assertHistoryLen(t, contract, t3, 1)
}

// TestOnPublishSpecialBadFormatNoSideEffect: a malformed topic beside a
// special request fails before the special request is answered.
func TestOnPublishSpecialBadFormatNoSideEffect(t *testing.T) {
	c, _ := txnWireClient(t)
	defer c.send(&utp.Disconnect{})
	topic := txnTopicName()
	id := uint16(8004)
	c.send(&utp.Publish{MessageID: id, Messages: []*utp.PublishMessage{
		{Topic: "unitdb/keygen", Payload: []byte(`{"Topic":"` + topic + `","Type":"rw"}`)},
		{Topic: "", Payload: []byte("no-topic")},
	}})
	e := txnWireError(t, c, id)
	if e.Status != types.ErrBadRequest.Status {
		t.Fatalf("status=%d want bad request", e.Status)
	}
	c.expectNone("keygen after a failed validation", isPublishOn("unitdb/keygen"), 400*time.Millisecond)
}

// TestOnPublishSpecialOnlyStillAcked: a batch of only special requests keeps
// its pre-transaction response behavior (response + one ACK).
func TestOnPublishSpecialOnlyStillAcked(t *testing.T) {
	c, _ := txnWireClient(t)
	defer c.send(&utp.Disconnect{})
	topic := txnTopicName()
	id := uint16(8005)
	keygenReq, _ := json.Marshal([]types.KeyGenRequest{{Topic: topic, Type: "r"}})
	c.send(&utp.Publish{MessageID: id, Messages: []*utp.PublishMessage{
		{Topic: "unitdb/keygen", Payload: keygenReq},
	}})
	m := c.waitFor("keygen response", isPublishOn("unitdb/keygen"))
	if len(payloadOf(m)) == 0 {
		t.Fatal("no keygen response")
	}
	txnWireAck(t, c, id)
}

// TestOnPublishForwardedSpecialDropped: a special request that arrives
// forwarded is answered by no node and leaves no response.
func TestOnPublishForwardedSpecialDropped(t *testing.T) {
	c := &_Conn{service: Globals.Service, send: make(chan lp.MessagePack, 4)}
	pub := utp.Publish{IsForwarded: true, Messages: []*utp.PublishMessage{
		{Topic: "unitdb/clientid", Payload: nil},
	}}
	if terr := c.onPublish(pub); terr != nil {
		t.Fatalf("forwarded special: %v", terr)
	}
	select {
	case <-c.send:
		t.Fatal("a forwarded special request was answered")
	default:
	}
}
