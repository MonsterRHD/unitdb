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
	"bytes"
	"context"
	"errors"
	"hash/fnv"
	"strconv"
	"sync"
	"time"

	"github.com/unit-io/unitdb/server/internal/message/security"
	"github.com/unit-io/unitdb/server/internal/pkg/log"
	"github.com/unit-io/unitdb/server/internal/pkg/uid"
	"github.com/unit-io/unitdb/server/internal/store"
	"github.com/unit-io/unitdb/server/internal/types"
	"github.com/unit-io/unitdb/server/utp"
)

// Multi-topic publish transaction coordinator and its local participant.
//
// The coordinator runs on the node the client is connected to. It pins one
// owner per topic from a single ring snapshot, asks every participant (its
// local owner and, once available, remote owners) to stage the batch, durably
// records its commit decision, and only then asks everyone to commit; any
// failed prepare aborts the whole batch. The same batch id is a single
// transaction: retries return the decided result without storing or
// delivering again.
//
// Remote participants and replica staging are wired in by later tasks via
// the seams below; with no seam, a batch that has remote topics asks its
// caller to use the pre-transaction legacy path (mixed-version clusters).

// errTxnUseLegacy asks the handler to serve the batch on the old
// per-message path: a remote owner cannot take transactional publishes.
var errTxnUseLegacy = errors.New("txn: a participant does not support batch transactions")

// txnDecisionLegacy is the reason of an aborted decision that marks a batch
// as one that must be served on the pre-transaction path: an owner it was
// pinned to cannot take transactions. It is sticky, so a retried batch with
// the same id takes the same path instead of staging again.
const txnDecisionLegacy = "a participant does not support batch transactions"

// txnResolveAttempts/txnResolveInterval bound how long a failed commit is
// retried in the background of the process that decided it. Past that, the
// durable decision still resolves on a client retry or after a restart.
const (
	txnResolveAttempts = 30
	txnResolveInterval = 250 * time.Millisecond
)

// newRemoteTxnParticipant builds a participant for a remote owner node, or
// returns errTxnUseLegacy when the node cannot take transactional batches.
// cluster_txn.go wires it; tests override it.
var newRemoteTxnParticipant func(p remoteParticipantParams) (txnParticipant, error)

// Seams over the local participant's durable calls, so tests can inject a
// store failure the way putHint injects a hint write failure. Production
// points them at the store.
var (
	txnStoreStage = func(rec *store.TxnRecord, payloads [][]byte) (*store.TxnRecord, error) {
		return store.Txn.Stage(rec, payloads)
	}
	txnStoreCommit = func(batchID string) (store.TxnRecord, [][]byte, bool, error) {
		return store.Txn.Commit(batchID)
	}
)

// publishWaitsReplica reports whether the publish waits for a replica, as
// the pre-transaction path does: always for reliable/batch when there is a
// cluster, and for express only unless replication is async. Standalone
// never waits.
func publishWaitsForReplica(cl *Cluster, deliveryMode uint8) bool {
	return cl != nil && cl.waitsForReplica(isReliable(deliveryMode))
}

// validatedMsg is a normal publish message that passed the handler's
// validation, with the owner the batch is pinned to.
type validatedMsg struct {
	msg   *utp.PublishMessage
	topic *security.Topic
	owner string
	// index is the message's position in the whole batch, so ids shared by
	// every owner - the deterministic replication id - are unique.
	index int
}

// batchID is the stable identity of a client's publish attempt: the
// contract, the originating session (which survives reconnects and is
// carried to owner nodes by the proxied connection) and the client's
// MessageID. Different connections using the same MessageID differ by
// session, so they are different batches.
func batchID(contract uint32, sessID uid.LID, messageID uint16) string {
	return strconv.FormatUint(uint64(contract), 10) + "." + strconv.FormatUint(uint64(sessID), 10) + "." + strconv.FormatUint(uint64(messageID), 10)
}

// batchContentHash fingerprints the batch's messages in order; a retry with
// another content under the same id is rejected.
func batchContentHash(msgs []*utp.PublishMessage) []byte {
	h := fnv.New128a()
	var n [8]byte
	for _, m := range msgs {
		n[0] = 1
		h.Write(n[:1])
		h.Write([]byte(m.Topic))
		n[0] = 2
		h.Write(n[:1])
		h.Write([]byte(m.Ttl))
		n[0] = 3
		h.Write(n[:1])
		h.Write(m.Payload)
	}
	return h.Sum(nil)
}

// replicaID is the deterministic replication id of a message in a batch, the
// same to whatever replica and whenever it reaches one, so replicas store it
// once by the existing seen mechanism.
func replicaID(batchID string, i int) string {
	return "tx/" + batchID + "#" + strconv.Itoa(i)
}

// batchGate serializes attempts of one batch within this process, so a
// retried commit cannot promote or fan out next to the first attempt.
type batchGate struct {
	mu sync.Mutex
}

var txnGates sync.Map // batchID -> *batchGate

func txnLock(batchID string) *batchGate {
	g, _ := txnGates.LoadOrStore(batchID, &batchGate{})
	return g.(*batchGate)
}

// txnParticipant is one owner of the batch's topics. Its methods return a
// *types.Error for a client-visible failure, or this package's txn sentinels
// (moved, retryable, legacy, unprepared) that the coordinator interprets.
type txnParticipant interface {
	nodeName() string
	// prepare stages this participant's messages durably but invisibly.
	prepare() error
	// commit promotes them and, when this call performs the promotion,
	// fans them out once. Idempotent.
	commit() error
	// abort discards the staged messages. Idempotent.
	abort(reason string)
}

// remoteParticipantParams carries what the remote participant seam needs
// (Task 3).
type remoteParticipantParams struct {
	svc          *_Service
	cluster      *Cluster
	origin       store.TxnOrigin
	batchID      string
	hash         []byte
	contract     uint32
	messageID    uint16
	deliveryMode uint8
	ringVersion  int
	waitReplica  bool
	node         string
	msgs         []*validatedMsg
}

// localTxnParticipant stages and commits the topics this node owns.
type localTxnParticipant struct {
	svc          *_Service
	cluster      *Cluster
	origin       store.TxnOrigin
	batchID      string
	hash         []byte
	contract     uint32
	messageID    uint16
	deliveryMode uint8
	ringVersion  int
	waitReplica  bool
	node         string
	msgs         []*validatedMsg

	prepared *store.TxnRecord
}

func (p *localTxnParticipant) nodeName() string { return p.node }

func (p *localTxnParticipant) txnRecord() *store.TxnRecord {
	rec := &store.TxnRecord{
		BatchID:      p.batchID,
		Contract:     p.contract,
		Origin:       p.origin,
		MessageID:    uint32(p.messageID),
		DeliveryMode: p.deliveryMode,
		ContentHash:  p.hash,
		RingVersion:  p.ringVersion,
		WaitReplica:  p.waitReplica,
		ReplicaNode:  p.node,
		Messages:     make([]store.TxnMessage, len(p.msgs)),
	}
	for i, m := range p.msgs {
		rec.Messages[i] = store.TxnMessage{
			Topic:       m.topic.Topic,
			Ttl:         m.msg.Ttl,
			Payload:     m.msg.Payload,
			PayloadHash: hashPayload(m.msg.Payload),
			Owner:       m.owner,
			ReplicaID:   replicaID(p.batchID, m.index),
		}
	}
	return rec
}

func hashPayload(payload []byte) []byte {
	h := fnv.New128a()
	h.Write(payload)
	return h.Sum(nil)
}

func (p *localTxnParticipant) prepare() error {
	// Fence: a topic this node believed local can have moved just before
	// staging; the coordinator re-plans when it reports moved.
	if moved := p.movedTopics(); moved != nil {
		return errTxnMoved
	}
	payloads := make([][]byte, len(p.msgs))
	for i, m := range p.msgs {
		payloads[i] = m.msg.Payload
	}
	// Pick the synchronous replica, when one can take a staged copy, and
	// stage on it BEFORE writing the owner record. The record then says
	// whether the copy is confirmed. A holder that never answers (frozen,
	// restarting) does not fail the publish - the owner commits and hands
	// the entries to it as hints, as a put outside a transaction does - and
	// its possibly-late staging is never promoted, so it cannot duplicate.
	chosenReplica := ""
	replicaReady := false
	if p.waitReplica && p.cluster != nil {
		topics := make([]string, len(p.msgs))
		for i, m := range p.msgs {
			topics[i] = m.topic.Topic
		}
		chosenReplica, _ = p.cluster.chooseTxnReplica(p.contract, topics)
	}
	if chosenReplica != "" {
		preview := p.txnRecord()
		preview.ChosenReplica = chosenReplica
		if err := p.cluster.stageTxnReplica(p.cluster.nodes[chosenReplica], preview); err == nil {
			replicaReady = true
		} else {
			log.ErrLogger.Warn().Err(err).Str("context", "txn.prepare").Str("batch", p.batchID).Str("node", chosenReplica).Msg("the replica did not stage the batch in time; it converges at commit and handoff")
		}
	}
	rec := p.txnRecord()
	rec.ChosenReplica = chosenReplica
	rec.ReplicaReady = replicaReady
	staged, err := txnStoreStage(rec, payloads)
	if err != nil {
		// Do not leave a confirmed replica staging orphaned by an owner
		// staging failure: the batch is about to be aborted.
		if replicaReady {
			p.cluster.abortTxnReplica(rec)
		}
		log.ErrLogger.Error().Err(err).Str("context", "txn.prepare").Str("batch", p.batchID).Str("node", p.node).Msg("unable to stage a batch")
		return types.ErrServerError
	}
	p.prepared = staged
	return nil
}

// movedTopics returns the participant's topics the node no longer owns.
func (p *localTxnParticipant) movedTopics() []string {
	if p.cluster == nil {
		return nil
	}
	topics := make([]string, len(p.msgs))
	for i, m := range p.msgs {
		topics[i] = m.topic.Topic
	}
	ok, moved, _ := p.cluster.ownsTopics(p.contract, topics)
	if ok {
		return nil
	}
	return moved
}

func (p *localTxnParticipant) commit() error {
	if moved := p.movedTopics(); moved != nil {
		return errTxnMoved
	}
	if p.prepared == nil {
		rec, err := store.Txn.State(p.batchID)
		if err != nil {
			return types.ErrServerError
		}
		p.prepared = &rec
	}
	rec, payloads, promoted, err := txnStoreCommit(p.batchID)
	if err != nil {
		log.ErrLogger.Error().Err(err).Str("context", "txn.commit").Str("batch", p.batchID).Str("node", p.node).Msg("unable to commit a batch")
		return types.ErrServerError
	}
	if promoted {
		// Make the other holders durable exactly as a put's replication
		// does: promote the replica whose staging was confirmed, or hint one
		// that was not, then queue the rest and hint holders that cannot
		// take them now (including ones out of the ring), before fanning out.
		if p.cluster != nil {
			switch {
			case rec.ChosenReplica == "":
				// No synchronous copy was attempted: post-commit
				// distribution serves every holder.
			case rec.ReplicaReady:
				if err := p.cluster.commitTxnReplica(&rec); err != nil {
					log.ErrLogger.Warn().Err(err).Str("context", "txn.commit").Str("batch", p.batchID).Str("node", rec.ChosenReplica).Msg("the replica did not answer commit; its handoff converges it")
					p.cluster.hintTxnReplica(&rec, payloads)
				}
			default:
				// Its staging was not confirmed: never promote it; a hint
				// hands the committed entries to it, once.
				p.cluster.hintTxnReplica(&rec, payloads)
			}
			p.cluster.distributeTxnCommitted(&rec, payloads)
		}
		fanoutOwnerBatch(rec, payloads)
	}
	return nil
}

func (p *localTxnParticipant) abort(reason string) {
	rec := p.prepared
	if rec == nil {
		r, err := store.Txn.State(p.batchID)
		if err != nil {
			return
		}
		rec = &r
	}
	if out, err := store.Txn.Abort(p.batchID, reason); err == nil && out.State == store.TxnAborted && p.cluster != nil {
		p.cluster.abortTxnReplica(rec)
	}
}

// txnOrigin builds the batch's origin context as owner nodes need it to fan
// the messages out and to tie proxied requests to the client's session.
func (c *_Conn) txnOrigin() store.TxnOrigin {
	name := ""
	if cl := c.clusterRef(); cl != nil {
		name = cl.thisNodeName
	}
	return store.TxnOrigin{
		Node:     name,
		ConnID:   uint64(c.connID),
		SessID:   uint64(c.sessID),
		ClientID: c.clientID,
		Insecure: c.insecure.Load(),
	}
}

// runBatchPublish drives a validated multi-message publish as one
// transaction. legacy is true when a remote participant cannot take
// transactional batches and the caller should serve the batch on the old
// per-message path instead.
func (c *_Conn) runBatchPublish(pub *utp.Publish, msgs []*validatedMsg) (legacy bool, terr *types.Error) {
	if len(msgs) == 0 {
		return false, nil
	}
	contract := c.clientID.Contract()
	id := batchID(contract, c.sessID, pub.MessageID)
	hash := batchContentHash(msgsAsWire(msgs))

	gate := txnLock(id)
	gate.mu.Lock()
	defer func() {
		gate.mu.Unlock()
		// The decision is durable by the time the gate is released; a later
		// attempt that no longer finds the gate reads it from the store.
		txnGates.Delete(id)
	}()

	origin := c.txnOrigin()
	cl := c.clusterRef()
	// A node that advertises no batch-transaction capability serves every
	// publish as the pre-transaction node did, including a publish another
	// node forwarded to it. Standalone has no peers to be compatible with
	// and keeps the transactional path.
	if cl != nil && !hasCapability(capTxnPublish) {
		return true, nil
	}
	for i := range msgs {
		msgs[i].index = i
	}

	// Idempotency: what did this batch already decide here?
	decision, err := store.Txn.Decision(id)
	if err != nil {
		log.ErrLogger.Error().Err(err).Str("context", "txn").Str("batch", id).Msg("unable to read the batch decision")
		return false, types.ErrServerError
	}
	if decision.State != store.TxnUnknown {
		if !bytes.Equal(decision.ContentHash, hash) {
			log.ErrLogger.Warn().Str("context", "txn").Str("batch", id).Msg("a publish reuses a batch id with different content")
			return false, types.ErrBadRequest
		}
		if decision.State == store.TxnCommitted {
			// Make sure every participant is committed, then answer success
			// without storing, delivering or metering anything again.
			if terr := c.resolveCommittedDecision(&decision); terr != nil {
				return false, terr
			}
			return false, nil
		}
		if decision.State == store.TxnAborted && decision.Reason == txnDecisionLegacy {
			// An earlier attempt pinned an owner that cannot take
			// transactions: keep serving this batch id on the
			// pre-transaction path, however the ring moved since.
			return true, nil
		}
	}

	seq := 1
	if decision.State == store.TxnAborted {
		seq = decision.Seq + 1
	}

	// Drive prepare/commit against the current ring, replanning when an
	// owner reports a move. Retryable prepare failures use the same window
	// the pre-transaction forward used.
	deadline := time.Now().Add(forwardRetryFor)
	var lastParticipants []txnParticipant
	for attempt := 0; ; attempt++ {
		ringVersion := 0
		if cl != nil {
			ringVersion = cl.getRingVersion()
		}
		groupOrder, groups, _, terr := c.planOwners(contract, cl, ringVersion, msgs)
		if terr != nil {
			return false, terr
		}
		participants, legacy, terr := c.buildParticipants(origin, id, hash, contract, pub.MessageID, pub.DeliveryMode, ringVersion, cl, groupOrder, groups)
		if legacy {
			return true, nil
		}
		if terr != nil {
			return false, terr
		}

		record := decisionRecord(origin, id, hash, contract, pub.MessageID, pub.DeliveryMode, ringVersion, publishWaitsForReplica(cl, pub.DeliveryMode), msgs, groupOrder)
		record.Seq = seq
		record.State = store.TxnPrepared
		if err := store.Txn.SaveDecision(record); err != nil {
			log.ErrLogger.Error().Err(err).Str("context", "txn").Str("batch", id).Msg("unable to record the batch decision")
			return false, types.ErrServerError
		}

		prepErr := prepareParticipants(participants)
		switch {
		case prepErr == nil:
			// Prepare complete: the decision is commit, durably, before
			// anyone is asked to promote.
		case errors.Is(prepErr, errTxnUseLegacy):
			// An owner reached at prepare cannot take transactions: abandon
			// whatever staged, mark the batch durably as legacy, and serve it
			// message by message - a retry finds the mark and takes the same
			// path without staging again.
			abortParticipants(lastParticipants, txnDecisionLegacy)
			abortParticipants(participants, txnDecisionLegacy)
			legacy := *record
			legacy.Seq = record.Seq + 1
			legacy.State = store.TxnAborted
			legacy.Reason = txnDecisionLegacy
			_ = store.Txn.SaveDecision(&legacy)
			return true, nil
		case errors.Is(prepErr, errTxnMoved):
			abortParticipants(lastParticipants, "the ring moved")
			abortParticipants(participants, "the ring moved")
			if time.Now().Before(deadline) {
				lastParticipants = participants
				continue
			}
			return false, types.ErrServerError
		case errors.Is(prepErr, errTxnRetryable):
			abortParticipants(participants, "a transient prepare failure")
			if time.Now().Before(deadline) {
				time.Sleep(forwardRetry)
				continue
			}
			return false, types.ErrServerError
		default:
			abortParticipants(participants, "a participant failed to prepare")
			aborted := *record
			aborted.Seq = record.Seq + 1
			aborted.State = store.TxnAborted
			aborted.Reason = "prepare failed"
			_ = store.Txn.SaveDecision(&aborted)
			return false, asTypesError(prepErr)
		}

		committed := *record
		committed.Seq = record.Seq + 1
		committed.State = store.TxnCommitted
		if err := store.Txn.SaveDecision(&committed); err != nil {
			abortParticipants(participants, "commit decision not persisted")
			return false, types.ErrServerError
		}

		commitErr := commitParticipants(participants)
		switch {
		case commitErr == nil:
			return false, nil
		case errors.Is(commitErr, errTxnMoved):
			// A committed decision with a moved owner: replan; committed
			// participants answer as no-ops and the moved owner is resolved
			// against the new owner.
			if time.Now().Before(deadline) {
				lastParticipants = participants
				continue
			}
			enqueueTxnResolve(id)
			return false, types.ErrServerError
		case errors.Is(commitErr, errTxnUnprepared):
			// A participant lost its prepared record: re-prepare it within
			// the decision window.
			abortParticipants(participants, "a participant lost its prepared state")
			if time.Now().Before(deadline) {
				continue
			}
			enqueueTxnResolve(id)
			return false, types.ErrServerError
		default:
			// The durable decision stands: resolve in the background and
			// the client retry converges.
			enqueueTxnResolve(id)
			return false, asTypesError(commitErr)
		}
	}
}

// asTypesError returns the participant error as a *types.Error: participant
// errors already are one, anything unclassified is a server error.
func asTypesError(err error) *types.Error {
	if err == nil {
		return nil
	}
	var te *types.Error
	if errors.As(err, &te) {
		return te
	}
	return types.ErrServerError
}

func msgsAsWire(msgs []*validatedMsg) []*utp.PublishMessage {
	out := make([]*utp.PublishMessage, len(msgs))
	for i, m := range msgs {
		out[i] = m.msg
	}
	return out
}

// planOwners assigns each message its owner from the current ring.
func (c *_Conn) planOwners(contract uint32, cl *Cluster, ringVersion int, msgs []*validatedMsg) (order []string, groups map[string][]*validatedMsg, hasRemote bool, terr *types.Error) {
	groups = make(map[string][]*validatedMsg)
	for _, m := range msgs {
		owner := ""
		if cl != nil && !isWildcardTopic(m.topic.Topic) {
			name := cl.getRing().Get(topicRingKey(contract, string(m.topic.Topic[:m.topic.Size])))
			if name != cl.thisNodeName {
				owner = name
				hasRemote = true
			}
		}
		m.owner = owner
		if _, ok := groups[owner]; !ok {
			order = append(order, owner)
		}
		groups[owner] = append(groups[owner], m)
	}
	return order, groups, hasRemote, nil
}

func (c *_Conn) buildParticipants(origin store.TxnOrigin, id string, hash []byte, contract uint32, messageID uint16, deliveryMode uint8, ringVersion int, cl *Cluster, order []string, groups map[string][]*validatedMsg) ([]txnParticipant, bool, *types.Error) {
	participants := make([]txnParticipant, 0, len(order))
	wait := publishWaitsForReplica(cl, deliveryMode)
	for _, node := range order {
		if node == "" {
			participants = append(participants, &localTxnParticipant{
				svc:          c.service,
				cluster:      cl,
				origin:       origin,
				batchID:      id,
				hash:         hash,
				contract:     contract,
				messageID:    messageID,
				deliveryMode: deliveryMode,
				ringVersion:  ringVersion,
				waitReplica:  wait,
				node:         "",
				msgs:         groups[node],
			})
			continue
		}
		if newRemoteTxnParticipant == nil {
			return nil, true, nil
		}
		rp, err := newRemoteTxnParticipant(remoteParticipantParams{
			svc:          c.service,
			cluster:      cl,
			origin:       origin,
			batchID:      id,
			hash:         hash,
			contract:     contract,
			messageID:    messageID,
			deliveryMode: deliveryMode,
			ringVersion:  ringVersion,
			waitReplica:  wait,
			node:         node,
			msgs:         groups[node],
		})
		if err != nil {
			if errors.Is(err, errTxnUseLegacy) {
				return nil, true, nil
			}
			log.ErrLogger.Error().Err(err).Str("context", "txn").Str("node", node).Msg("remote participant rejected a batch")
			return nil, false, types.ErrServerError
		}
		participants = append(participants, rp)
	}
	return participants, false, nil
}

// decisionRecord assembles the coordinator's durable decision, carrying the
// payloads so a failover can drive the batch again on another node.
func decisionRecord(origin store.TxnOrigin, id string, hash []byte, contract uint32, messageID uint16, deliveryMode uint8, ringVersion int, waitReplica bool, msgs []*validatedMsg, order []string) *store.TxnRecord {
	rec := &store.TxnRecord{
		BatchID:      id,
		Contract:     contract,
		Origin:       origin,
		MessageID:    uint32(messageID),
		DeliveryMode: deliveryMode,
		ContentHash:  hash,
		RingVersion:  ringVersion,
		WaitReplica:  waitReplica,
		Messages:     make([]store.TxnMessage, len(msgs)),
	}
	for i, m := range msgs {
		rec.Messages[i] = store.TxnMessage{
			Topic:     m.topic.Topic,
			Ttl:       m.msg.Ttl,
			Payload:   m.msg.Payload,
			Owner:     m.owner,
			ReplicaID: replicaID(id, m.index),
		}
	}
	return rec
}

// prepareParticipants prepares every participant in parallel; one failure is
// enough to fail the batch.
func prepareParticipants(participants []txnParticipant) error {
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		first error
	)
	wg.Add(len(participants))
	for _, p := range participants {
		go func(p txnParticipant) {
			defer wg.Done()
			if err := p.prepare(); err != nil {
				mu.Lock()
				if first == nil {
					first = err
				}
				mu.Unlock()
			}
		}(p)
	}
	wg.Wait()
	return first
}

func commitParticipants(participants []txnParticipant) error {
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		first error
	)
	wg.Add(len(participants))
	for _, p := range participants {
		go func(p txnParticipant) {
			defer wg.Done()
			if err := p.commit(); err != nil {
				mu.Lock()
				if first == nil {
					first = err
				}
				mu.Unlock()
			}
		}(p)
	}
	wg.Wait()
	return first
}

// abortParticipants best-effort discards the staged batches of participants
// that prepared. It never returns an error: an abort that does not arrive is
// covered by the aborted decision and the ttl-bounded staging records.
func abortParticipants(participants []txnParticipant, reason string) {
	var wg sync.WaitGroup
	wg.Add(len(participants))
	for _, p := range participants {
		go func(p txnParticipant) {
			defer wg.Done()
			p.abort(reason)
		}(p)
	}
	wg.Wait()
}

// resolveCommittedDecision drives the commit of every participant of a batch
// this node already decided to commit, returning once they all report
// committed.
func (c *_Conn) resolveCommittedDecision(decision *store.TxnRecord) *types.Error {
	return resolveDecision(c.service, c, decision)
}

// resolveDecision drives a loaded decision's participants to their terminal
// state. conn is nil when recovery runs outside a client attempt. Owners
// are re-planned against the current ring, so an owner that moved resolves
// the batch against the previous one when its prepare arrives.
func resolveDecision(svc *_Service, c *_Conn, decision *store.TxnRecord) *types.Error {
	if decision.State != store.TxnCommitted {
		return nil
	}
	originConn := c
	if originConn == nil {
		originConn = decisionOriginConn(svc, decision)
	}
	cl := originConn.clusterRef()
	ringVersion := 0
	if cl != nil {
		ringVersion = cl.getRingVersion()
	}
	msgs := make([]*validatedMsg, len(decision.Messages))
	for i, m := range decision.Messages {
		v := &validatedMsg{
			msg:   &utp.PublishMessage{Topic: m.Topic, Payload: m.Payload, Ttl: m.Ttl},
			topic: security.ParseKey(m.Topic),
			index: i,
		}
		msgs[i] = v
	}
	order, groups, _, terr := originConn.planOwners(decision.Contract, cl, ringVersion, msgs)
	if terr != nil {
		return terr
	}
	participants, legacy, terr := originConn.buildParticipants(
		decision.Origin, decision.BatchID, decision.ContentHash, decision.Contract,
		uint16(decision.MessageID), decision.DeliveryMode, ringVersion,
		cl, order, groups)
	if terr != nil {
		return terr
	}
	if legacy {
		// The participants are older nodes reached through the legacy path:
		// the batch is committed wherever the new path already ran; nothing
		// safe to drive here.
		log.ErrLogger.Warn().Str("context", "txn.resolve").Str("batch", decision.BatchID).Msg("committed decision needs a legacy participant")
		return types.ErrServerError
	}
	return asTypesError(commitParticipants(participants))
}

// decisionOriginConn builds a minimal connection context for participant
// construction from a decision record outside a client attempt. Only its
// service and origin identity are used by the participants.
func decisionOriginConn(svc *_Service, decision *store.TxnRecord) *_Conn {
	return &_Conn{
		service:  svc,
		clientID: decision.Origin.ClientID,
		connID:   uid.LID(decision.Origin.ConnID),
		sessID:   uid.LID(decision.Origin.SessID),
	}
}

// ----- in-doubt resolver -----

var (
	txnResolveOnce sync.Once
	txnResolveCh   chan string
	txnResolveStop chan struct{}
)

// startTxnResolver starts the background resolver of committed decisions
// whose commit did not complete in the client attempt. It runs once.
func startTxnResolver(ctx context.Context, svc *_Service) {
	txnResolveOnce.Do(func() {
		txnResolveCh = make(chan string, 1024)
		txnResolveStop = make(chan struct{})
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-txnResolveStop:
					return
				case id := <-txnResolveCh:
					resolveWithRetry(svc, id)
				}
			}
		}()
	})
}

// stopTxnResolver stops the resolver; tests use it between service runs.
func stopTxnResolver() {
	if txnResolveStop != nil {
		select {
		case <-txnResolveStop:
		default:
			close(txnResolveStop)
		}
	}
}

func enqueueTxnResolve(batchID string) {
	if txnResolveCh == nil {
		return
	}
	select {
	case txnResolveCh <- batchID:
	default:
		// The queue is full: the client retry and restart recovery still
		// resolve the durable decision.
		log.ErrLogger.Warn().Str("context", "txn.resolve").Str("batch", batchID).Msg("resolver queue full; the batch resolves on retry")
	}
}

// resolveWithRetry drives a decision's commits for a bounded time, long
// enough to outlast a short owner restart or re-election.
func resolveWithRetry(svc *_Service, batchID string) {
	for attempt := 0; attempt < txnResolveAttempts; attempt++ {
		decision, err := store.Txn.Decision(batchID)
		if err != nil || decision.State != store.TxnCommitted {
			return
		}
		if terr := resolveDecision(svc, nil, &decision); terr == nil {
			return
		}
		select {
		case <-time.After(txnResolveInterval):
		case <-txnResolveStop:
			return
		}
	}
	log.ErrLogger.Warn().Str("context", "txn.resolve").Str("batch", batchID).Msg("a committed batch is not resolved yet; it converges on retry or restart")
}

// resolveTxnsOnRecover finishes durable transactions found at startup:
// committed decisions are driven to completion, prepared decisions without a
// terminal commit are presumed aborted. It is called after the store is open
// (Task 5 wires it into service/cluster startup).
func resolveTxnsOnRecover(svc *_Service, decisions []store.TxnRecord) {
	for i := range decisions {
		decision := decisions[i]
		switch decision.State {
		case store.TxnCommitted:
			if terr := resolveDecision(svc, nil, &decision); terr != nil {
				enqueueTxnResolve(decision.BatchID)
			}
		case store.TxnPrepared:
			// Presumed abort: this node died before recording a terminal
			// decision. Participants still holding the batch abort it on
			// their own recovery; drop the local prepared decision.
			aborted := decision
			aborted.Seq = decision.Seq + 1
			aborted.State = store.TxnAborted
			aborted.Reason = "coordinator restarted before commit"
			_ = store.Txn.SaveDecision(&aborted)
		}
	}
}

// RecoverTransactions resumes durable batch transactions found at startup:
// it is called after the store is open and the cluster started, before the
// server takes client connections.
func RecoverTransactions() {
	owners, replicas, decisions, err := store.Txn.Recover()
	if err != nil {
		log.ErrLogger.Error().Err(err).Str("context", "txn.recover").Msg("unable to enumerate durable batch transactions")
		return
	}
	svc := Globals.Service
	if svc == nil {
		return
	}
	resolveTxnsOnRecover(svc, decisions)
	participantTxnsOnRecover(svc, owners, replicas)
}

// participantTxnsOnRecover finishes or aborts the participant records this
// node holds after a restart. Committed-but-unfinished promotions are
// completed (fan-out happens only to subscribers connected at this node);
// prepared records wait for the coordinator and are otherwise ttl-cleaned;
// aborted records' staging is already gone.
func participantTxnsOnRecover(svc *_Service, owners, replicas []store.TxnRecord) {
	for i := range owners {
		rec := owners[i]
		switch rec.State {
		case store.TxnCommitted:
			// Commit is idempotent: finish a promotion interrupted by the
			// crash; no live fan-out is repeated when it was already
			// committed (store reports promoted=false).
			if _, _, _, err := store.Txn.Commit(rec.BatchID); err != nil {
				log.ErrLogger.Error().Err(err).Str("context", "txn.recover").Str("batch", rec.BatchID).Msg("unable to finish a committed owner batch")
			}
		case store.TxnPrepared:
			// Keep it: the coordinator's commit or abort is retried. Beyond
			// the ttl the record and staging expire on their own.
		}
	}
	for i := range replicas {
		rec := replicas[i]
		if rec.State == store.TxnCommitted {
			if _, _, err := store.Txn.CommitReplica(rec.BatchID, nil); err != nil {
				log.ErrLogger.Error().Err(err).Str("context", "txn.recover").Str("batch", rec.BatchID).Msg("unable to finish a committed replica batch")
			}
		}
	}
}
