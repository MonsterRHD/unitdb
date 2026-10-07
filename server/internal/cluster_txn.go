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

	"github.com/unit-io/unitdb/server/internal/message/security"
	"github.com/unit-io/unitdb/server/internal/pkg/log"
	"github.com/unit-io/unitdb/server/internal/pkg/uid"
	"github.com/unit-io/unitdb/server/internal/store"
	"github.com/unit-io/unitdb/server/internal/types"
)

// Multi-topic publish transactions across cluster nodes.
//
// The coordinator stages each owner group with TxPrepare, commits with
// TxCommit only after every participant prepared and its own decision is
// durable, and aborts with TxAbort on failure. Each owner stages its
// reliable replica synchronously (TxReplicaPrepare) or queues the
// deterministic replication entries after commit. Both prepare and commit
// are fenced by the owner's current ring: an owner that has lost a topic
// reports Moved instead of promoting it, and a new owner resolves the batch
// against the previous owner before staging.

// Participant outcome sentinels. They are process-local: an rpc failure that
// may be the missing method is classified with the existing capability
// machinery before any call is made.
var (
	errTxnMoved       = errors.New("txn: the topic moved to another owner")
	errTxnRetryable   = errors.New("txn: the participant had a transient failure")
	errTxnUnprepared  = errors.New("txn: the participant does not hold the batch")
	errTxnMismatch    = errors.New("txn: the batch content does not match")
	errTxnReplicaFail = errors.New("txn: the reliable replica could not stage the batch")
)

const (
	// txnPrepareTimeout bounds the prepare RPC, which on an owner also waits
	// for its replica's staging.
	txnPrepareTimeout = forwardRetryFor
	txnCommitTimeout  = replicaAckTimeout * 2
	txnAbortTimeout   = replicaAckTimeout
)

// TxnEntry is one message of a batch as an owner stages it.
type TxnEntry struct {
	Topic     string
	Payload   []byte
	Ttl       string
	ReplicaID string
}

// TxnPrepareReq asks an owner to stage a batch. PinOwner is the owner the
// coordinator pinned from its ring snapshot; when it is another node's name,
// the receiver is the new owner after a ring change and resolves the batch
// against the previous owner first.
type TxnPrepareReq struct {
	Node         string
	BatchID      string
	Contract     uint32
	Origin       ClusterSess
	MessageID    uint32
	DeliveryMode uint8
	ContentHash  []byte
	RingVersion  int
	PinOwner     string
	WaitReplica  bool
	Entries      []TxnEntry
}

// TxnPrepareResp reports the staged state, or why the prepare did not stage.
type TxnPrepareResp struct {
	// State: 1 prepared, 2 already committed.
	State uint8
	// Moved: this node no longer owns one of the topics; Owner names the
	// current owner of the first moved topic.
	Moved  bool
	Owner  string
	Reason string
}

// TxnCommitReq asks an owner to commit a staged batch.
type TxnCommitReq struct {
	Node        string
	BatchID     string
	PinOwner    string
	RingVersion int
}

// TxnCommitResp reports Committed (a no-op when already committed) or Moved.
type TxnCommitResp struct {
	Committed bool
	Moved     bool
	Owner     string
	// Unprepared: the node holds no record of the batch (it restarted past
	// its retention); the coordinator must prepare again.
	Unprepared bool
}

// TxnAbortReq discards a staged batch.
type TxnAbortReq struct {
	Node    string
	BatchID string
	Reason  string
}

// TxnAbortResp reports whether the batch was committed when the abort came
// (it cannot be undone).
type TxnAbortResp struct {
	Committed bool
}

// TxnStateReq asks an owner for a batch's state, used by a new owner after a
// ring change and by retries that found an rpc outcome uncertain.
type TxnStateReq struct {
	Node     string
	BatchID  string
	Contract uint32
	Topics   []string
}

type TxnStateResp struct {
	// State: 0 unknown, 1 prepared, 2 committed, 3 aborted.
	State uint8
	Moved bool
	Owner string
}

// TxnReplicaReq stages/commits/aborts a batch at a replica. The payloads
// ride the prepare only; commit and abort name the batch.
type TxnReplicaReq struct {
	Node        string
	BatchID     string
	Contract    uint32
	Entries     []TxnEntry
	ContentHash []byte
}

type TxnReplicaResp struct {
	// AlreadyReplicated: the replica already holds the deterministic message
	// ids (from a hint or regular replication); no staging is needed.
	AlreadyReplicated bool
	Prepared          bool
	Committed         bool
}

// ----- fencing -----

// txnMovedTopics returns the topics of req the node does not currently own
// (wildcard topics stay local and are skipped), with the current owner of
// the first moved one.
func (c *Cluster) txnMovedTopics(contract uint32, topics []string) (moved []string, owner string) {
	for _, t := range topics {
		parsed := security.ParseKey(t)
		name := parsed.Topic[:parsed.Size]
		if isWildcardTopic(name) {
			continue
		}
		if c.isRemoteTopic(contract, name) {
			moved = append(moved, t)
			if owner == "" {
				owner = c.getRing().Get(topicRingKey(contract, name))
			}
		}
	}
	return moved, owner
}

// ownsTopics reports whether this node currently owns every non-wildcard
// topic; the moved list and the current owner of the first moved topic are
// returned for the local participant's fence.
func (c *Cluster) ownsTopics(contract uint32, topics []string) (bool, []string, string) {
	moved, owner := c.txnMovedTopics(contract, topics)
	return len(moved) == 0, moved, owner
}

// queryRemoteBatchState asks an owner for a batch's state. Unknown/unreachable
// is TxnUnknown; it is up to the caller to interpret it with the ring.
func (c *Cluster) queryRemoteBatchState(node, batchID string, contract uint32, topics []string) TxnStateResp {
	n := c.nodes[node]
	if n == nil {
		return TxnStateResp{}
	}
	req := &TxnStateReq{Node: c.thisNodeName, BatchID: batchID, Contract: contract, Topics: topics}
	var resp TxnStateResp
	if err := n.callTimeout("Cluster.TxnState", req, &resp, txnAbortTimeout); err != nil {
		return TxnStateResp{}
	}
	return resp
}

// abortRemoteBatch best-effort aborts a batch another node staged.
func (c *Cluster) abortRemoteBatch(node, batchID, reason string) TxnAbortResp {
	n := c.nodes[node]
	if n == nil {
		return TxnAbortResp{}
	}
	var resp TxnAbortResp
	_ = n.callTimeout("Cluster.TxnAbort", &TxnAbortReq{Node: c.thisNodeName, BatchID: batchID, Reason: reason}, &resp, txnAbortTimeout)
	return resp
}

// nodeInLiveRing reports whether node is part of the current ring.
func (c *Cluster) nodeInLiveRing(node string) bool {
	for _, n := range c.getRingNodes() {
		if n == node {
			return true
		}
	}
	return false
}

// ----- owner-side record mapping -----

func txnRecordFromReq(req *TxnPrepareReq, chosenReplica string) *store.TxnRecord {
	rec := &store.TxnRecord{
		BatchID:       req.BatchID,
		Contract:      req.Contract,
		Origin:        store.TxnOrigin{Node: req.Origin.Node, ConnID: uint64(req.Origin.ConnID), SessID: uint64(req.Origin.SessID), ClientID: req.Origin.ClientID, Insecure: req.Origin.Insecure},
		MessageID:     req.MessageID,
		DeliveryMode:  req.DeliveryMode,
		ContentHash:   req.ContentHash,
		RingVersion:   req.RingVersion,
		WaitReplica:   req.WaitReplica,
		ChosenReplica: chosenReplica,
		Messages:      make([]store.TxnMessage, len(req.Entries)),
	}
	for i, e := range req.Entries {
		rec.Messages[i] = store.TxnMessage{
			Topic:     e.Topic,
			Ttl:       e.Ttl,
			Payload:   e.Payload,
			Owner:     req.PinOwner,
			ReplicaID: e.ReplicaID,
		}
	}
	return rec
}

func txnEntriesFromRecord(rec *store.TxnRecord) []TxnEntry {
	entries := make([]TxnEntry, len(rec.Messages))
	for i, m := range rec.Messages {
		entries[i] = TxnEntry{Topic: m.Topic, Payload: m.Payload, Ttl: m.Ttl, ReplicaID: m.ReplicaID}
	}
	return entries
}

func txnPayloads(entries []TxnEntry) [][]byte {
	payloads := make([][]byte, len(entries))
	for i, e := range entries {
		payloads[i] = e.Payload
	}
	return payloads
}

// fanoutOwnerBatch fans a promotion out to the owner's subscribers and
// counts the inbound meters once. It is the owner side of a commit, local or
// over RPC.
func fanoutOwnerBatch(rec store.TxnRecord, payloads [][]byte) {
	svc := Globals.Service
	if svc == nil {
		return
	}
	cl := Globals.Cluster
	for i := range rec.Messages {
		payload := payloads[i]
		if payload == nil {
			continue
		}
		m := rec.Messages[i]
		svc.inflight.Add(1)
		go func(topic string) {
			defer svc.inflight.Done()
			fanoutCommitted(svc, cl, rec.Contract, uint16(rec.MessageID), rec.DeliveryMode, security.ParseKey(topic), payload)
		}(m.Topic)
		svc.meter.InMsgs.Inc(1)
		svc.meter.InBytes.Inc(int64(len(payload)))
	}
}

// ----- replica staging/commit (owner drives) -----

// chooseTxnReplica returns the live node the owner stages its synchronous
// replica on. name is empty when no replica can take a staged copy: the ring
// is below the replica count, a holder is an older or disconnected node, or
// the only other holder cannot stage. The publish then takes the same
// best-effort path as one outside a transaction: it is committed on the
// owner and handed to holders after commit (a queue, or a hint that reaches
// them when they can take it), never refused.
func (c *Cluster) chooseTxnReplica(contract uint32, topics []string) (name string, canStage bool) {
	if c.replicas < 2 || len(c.getRingNodes()) < 2 {
		return "", false
	}
	for _, t := range topics {
		parsed := security.ParseKey(t)
		name := parsed.Topic[:parsed.Size]
		if isWildcardTopic(name) {
			continue
		}
		for _, replica := range c.getRing().GetN(topicRingKey(contract, name), c.replicas) {
			if replica == c.thisNodeName {
				continue
			}
			n := c.nodes[replica]
			if n == nil || !n.connected || !n.supports(capReplicate) || !n.supports(capTxnPublish) {
				continue
			}
			// One replica takes the whole batch, whatever its placement for
			// each individual topic.
			return replica, true
		}
		return "", false
	}
	return "", false
}

// stageTxnReplica stages the batch on its synchronous replica.
func (c *Cluster) stageTxnReplica(n *ClusterNode, rec *store.TxnRecord) error {
	if n == nil {
		return errTxnReplicaFail
	}
	req := &TxnReplicaReq{Node: c.thisNodeName, BatchID: rec.BatchID, Contract: rec.Contract, Entries: txnEntriesFromRecord(rec), ContentHash: rec.ContentHash}
	var resp TxnReplicaResp
	if err := n.callTimeout("Cluster.TxnReplicaPrepare", req, &resp, replicaAckTimeout); err != nil {
		return err
	}
	if !resp.Prepared && !resp.AlreadyReplicated {
		return errTxnReplicaFail
	}
	return nil
}

// commitTxnReplica tells the staged replica to promote. An error means no
// visible copy is confirmed there (it did not answer, or it never staged):
// the caller keeps it hints so its handoff stores the batch.
func (c *Cluster) commitTxnReplica(rec *store.TxnRecord) error {
	if rec.ChosenReplica == "" {
		return nil
	}
	n := c.nodes[rec.ChosenReplica]
	if n == nil {
		return errTxnReplicaFail
	}
	var resp TxnReplicaResp
	if err := n.callTimeout("Cluster.TxnReplicaCommit", &TxnReplicaReq{Node: c.thisNodeName, BatchID: rec.BatchID, Contract: rec.Contract}, &resp, replicaAckTimeout); err != nil {
		return err
	}
	if !resp.Committed && !resp.AlreadyReplicated {
		return errTxnReplicaFail
	}
	return nil
}

// hintTxnReplica keeps each committed entry for the synchronous replica that
// did not answer its commit, so its handoff stores the entry (idempotently,
// under the deterministic ReplicaID) instead of leaving only the hidden
// staged copy.
func (c *Cluster) hintTxnReplica(rec *store.TxnRecord, payloads [][]byte) {
	if rec.ChosenReplica == "" {
		return
	}
	for i, m := range rec.Messages {
		if payloads[i] == nil {
			continue
		}
		c.hint(rec.ChosenReplica, ReplicaEntry{
			ID:        m.ReplicaID,
			Contract:  rec.Contract,
			Topic:     m.Topic,
			Payload:   payloads[i],
			Ttl:       m.Ttl,
			ExpiresAt: m.ExpiresAt,
		})
	}
}

// abortTxnReplica discards the staged replica copy.
func (c *Cluster) abortTxnReplica(rec *store.TxnRecord) {
	if rec.ChosenReplica == "" {
		return
	}
	n := c.nodes[rec.ChosenReplica]
	if n == nil {
		return
	}
	var resp TxnReplicaResp
	_ = n.callTimeout("Cluster.TxnReplicaAbort", &TxnReplicaReq{Node: c.thisNodeName, BatchID: rec.BatchID, Contract: rec.Contract}, &resp, txnAbortTimeout)
}

// distributeTxnCommitted hands a committed owner batch's entries to the
// topic's other holders the way a put's replication does: live holders take
// them asynchronously apart from the one that staged the batch
// (skipLive), holders that cannot take them and holders currently out of the
// ring get hints, so a node that was down when the batch committed - the
// topic's owner included - receives them once it is back. The deterministic
// ReplicaIDs make every copy idempotent.
func (c *Cluster) distributeTxnCommitted(rec *store.TxnRecord, payloads [][]byte) {
	for i, m := range rec.Messages {
		if payloads[i] == nil {
			continue
		}
		c.replicateEntry(ReplicaEntry{
			ID:        m.ReplicaID,
			Contract:  rec.Contract,
			Topic:     m.Topic,
			Payload:   payloads[i],
			Ttl:       m.Ttl,
			ExpiresAt: m.ExpiresAt,
		}, false, rec.ChosenReplica)
	}
}

// ----- owner RPC methods -----

func (c *Cluster) txnCapable() bool {
	return hasCapability(capTxnPublish)
}

// TxnPrepare is the owner's prepare handler.
func (c *Cluster) TxnPrepare(req *TxnPrepareReq, resp *TxnPrepareResp) error {
	if !c.txnCapable() {
		return errCapabilityOff(capTxnPublish)
	}
	topics := make([]string, len(req.Entries))
	for i, e := range req.Entries {
		topics[i] = e.Topic
	}
	if moved, owner := c.txnMovedTopics(req.Contract, topics); len(moved) != 0 {
		resp.Moved, resp.Owner, resp.Reason = true, owner, "the ring moved before prepare"
		return nil
	}

	// A new owner after a ring change: resolve the batch against the
	// previous owner before touching the store.
	if req.PinOwner != "" && req.PinOwner != c.thisNodeName {
		if c.nodeInLiveRing(req.PinOwner) {
			switch state := c.queryRemoteBatchState(req.PinOwner, req.BatchID, req.Contract, topics); state.State {
			case 2:
				resp.State = 2
				return nil
			case 1:
				c.abortRemoteBatch(req.PinOwner, req.BatchID, "the topic moved to a new owner")
			}
		}
		// The previous owner left the ring (dead) or never held the batch:
		// stage fresh here.
	}

	// A re-prepare of a batch already here is idempotent; a different
	// content is refused.
	existing, _ := store.Txn.State(req.BatchID)
	if existing.State != store.TxnUnknown {
		if !equalHash(existing.ContentHash, req.ContentHash) {
			resp.Reason = "the batch id is reused with different content"
			return errTxnMismatch
		}
		if existing.State == store.TxnCommitted {
			resp.State = 2
			return nil
		}
	}

	// Pick the synchronous replica and stage it BEFORE the owner record:
	// the record says whether the copy is confirmed. A holder that never
	// answers does not fail the prepare; the owner commits and hints the
	// entries to it, and its possibly-late staging is never promoted. A
	// re-prepare keeps the choice and confirmation already on record.
	payloads := txnPayloads(req.Entries)
	chosenReplica := ""
	replicaReady := false
	if existing.State == store.TxnPrepared {
		chosenReplica = existing.ChosenReplica
		replicaReady = existing.ReplicaReady
	} else if req.WaitReplica {
		chosenReplica, _ = c.chooseTxnReplica(req.Contract, topics)
		if chosenReplica != "" {
			preview := txnRecordFromReq(req, chosenReplica)
			if err := c.stageTxnReplica(c.nodes[chosenReplica], preview); err == nil {
				replicaReady = true
			} else {
				log.ErrLogger.Warn().Err(err).Str("context", "cluster.txn").Str("node", chosenReplica).Str("batch", req.BatchID).Msg("the replica did not stage the batch in time; it converges at commit and handoff")
			}
		}
	}

	rec := txnRecordFromReq(req, chosenReplica)
	rec.ReplicaReady = replicaReady
	if _, err := store.Txn.Stage(rec, payloads); err != nil {
		if replicaReady {
			c.abortTxnReplica(rec) // the batch is failing to prepare
		}
		if errors.Is(err, store.ErrTxnConflict) {
			resp.Reason = "the batch id is reused with different content"
			return errTxnMismatch
		}
		log.ErrLogger.Error().Err(err).Str("context", "cluster.txn").Str("batch", req.BatchID).Msg("unable to stage a batch")
		return err
	}
	resp.State = 1
	return nil
}

// TxnCommit is the owner's commit handler.
func (c *Cluster) TxnCommit(req *TxnCommitReq, resp *TxnCommitResp) error {
	if !c.txnCapable() {
		return errCapabilityOff(capTxnPublish)
	}
	rec, err := store.Txn.State(req.BatchID)
	if err != nil {
		return err
	}
	if rec.State == store.TxnUnknown {
		resp.Unprepared = true
		return nil
	}
	if rec.State == store.TxnCommitted {
		resp.Committed = true
		return nil
	}

	topics := make([]string, len(rec.Messages))
	for i, m := range rec.Messages {
		topics[i] = m.Topic
	}
	if moved, owner := c.txnMovedTopics(rec.Contract, topics); len(moved) != 0 {
		resp.Moved, resp.Owner = true, owner
		return nil
	}

	committed, payloads, promoted, err := store.Txn.Commit(req.BatchID)
	if err != nil {
		log.ErrLogger.Error().Err(err).Str("context", "cluster.txn").Str("batch", req.BatchID).Msg("unable to commit a batch")
		return err
	}
	resp.Committed = true
	if promoted {
		// Make the other holders durable exactly as a put's replication
		// does: promote only the replica whose staging was confirmed, hint
		// one that was not, then queue the rest and hint holders that
		// cannot take them now, before fanning out.
		if committed.ChosenReplica != "" {
			if committed.ReplicaReady {
				if err := c.commitTxnReplica(&committed); err != nil {
					log.ErrLogger.Warn().Err(err).Str("context", "cluster.txn").Str("node", committed.ChosenReplica).Str("batch", committed.BatchID).Msg("the replica did not answer commit; its handoff converges it")
					c.hintTxnReplica(&committed, payloads)
				}
			} else {
				c.hintTxnReplica(&committed, payloads)
			}
		}
		c.distributeTxnCommitted(&committed, payloads)
		fanoutOwnerBatch(committed, payloads)
	}
	return nil
}

// TxnAbort is the owner's abort handler.
func (c *Cluster) TxnAbort(req *TxnAbortReq, resp *TxnAbortResp) error {
	rec, err := store.Txn.Abort(req.BatchID, req.Reason)
	if err != nil {
		if errors.Is(err, store.ErrTxnCommitted) {
			resp.Committed = true
			return nil
		}
		return err
	}
	if rec.State == store.TxnAborted && rec.ChosenReplica != "" {
		c.abortTxnReplica(&rec)
	}
	return nil
}

// TxnState is the owner's state handler.
func (c *Cluster) TxnState(req *TxnStateReq, resp *TxnStateResp) error {
	rec, err := store.Txn.State(req.BatchID)
	if err != nil {
		return err
	}
	resp.State = uint8(rec.State)
	if len(req.Topics) != 0 {
		if moved, owner := c.txnMovedTopics(req.Contract, req.Topics); len(moved) != 0 {
			resp.Moved, resp.Owner = true, owner
		}
	}
	return nil
}

// ----- replica RPC methods -----

// TxnReplicaPrepare stages a hidden replica copy of the batch.
func (c *Cluster) TxnReplicaPrepare(req *TxnReplicaReq, resp *TxnReplicaResp) error {
	if !c.txnCapable() || !hasCapability(capReplicate) {
		return errCapabilityOff(capTxnPublish)
	}
	// Only a batch whose every id already arrived skips staging: a partially
	// delivered batch is staged in full, and the commit's delivered predicate
	// promotes just the entries that are not visible yet. The ids staged here
	// are deliberately NOT added to seen, so staging cannot suppress the
	// copy it is about to make visible.
	if len(req.Entries) > 0 {
		all := true
		for _, e := range req.Entries {
			if e.ReplicaID == "" || !c.seen.has(e.ReplicaID) {
				all = false
				break
			}
		}
		if all {
			resp.AlreadyReplicated = true
			return nil
		}
	}
	rec := &store.TxnRecord{
		BatchID:     req.BatchID,
		Contract:    req.Contract,
		ContentHash: req.ContentHash,
		WaitReplica: true,
		ReplicaNode: req.Node,
		Messages:    make([]store.TxnMessage, len(req.Entries)),
	}
	for i, e := range req.Entries {
		rec.Messages[i] = store.TxnMessage{Topic: e.Topic, Ttl: e.Ttl, Payload: e.Payload, ReplicaID: e.ReplicaID, Owner: req.Node}
	}
	if _, err := store.Txn.StageReplica(rec, txnPayloads(req.Entries)); err != nil {
		if errors.Is(err, store.ErrTxnConflict) {
			return errTxnMismatch
		}
		return err
	}
	resp.Prepared = true
	return nil
}

// TxnReplicaCommit promotes the staged replica copy.
func (c *Cluster) TxnReplicaCommit(req *TxnReplicaReq, resp *TxnReplicaResp) error {
	rec, promoted, err := store.Txn.CommitReplica(req.BatchID, c.seen.has)
	if err != nil {
		if errors.Is(err, store.ErrTxnUnknown) {
			// The staging expired or never arrived; the owner's hints
			// re-deliver.
			resp.AlreadyReplicated = true
			return nil
		}
		return err
	}
	resp.Committed = true
	if promoted {
		for _, m := range rec.Messages {
			c.seen.add(m.ReplicaID)
		}
	}
	return nil
}

// TxnReplicaAbort discards the staged replica copy.
func (c *Cluster) TxnReplicaAbort(req *TxnReplicaReq, resp *TxnReplicaResp) error {
	_, err := store.Txn.AbortReplica(req.BatchID, "the batch was aborted")
	return err
}

// ----- coordinator-side remote participant -----

// remoteTxnParticipant drives one remote owner.
type remoteTxnParticipant struct {
	cluster *Cluster
	node    *ClusterNode
	req     *TxnPrepareReq
	// precommitted: this owner answered prepare with "already committed"
	// while a ring change was resolved.
	precommitted bool
}

func (p *remoteTxnParticipant) nodeName() string { return p.node.name }

func (p *remoteTxnParticipant) prepare() error {
	if !p.node.supports(capTxnPublish) {
		return errTxnUseLegacy
	}
	var resp TxnPrepareResp
	err := p.node.callTimeout("Cluster.TxnPrepare", p.req, &resp, txnPrepareTimeout)
	if err != nil {
		if p.node.lacks(err, capTxnPublish) || missingMethod(err, capTxnPublish) {
			return errTxnUseLegacy
		}
		if retryable(err) {
			return errTxnRetryable
		}
		log.ErrLogger.Error().Err(err).Str("context", "cluster.txn").Str("node", p.node.name).Str("batch", p.req.BatchID).Msg("prepare failed")
		return types.ErrServerError
	}
	if resp.Moved {
		return errTxnMoved
	}
	if resp.State == 2 {
		p.precommitted = true
	}
	return nil
}

func (p *remoteTxnParticipant) commit() error {
	if p.precommitted {
		return nil
	}
	if !p.node.supports(capTxnPublish) {
		return errTxnUseLegacy
	}
	req := &TxnCommitReq{Node: p.cluster.thisNodeName, BatchID: p.req.BatchID, PinOwner: p.node.name, RingVersion: p.req.RingVersion}
	var resp TxnCommitResp
	err := p.node.callTimeout("Cluster.TxnCommit", req, &resp, txnCommitTimeout)
	if err != nil {
		if p.node.lacks(err, capTxnPublish) || missingMethod(err, capTxnPublish) {
			return errTxnUseLegacy
		}
		// The commit is idempotent: whether the call was processed or not,
		// the durable decision is resolved by a retried commit; report the
		// outcome as uncertain for this attempt, the client retry converges.
		return errTxnUncertain
	}
	if resp.Moved {
		return errTxnMoved
	}
	if resp.Unprepared {
		return errTxnUnprepared
	}
	return nil
}

func (p *remoteTxnParticipant) abort(reason string) {
	if !p.node.supports(capTxnPublish) {
		return
	}
	var resp TxnAbortResp
	_ = p.node.callTimeout("Cluster.TxnAbort", &TxnAbortReq{Node: p.cluster.thisNodeName, BatchID: p.req.BatchID, Reason: reason}, &resp, txnAbortTimeout)
}

// errTxnUncertain marks a commit whose outcome is not known; the durable
// decision makes the coordinator resolve rather than retry blindly.
var errTxnUncertain = errors.New("txn: the commit outcome is uncertain")

func equalHash(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func init() {
	newRemoteTxnParticipant = func(p remoteParticipantParams) (txnParticipant, error) {
		n := p.cluster.nodes[p.node]
		if n == nil {
			return nil, errTxnRetryable
		}
		entries := make([]TxnEntry, len(p.msgs))
		for i, m := range p.msgs {
			entries[i] = TxnEntry{
				Topic:     m.topic.Topic,
				Payload:   m.msg.Payload,
				Ttl:       m.msg.Ttl,
				ReplicaID: replicaID(p.batchID, m.index),
			}
		}
		return &remoteTxnParticipant{
			cluster: p.cluster,
			node:    n,
			req: &TxnPrepareReq{
				Node:         p.origin.Node,
				BatchID:      p.batchID,
				Contract:     p.contract,
				Origin:       ClusterSess{Node: p.origin.Node, ConnID: uid.LID(p.origin.ConnID), SessID: uid.LID(p.origin.SessID), ClientID: p.origin.ClientID, Insecure: p.origin.Insecure},
				MessageID:    uint32(p.messageID),
				DeliveryMode: p.deliveryMode,
				ContentHash:  p.hash,
				RingVersion:  p.ringVersion,
				PinOwner:     p.node,
				WaitReplica:  p.waitReplica,
				Entries:      entries,
			},
		}, nil
	}
}
