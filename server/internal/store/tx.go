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

package store

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/unit-io/unitdb/server/internal/pkg/log"
)

// Transactional multi-topic publish storage.
//
// A batch publish is a two-phase transaction: participants (the nodes that
// own the batch's topics) STAGE its messages under hidden $sys.txstage.*
// topics together with a PREPARED registry record, and only COMMIT moves the
// messages to their real topics. Staged messages are invisible to a topic's
// history, relays, the topic index and subscribers, so a batch that fails to
// prepare leaves nothing behind.
//
// Durability and idempotency rest on these properties of the underlying
// store:
//
//   - puts are taken to the write-ahead log by one flush: the writes up to
//     Flush are rotated into one log and waited for, so after a crash they
//     are recovered together (the ones only still in memory are all lost);
//   - PutWithID takes a pre-allocated id whose seq is never used by anything
//     else. The real message ids are allocated at stage time, after every id
//     already handed out, and their seqs are covered by the staged records'
//     higher seqs, so recovery raises the store sequence past the
//     reservations; promoting the same batch again writes the same id with
//     the same bytes, an idempotent overwrite;
//   - staged records use their own ids, so deleting them never tombstones a
//     promoted message.
//
// Writes within a transition are ordered on purpose. Staging puts the
// messages first and its PREPARED registry record last, and a flush takes
// them: a crash can leave hidden staged copies but never a readable prepared
// batch without them (a retried stage also verifies and refills). Commit
// promotes in id order and writes its COMMITTED record last; a crash during
// promotion leaves a PREPARED batch with part of its messages written, which
// the retried commit (same ids, same bytes) completes - never a permanent
// half batch.
//
// The visibility boundary is the commit point: fan-out to subscribers starts
// only after Commit returns. Reads of the real topics during the promotion
// flush can briefly see part of the batch; that is the store's own isolation
// limit on the success path. A failed or aborted batch never writes its real
// topics at all.
//
// The store's managed Batch API is deliberately not used: its per-batch
// nanosecond memdb blocks interleave with the ordinary truncated time blocks
// and lose ordinary puts on recovery in this configuration.

const (
	txnPrepared  = 1
	txnCommitted = 2
	txnAborted   = 3

	// seq orders a batch's records: the first prepared record, then a
	// terminal one, then another prepared record if an aborted batch is
	// retried, and so on. latestRecord takes the highest.
	txnSeqFirstPrepared = 1
	txnSeqStep          = 1

	// maxTxnTTL bounds how long staged messages, registry and decision
	// records of a batch whose messages never expire are kept for retries
	// and recovery, as replicated messages' ids are.
	maxTxnTTL = 24 * time.Hour
)

var (
	// ErrTxnUnknown is returned when a batch has no record here.
	ErrTxnUnknown = errors.New("store: transaction is not staged here")
	// ErrTxnConflict is returned when a batch id is reused with a different
	// request: a retried publish must carry the same messages.
	ErrTxnConflict = errors.New("store: a different transaction uses the same batch id")
	// ErrTxnCommitted is returned when a committed batch is asked to abort.
	ErrTxnCommitted = errors.New("store: transaction already committed")
	// ErrTxnAborted is returned when an aborted batch is asked to commit.
	ErrTxnAborted = errors.New("store: transaction already aborted")
	// ErrTxnMissing is returned when a staged record a commit looks for is gone.
	ErrTxnMissing = errors.New("store: a staged transaction record is missing")
)

// TxnState is a participant's or coordinator's state of a batch.
type TxnState uint8

const (
	TxnUnknown   TxnState = 0
	TxnPrepared  TxnState = txnPrepared
	TxnCommitted TxnState = txnCommitted
	TxnAborted   TxnState = txnAborted
)

// TxnOrigin identifies the session a batch comes from, as the cluster
// forwards it: its node, connection and session ids, client id, and the
// connection's insecure flag. It is what a participant needs to fan the
// committed messages out exactly as for a locally published one.
type TxnOrigin struct {
	Node     string
	ConnID   uint64
	SessID   uint64
	ClientID []byte
	Insecure bool
}

// TxnMessage is one message of a staged batch. Payload is carried by
// coordinator decision records (which may have to drive the batch elsewhere
// again after a failover); participant registries leave it empty, since the
// staged record under StageTopic holds the bytes.
type TxnMessage struct {
	Topic       string
	Ttl         string
	ExpiresAt   int64
	Payload     []byte
	PayloadHash []byte
	// ID is the id the message is promoted to its real topic with; StageID
	// is the id of its hidden staged record. Both are allocated at stage
	// time. On a replica, they name that replica's own store records.
	ID      []byte
	StageID []byte
	// Owner is the node pinned as the topic's owner when the batch was
	// prepared; "" for a topic owned locally.
	Owner string
	// ReplicaID is the deterministic id the owner's replication and hints
	// use for the message, so a replica stores it once however it reaches it.
	ReplicaID string
}

// TxnRecord is a participant's or coordinator's durable record of a batch.
type TxnRecord struct {
	BatchID      string
	Contract     uint32
	Origin       TxnOrigin
	MessageID    uint32
	DeliveryMode uint8
	ContentHash  []byte
	// RingVersion and the messages' pinned owners fence the commit against a
	// ring change.
	RingVersion int
	WaitReplica bool
	// State and Seq order the record against older records of the batch.
	State TxnState
	Seq   int
	// Reason names why an aborted batch was aborted.
	Reason   string
	Messages []TxnMessage
	// ReplicaNode is the replica participant this record belongs to, for
	// replica registries; "" for an owner.
	ReplicaNode string
	// ChosenReplica is the replica node an owner staged this batch on, so a
	// commit or a recovery after restart reaches the same replica.
	ChosenReplica string
	// ReplicaReady says the chosen replica's hidden staging was confirmed
	// before the owner record was written. When it never answered in time
	// the owner commits without it and hands the entries to it as hints; its
	// possibly-late staging is never promoted, so it cannot duplicate them.
	ReplicaReady bool
	CreatedAt    int64
	UpdatedAt    int64
}

// Txn is the anchor for a batch transaction's staged records and registries.
var Txn TxnStore

// TxnStore stores multi-topic publish transactions.
type TxnStore struct {
	// locks serializes state transitions of one batch within this process,
	// so a retried commit cannot promote alongside the first.
	locks sync.Map // batchID -> *sync.Mutex
}

func (s *TxnStore) batchLock(batchID string) *sync.Mutex {
	m, _ := s.locks.LoadOrStore(batchID, &sync.Mutex{})
	return m.(*sync.Mutex)
}

// stageTopic returns the hidden topic message i of a participant's batch is
// staged under, kind is sysTxStage for an owner and sysTxReplicaStage for a
// replica.
func stageTopic(kind, batchID string, i int) string {
	return sysTopic(kind, batchID+"."+strconv.Itoa(i))
}

// registryTopic returns the topic a participant's batch records are kept
// under.
func registryTopic(kind, batchID string) string {
	return sysTopic(kind, batchID)
}

// indexTopicName returns the fixed topic listing batch ids for recovery.
func indexTopicName(kind string) string {
	return sysTopic(kind, "batches")
}

func encodeRecord(rec *TxnRecord, withPayload bool) ([]byte, error) {
	r := *rec
	if !withPayload {
		r.Messages = make([]TxnMessage, len(rec.Messages))
		for i, m := range rec.Messages {
			m.Payload = nil
			r.Messages[i] = m
		}
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&r); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeRecord(raw []byte) (TxnRecord, error) {
	var rec TxnRecord
	err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&rec)
	return rec, err
}

// latestRecord reads every record of a batch and returns the one with the
// highest Seq (its terminal state, or its latest prepared attempt). Records
// that no longer decode are skipped, as other reads skip bad records.
func latestRecord(kind, batchID string) (TxnRecord, error) {
	raw, err := adp.Get(sysContract, registryTopic(kind, batchID), "")
	if err != nil {
		return TxnRecord{}, err
	}
	var best TxnRecord
	for _, b := range raw {
		rec, derr := decodeRecord(b)
		if derr != nil {
			continue
		}
		if rec.Seq > best.Seq || (rec.Seq == best.Seq && rec.UpdatedAt > best.UpdatedAt) {
			best = rec
		}
	}
	return best, nil
}

// recordBatches lists the distinct batch ids the index of kind holds.
func recordBatches(kind string) ([]string, error) {
	raw, err := adp.Get(sysContract, indexTopicName(kind), "")
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool, len(raw))
	var batches []string
	for _, b := range raw {
		id := string(b)
		if id != "" && !seen[id] {
			seen[id] = true
			batches = append(batches, id)
		}
	}
	return batches, nil
}

// txnTTL is how long the batch's transaction records and staged messages are
// kept: the shortest time any of its messages still has to live, bounded by
// maxTxnTTL, so a batch whose messages expire leaves nothing behind them.
func txnTTL(rec *TxnRecord) string {
	left := maxTxnTTL
	now := time.Now()
	for _, m := range rec.Messages {
		if m.ExpiresAt != 0 {
			if d := time.Unix(m.ExpiresAt, 0).Sub(now); d < left {
				left = d
			}
		}
	}
	secs := int64(left/time.Second) + 1
	if secs < 1 {
		secs = 1
	}
	return strconv.FormatInt(secs, 10)
}

// remainingTTL is the store ttl of a promoted message with expiry expiresAt:
// "" for one that never expires.
func remainingTTL(expiresAt int64) string {
	if expiresAt == 0 {
		return ""
	}
	secs := expiresAt - time.Now().Unix()
	if secs < 1 {
		secs = 1
	}
	return strconv.FormatInt(secs, 10)
}

// prepareStaged allocates a batch's ids, fills its messages' expiry, stages
// the messages with a PREPARED record, and flushes the transition to the
// log. It is shared by owner and replica participants.
func (s *TxnStore) prepareStaged(rec *TxnRecord, payloads [][]byte, stageKind, regKind, indexKind string) (*TxnRecord, error) {
	if len(rec.Messages) == 0 {
		return nil, errors.New("store: cannot stage an empty transaction")
	}
	lock := s.batchLock(rec.BatchID)
	lock.Lock()
	defer lock.Unlock()

	existing, err := latestRecord(regKind, rec.BatchID)
	if err != nil {
		return nil, err
	}
	if existing.State != TxnUnknown && !bytes.Equal(existing.ContentHash, rec.ContentHash) {
		// Never let a different request take over a batch id.
		return nil, ErrTxnConflict
	}
	if existing.State == TxnCommitted {
		return &existing, nil
	}

	now := time.Now().Unix()
	rec.State = TxnPrepared
	rec.UpdatedAt = now
	switch existing.State {
	case TxnAborted:
		// The batch was aborted and the client retries it with the same
		// content: a fresh attempt runs, ordered after the abort.
		rec.Seq = existing.Seq + txnSeqStep
		rec.CreatedAt = now
	case TxnPrepared:
		rec.Seq = existing.Seq
		rec.CreatedAt = existing.CreatedAt
	default:
		rec.Seq = txnSeqFirstPrepared
		if rec.CreatedAt == 0 {
			rec.CreatedAt = now
		}
	}
	for i := range rec.Messages {
		m := &rec.Messages[i]
		if len(m.Payload) == 0 {
			if i >= len(payloads) {
				return nil, fmt.Errorf("store: staged message %d has no payload", i)
			}
			m.Payload = payloads[i]
		}
	}

	if existing.State == TxnPrepared {
		// Re-prepare: keep the ids and expiry already allocated on disk, and
		// refill any staged record a torn flush did not take to the log.
		if len(existing.Messages) != len(rec.Messages) {
			return nil, ErrTxnConflict
		}
		refill := false
		for i := range rec.Messages {
			em, m := &existing.Messages[i], &rec.Messages[i]
			m.ID, m.StageID, m.ExpiresAt, m.Owner, m.ReplicaID = em.ID, em.StageID, em.ExpiresAt, em.Owner, em.ReplicaID
			raw, err := adp.Get(rec.Contract, stageTopic(stageKind, rec.BatchID, i), "")
			if err != nil {
				return nil, err
			}
			if len(raw) == 0 {
				refill = true
				continue
			}
			if m.ExpiresAt == 0 {
				m.ExpiresAt = ExpiresAt(m.Ttl)
			}
		}
		ttl := txnTTL(rec)
		if refill {
			for i := range rec.Messages {
				m := &rec.Messages[i]
				if err := adp.PutWithID(rec.Contract, m.StageID, stageTopic(stageKind, rec.BatchID, i), wrap(m.Payload, m.ExpiresAt), ttl); err != nil {
					return nil, err
				}
			}
		}
		return s.writePrepared(rec, regKind, indexKind, ttl)
	}

	for i := range rec.Messages {
		m := &rec.Messages[i]
		if m.ExpiresAt == 0 {
			m.ExpiresAt = ExpiresAt(m.Ttl)
		}
	}

	// Allocate the messages' real ids first: nothing writes those seqs but a
	// promotion. The staged records' ids, allocated after, have higher seqs
	// in the same store: recovered from the write-ahead log they raise the
	// store sequence past the real id reservations.
	for i := range rec.Messages {
		id, err := adp.NewID()
		if err != nil {
			return nil, err
		}
		rec.Messages[i].ID = id
	}
	for i := range rec.Messages {
		id, err := adp.NewID()
		if err != nil {
			return nil, err
		}
		rec.Messages[i].StageID = id
	}

	ttl := txnTTL(rec)

	// Staged messages first, the registry record last, then one flush takes
	// the whole transition to the log.
	for i := range rec.Messages {
		m := &rec.Messages[i]
		if err := adp.PutWithID(rec.Contract, m.StageID, stageTopic(stageKind, rec.BatchID, i), wrap(m.Payload, m.ExpiresAt), ttl); err != nil {
			return nil, err
		}
	}
	return s.writePrepared(rec, regKind, indexKind, ttl)
}

// writePrepared writes the PREPARED registry record and the recovery index
// entry and flushes the transition to the write-ahead log.
func (s *TxnStore) writePrepared(rec *TxnRecord, regKind, indexKind, ttl string) (*TxnRecord, error) {
	registryRaw, err := encodeRecord(rec, false)
	if err != nil {
		return nil, err
	}
	if err := adp.Put(sysContract, registryTopic(regKind, rec.BatchID), registryRaw, ttl); err != nil {
		return nil, err
	}
	if err := adp.Put(sysContract, indexTopicName(indexKind), []byte(rec.BatchID), ttl); err != nil {
		return nil, err
	}
	if err := adp.Flush(); err != nil {
		return nil, err
	}
	return rec, nil
}

// commitStaged promotes a participant's staged messages to their real topics,
// writes the COMMITTED record (flushed last), then removes the staged
// records. It is idempotent: a committed batch commits again as a no-op.
// promoted is true only when this call made the prepared->committed
// transition; promotedPayloads then holds the plain payload promoted for
// each message of the record, nil for an expired one, so the caller can fan
// the batch out exactly once.
func (s *TxnStore) commitStaged(batchID, stageKind, regKind, indexKind, targetKind string, afterPromote func(TxnMessage) error, skip func(TxnMessage) bool) (rec TxnRecord, promotedPayloads [][]byte, promoted bool, err error) {
	lock := s.batchLock(batchID)
	lock.Lock()
	defer lock.Unlock()

	rec, err = latestRecord(regKind, batchID)
	if err != nil {
		return TxnRecord{}, nil, false, err
	}
	if rec.State == TxnUnknown {
		return TxnRecord{}, nil, false, ErrTxnUnknown
	}
	if rec.State == TxnCommitted {
		return rec, nil, false, nil
	}
	if rec.State == TxnAborted {
		return rec, nil, false, ErrTxnAborted
	}

	now := time.Now()
	committed := rec
	committed.State = TxnCommitted
	committed.Seq = rec.Seq + txnSeqStep
	committed.UpdatedAt = now.Unix()
	promotedPayloads = make([][]byte, len(rec.Messages))

	// Promote in allocation order, with the pre-allocated ids: re-promoting
	// after a crash writes the same id and bytes, an idempotent overwrite.
	// The COMMITTED record is written and flushed only after every message
	// is promoted, so a crash leaves a PREPARED batch that the retried commit
	// completes - never a permanent half batch.
	indexed := make(map[string]bool, len(rec.Messages))
	var hookMessages []TxnMessage
	for i := range rec.Messages {
		m := &rec.Messages[i]
		if expired(m.ExpiresAt, now) {
			continue
		}
		if skip != nil && skip(*m) {
			// Another path already put this message where the promotion
			// would put it (a hint or replication beat the staged commit):
			// just drop the hidden staged copy, never promote a second one.
			continue
		}
		raw, err := adp.Get(rec.Contract, stageTopic(stageKind, batchID, i), "")
		if err != nil {
			return rec, nil, false, err
		}
		if len(raw) != 1 {
			return rec, nil, false, ErrTxnMissing
		}
		target := m.Topic
		if targetKind != "" {
			target = sysTopic(targetKind, m.Topic)
		}
		if err := adp.PutWithID(rec.Contract, m.ID, target, raw[0], remainingTTL(m.ExpiresAt)); err != nil {
			return rec, nil, false, err
		}
		if payload, _, known := unwrap(raw[0]); known {
			promotedPayloads[i] = payload
		} else {
			promotedPayloads[i] = raw[0]
		}
		hookMessages = append(hookMessages, *m)
		if name := topicName(m.Topic); !indexed[name] {
			indexed[name] = true
			b := make([]byte, 4+len(name))
			binaryPutUint32(b[:4], rec.Contract)
			copy(b[4:], name)
			if err := adp.Put(sysContract, sysTopic(sysIndex, topicIndexTopic), b, ""); err != nil {
				return rec, nil, false, err
			}
		}
	}
	registryRaw, err := encodeRecord(&committed, false)
	if err != nil {
		return rec, nil, false, err
	}
	ttl := txnTTL(&rec)
	if err := adp.Put(sysContract, registryTopic(regKind, batchID), registryRaw, ttl); err != nil {
		return rec, nil, false, err
	}
	if err := adp.Put(sysContract, indexTopicName(indexKind), []byte(batchID), ttl); err != nil {
		return rec, nil, false, err
	}
	if err := adp.Flush(); err != nil {
		return rec, nil, false, err
	}

	// The promotion and its commit record are durable: run the post-commit
	// hooks (recording replicated ids), make the topics known in this
	// process's index and drop the staged records. A failed drop is
	// harmless: the record is committed and the staged copy is hidden and
	// ttl-bounded.
	if afterPromote != nil {
		for _, m := range hookMessages {
			if err := afterPromote(m); err != nil {
				logTxDropped(batchID, err)
			}
		}
	}
	for name := range indexed {
		rememberTopic(rec.Contract, name)
	}
	for i := range rec.Messages {
		m := &rec.Messages[i]
		if err := adp.Delete(rec.Contract, m.StageID, stageTopic(stageKind, batchID, i)); err != nil {
			logTxDropped(batchID, err)
		}
	}
	return committed, promotedPayloads, true, nil
}

// abortStaged writes an ABORTED record and removes a participant's staged
// messages. It is idempotent: aborting an unknown or already aborted batch
// does nothing, and a committed batch cannot be aborted. The abort record is
// flushed before this returns, so a delayed commit after a restart is fenced
// by it.
func (s *TxnStore) abortStaged(batchID, stageKind, regKind, indexKind, reason string) (TxnRecord, error) {
	lock := s.batchLock(batchID)
	lock.Lock()
	defer lock.Unlock()

	rec, err := latestRecord(regKind, batchID)
	if err != nil {
		return TxnRecord{}, err
	}
	switch rec.State {
	case TxnUnknown:
		// Nothing was ever staged here: nothing to abort and nothing a
		// delayed commit could promote.
		return TxnRecord{BatchID: batchID, State: TxnAborted}, nil
	case TxnCommitted:
		return rec, ErrTxnCommitted
	case TxnAborted:
		return rec, nil
	}

	aborted := rec
	aborted.State = TxnAborted
	aborted.Seq = rec.Seq + txnSeqStep
	aborted.Reason = reason
	aborted.UpdatedAt = time.Now().Unix()
	ttl := txnTTL(&aborted)
	raw, err := encodeRecord(&aborted, false)
	if err != nil {
		return rec, err
	}
	if err := adp.Put(sysContract, registryTopic(regKind, batchID), raw, ttl); err != nil {
		return rec, err
	}
	if err := adp.Put(sysContract, indexTopicName(indexKind), []byte(batchID), ttl); err != nil {
		return rec, err
	}
	if err := adp.Flush(); err != nil {
		return rec, err
	}
	for i := range rec.Messages {
		m := &rec.Messages[i]
		if err := adp.Delete(rec.Contract, m.StageID, stageTopic(stageKind, batchID, i)); err != nil {
			logTxDropped(batchID, err)
		}
	}
	return aborted, nil
}

// topicName returns a topic without its options, as the topic index keeps it.
func topicName(topic string) string {
	if i := strings.IndexByte(topic, '?'); i >= 0 {
		return topic[:i]
	}
	return topic
}

// rememberTopic records in the in-memory topic index that this node stores
// messages for topic; the durable index entry was written with the promotion.
func rememberTopic(contract uint32, topic string) {
	if i := strings.IndexByte(topic, '?'); i >= 0 {
		topic = topic[:i]
	}
	topics.Lock()
	topics.seen[TopicRef{Contract: contract, Topic: topic}] = true
	topics.Unlock()
}

// binaryPutUint32 writes v into b little-endian; it is encoding/binary in
// small to keep this file's import list focused.
func binaryPutUint32(b []byte, v uint32) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
}

// logTxDropped records a staged record that could not be deleted after its
// batch ended; the record is hidden and ttl-bounded.
func logTxDropped(batchID string, err error) {
	log.ErrLogger.Error().Err(err).Str("context", "store.Txn").Str("batch", batchID).Msg("unable to delete a staged transaction record")
}

// ----- Owner participant -----

// Stage stages an owner participant's batch: the messages are stored under
// hidden per-message topics with a PREPARED registry record, all as one
// durable batch. rec's messages are filled with their allocated ids and
// expiry. Re-staging a batch already prepared or committed with the same
// content is idempotent.
func (s *TxnStore) Stage(rec *TxnRecord, payloads [][]byte) (*TxnRecord, error) {
	return s.prepareStaged(rec, payloads, sysTxStage, sysTxRegistry, sysTxIndex)
}

// State returns this node's participant state of an owner batch.
func (s *TxnStore) State(batchID string) (TxnRecord, error) {
	return latestRecord(sysTxRegistry, batchID)
}

// Commit promotes an owner batch's staged messages to their real topics. It
// returns the record, the plain payload promoted for each message (nil for
// an expired one), whether this call performed the promotion, and an error.
// A batch already committed returns (record, nil, false, nil).
func (s *TxnStore) Commit(batchID string) (TxnRecord, [][]byte, bool, error) {
	return s.commitStaged(batchID, sysTxStage, sysTxRegistry, sysTxIndex, "", nil, nil)
}

// Abort aborts an owner batch.
func (s *TxnStore) Abort(batchID, reason string) (TxnRecord, error) {
	return s.abortStaged(batchID, sysTxStage, sysTxRegistry, sysTxIndex, reason)
}

// ----- Replica participant -----

// StageReplica stages a replica participant's batch.
func (s *TxnStore) StageReplica(rec *TxnRecord, payloads [][]byte) (*TxnRecord, error) {
	return s.prepareStaged(rec, payloads, sysTxReplicaStage, sysTxReplicaRegistry, sysTxReplicaIndex)
}

// ReplicaState returns this node's replica state of a batch.
func (s *TxnStore) ReplicaState(batchID string) (TxnRecord, error) {
	return latestRecord(sysTxReplicaRegistry, batchID)
}

// CommitReplica promotes a replica batch's staged messages to the replica
// namespace and records their deterministic replication ids, so they are
// stored once however they reach this node afterwards. delivered reports
// whether an entry's ReplicaID is already held here: a hint or regular
// replication that beat this commit already put the message there, so that
// entry is only unstaged, never promoted again. It reports whether this call
// performed the promotion.
func (s *TxnStore) CommitReplica(batchID string, delivered func(replicaID string) bool) (TxnRecord, bool, error) {
	rec, _, promoted, err := s.commitStaged(batchID, sysTxReplicaStage, sysTxReplicaRegistry, sysTxReplicaIndex, sysReplicas, func(m TxnMessage) error {
		if m.ReplicaID == "" {
			return nil
		}
		// Recorded after the message, as Cluster.Replicate does: a crash
		// between the two stores it twice at worst, never zero times.
		return Seen.Put(m.ReplicaID, m.ExpiresAt)
	}, func(m TxnMessage) bool {
		return m.ReplicaID != "" && delivered != nil && delivered(m.ReplicaID)
	})
	return rec, promoted, err
}

// AbortReplica aborts a replica batch.
func (s *TxnStore) AbortReplica(batchID, reason string) (TxnRecord, error) {
	return s.abortStaged(batchID, sysTxReplicaStage, sysTxReplicaRegistry, sysTxReplicaIndex, reason)
}

// ----- Coordinator decision -----

// SaveDecision writes the coordinator's decision record for a batch
// (prepared, committed or aborted), flushed to the write-ahead log. The
// record carries the batch's payloads: if this node has to drive the batch
// again after a failover, a new owner may need them. The caller sets the
// record's state and seq (from the record Decision returns).
func (s *TxnStore) SaveDecision(rec *TxnRecord) error {
	lock := s.batchLock("decision:" + rec.BatchID)
	lock.Lock()
	defer lock.Unlock()

	if rec.Seq == 0 {
		rec.Seq = txnSeqFirstPrepared
	}
	rec.UpdatedAt = time.Now().Unix()
	if rec.CreatedAt == 0 {
		rec.CreatedAt = rec.UpdatedAt
	}
	ttl := txnTTL(rec)
	raw, err := encodeRecord(rec, true)
	if err != nil {
		return err
	}
	if err := adp.Put(sysContract, registryTopic(sysTxDecision, rec.BatchID), raw, ttl); err != nil {
		return err
	}
	if err := adp.Put(sysContract, indexTopicName(sysTxDecisionIndex), []byte(rec.BatchID), ttl); err != nil {
		return err
	}
	return adp.Flush()
}

// Decision returns the coordinator's decision record for a batch, if this
// node recorded one.
func (s *TxnStore) Decision(batchID string) (TxnRecord, error) {
	return latestRecord(sysTxDecision, batchID)
}

// Recover lists the batches this node took part in that still have a record:
// owner participants' and replica participants' batches, and the batches this
// node coordinated, each as its latest record. Records that already expired
// are simply absent.
func (s *TxnStore) Recover() (owners, replicas, decisions []TxnRecord, err error) {
	owners, err = recoverBatches(sysTxIndex, sysTxRegistry)
	if err != nil {
		return nil, nil, nil, err
	}
	replicas, err = recoverBatches(sysTxReplicaIndex, sysTxReplicaRegistry)
	if err != nil {
		return nil, nil, nil, err
	}
	decisions, err = recoverBatches(sysTxDecisionIndex, sysTxDecision)
	if err != nil {
		return nil, nil, nil, err
	}
	return owners, replicas, decisions, nil
}

func recoverBatches(indexKind, regKind string) ([]TxnRecord, error) {
	batches, err := recordBatches(indexKind)
	if err != nil {
		return nil, err
	}
	var recs []TxnRecord
	for _, id := range batches {
		rec, err := latestRecord(regKind, id)
		if err != nil {
			return recs, err
		}
		if rec.State != TxnUnknown {
			recs = append(recs, rec)
		}
	}
	return recs, nil
}
