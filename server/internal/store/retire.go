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

// A contract's retirement removes, by its generation, everything the store
// holds of the contract but the persistent retirement itself, which stays in
// the security state: its messages and their reliable replicas, its entries
// of the topic index, its subscriptions, the replica hints and the replicated
// messages' dedup ids that refer to it, and its sessions' rows and logs.
//
// Every operation is scoped by the contract: a record under contract 0, the
// node's own ($sys) namespace, is removed only when it names the contract
// (an index entry, a hint's entry, a session row naming it). Records of
// other contracts and TTL expiry are untouched. Each removal is idempotent:
// a node that resumes an interrupted retirement runs them again and changes
// nothing the second time. Nothing here is concurrent with the contract's
// writes: the cluster bars them before it drains and purges.

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"

	"github.com/unit-io/unitdb/server/internal/pkg/log"
)

// retireBatch reads up to the store's most records at once.
const retireBatch = "?last=100000"

// retireSubTopic matches every subscription topic of a contract:
// "$sys.sub..." is a multi-wildcard query for "$sys.sub.<anything>".
var retireSubTopic = sysTopic(sysSubscriptions, "...")

// RetireLeftovers is what a node still holds of a retiring contract: the
// resources left after a purge, or found by a scan. Every field is zero when
// the retirement cleared the node.
type RetireLeftovers struct {
	Messages      int `json:"messages"`
	Replicas      int `json:"replicas"`
	Index         int `json:"index"`
	Subscriptions int `json:"subscriptions"`
	Hints         int `json:"hints"`
	Seen          int `json:"seen"`
	SessionRows   int `json:"session_rows"`
	SessionLogs   int `json:"session_logs"`
}

// Empty reports whether nothing is left.
func (l RetireLeftovers) Empty() bool {
	return l == RetireLeftovers{}
}

// Map reports the leftovers by resource, for diagnostics, omitting the zero
// ones.
func (l RetireLeftovers) Map() map[string]int {
	m := make(map[string]int)
	add := func(name string, n int) {
		if n > 0 {
			m[name] = n
		}
	}
	add("messages", l.Messages)
	add("replicas", l.Replicas)
	add("index", l.Index)
	add("subscriptions", l.Subscriptions)
	add("hints", l.Hints)
	add("seen", l.Seen)
	add("session_rows", l.SessionRows)
	add("session_logs", l.SessionLogs)
	return m
}

// deleteAllAt deletes every record on contract under topic, an exact topic,
// looping by batches until none are left.
func deleteAllAt(contract uint32, topic string) error {
	for {
		ids, _, err := adp.GetWithIDs(contract, topic+retireBatch)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		for _, id := range ids {
			if err := adp.Delete(contract, id, topic); err != nil {
				return err
			}
		}
		// Persist the tombstones before reading again, so an interrupted
		// purge does not read the entries again after a restart.
		if err := adp.Flush(); err != nil {
			return err
		}
	}
}

// countAt counts the records on contract under topic, up to a batch.
func countAt(contract uint32, topic string) int {
	ids, _, err := adp.GetWithIDs(contract, topic+retireBatch)
	if err != nil {
		log.ErrLogger.Error().Err(err).Str("context", "store.retire").Uint32("contract", contract).Str("topic", topic).Msg("unable to scan a retiring contract")
	}
	return len(ids)
}

// Retire removes the contract's messages, their reliable replicas and the
// contract's entries of the topic index.
func (m *MessageStore) Retire(contract uint32) error {
	topics.Lock()
	refs := make([]TopicRef, 0)
	for ref := range topics.seen {
		if ref.Contract == contract {
			refs = append(refs, ref)
		}
	}
	topics.Unlock()
	for _, ref := range refs {
		if err := deleteAllAt(contract, ref.Topic); err != nil {
			return err
		}
		if err := deleteAllAt(contract, sysTopic(sysReplicas, ref.Topic)); err != nil {
			return err
		}
	}
	if err := removeFromIndex(contract); err != nil {
		return err
	}
	topics.Lock()
	for _, ref := range refs {
		delete(topics.seen, ref)
	}
	topics.Unlock()
	return nil
}

// indexTopicStored is the topic the index records are kept under.
func indexTopicStored() string {
	return sysTopic(sysIndex, topicIndexTopic)
}

// removeFromIndex removes the index records naming the contract. Records of
// other contracts stay, so the scan re-queries only while a batch deletes
// one.
func removeFromIndex(contract uint32) error {
	prefix := make([]byte, 4)
	binary.LittleEndian.PutUint32(prefix, contract)
	for {
		ids, recs, err := adp.GetWithIDs(sysContract, indexTopicStored()+retireBatch)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		deleted := 0
		for i, b := range recs {
			if len(b) >= 4 && bytes.Equal(b[:4], prefix) {
				if err := adp.Delete(sysContract, ids[i], indexTopicStored()); err != nil {
					return err
				}
				deleted++
			}
		}
		if err := adp.Flush(); err != nil {
			return err
		}
		if deleted == 0 {
			// Older records the batch did not reach could only exist past
			// the store's query limit; the index holds one small record per
			// topic, far below it.
			return nil
		}
	}
}

// countIndex counts the index records naming the contract.
func countIndex(contract uint32) int {
	prefix := make([]byte, 4)
	binary.LittleEndian.PutUint32(prefix, contract)
	_, recs, err := adp.GetWithIDs(sysContract, indexTopicStored()+retireBatch)
	if err != nil {
		log.ErrLogger.Error().Err(err).Str("context", "store.retire").Msg("unable to scan the topic index")
		return 0
	}
	n := 0
	for _, b := range recs {
		if len(b) >= 4 && bytes.Equal(b[:4], prefix) {
			n++
		}
	}
	return n
}

// Retire removes every subscription of the contract, with a wildcard query:
// their topics need not be in the topic index, as a subscription may have no
// message.
func (s *SubscriptionStore) Retire(contract uint32) (int, error) {
	n, err := adp.DeleteMatching(contract, retireSubTopic)
	if err != nil {
		return n, err
	}
	return n, adp.Flush()
}

// hintEntryShape mirrors the cluster's replicaHint enough to read what a hint
// refers to: a message's contract, or a session log change's session. gob
// matches the fields by name, so it decodes the cluster's type although this
// package cannot name it. The fields must be named, not embedded: gob
// flattens an anonymous struct field and would misread the nesting.
type hintEntryPayload struct {
	ID        string
	Contract  uint32
	Topic     string
	Payload   []byte
	Ttl       string
	ExpiresAt int64
}

type hintEntryShape struct {
	ID    []byte
	Entry hintEntryPayload
	Op    *LogOp
}

// hintMatches decodes a hint and reports whether it refers to the contract:
// a message of it, or a change of one of its sessions' logs.
func hintMatches(raw []byte, contract uint32, sessions map[uint32]bool) (messageID string, sessionHint bool, matches bool) {
	var h hintEntryShape
	if err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&h); err != nil {
		return "", false, false
	}
	if h.Op != nil {
		return "", true, sessions != nil && sessions[h.Op.Block]
	}
	if h.Entry.Contract == contract {
		return h.Entry.ID, false, true
	}
	return "", false, false
}

// scanHints reads the hints kept for nodes and applies fn to the ones
// referring to the contract. fn may delete the hint. A read-only scan
// (fn nil) makes one pass per node; a deleting one repeats while a batch
// matched and was removed, stopping on a batch that matched none: the other
// hints survive and are returned first.
func scanHints(nodes []string, contract uint32, sessions map[uint32]bool, fn func(node string, id []byte, messageID string) error) (matched int, seenIDs []string, err error) {
	for _, node := range nodes {
		topic := hintTopic(node)
		for {
			ids, recs, err := adp.GetWithIDs(sysContract, topic+retireBatch)
			if err != nil {
				return matched, seenIDs, err
			}
			if len(ids) == 0 {
				break
			}
			round := 0
			for i, b := range recs {
				messageID, sessionHint, ok := hintMatches(b, contract, sessions)
				if !ok {
					continue
				}
				if fn != nil {
					if err := fn(node, ids[i], messageID); err != nil {
						return matched, seenIDs, err
					}
					round++
				}
				matched++
				if !sessionHint && messageID != "" {
					seenIDs = append(seenIDs, messageID)
				}
			}
			// A read-only scan counts one pass; a deleting one repeats only
			// while it removed a matched hint from the batch.
			if fn == nil || round == 0 {
				break
			}
		}
	}
	return matched, seenIDs, nil
}

// RetireHints deletes the replica hints referring to the contract: messages
// of it, and log changes of sessions named by blocks. It returns the dedup
// ids of the contract's messages the hints named, for RetireSeen.
func RetireHints(contract uint32, nodes []string, blocks map[uint32]bool) ([]string, error) {
	_, ids, err := scanHints(nodes, contract, blocks, func(node string, id []byte, _ string) error {
		return adp.Delete(sysContract, id, hintTopic(node))
	})
	if err != nil {
		return ids, err
	}
	return ids, adp.Flush()
}

// countHints counts the replica hints referring to the contract.
func countHints(contract uint32, nodes []string, blocks map[uint32]bool) int {
	n, _, err := scanHints(nodes, contract, blocks, nil)
	if err != nil {
		log.ErrLogger.Error().Err(err).Str("context", "store.retire").Msg("unable to scan hints")
	}
	return n
}

// RetireSeen deletes the dedup ids of replicated messages of the contract, as
// the purged hints named them. An id not named any more already expired: a
// dedup id lives no longer than its message, at most 24h.
func RetireSeen(ids []string) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	want := make(map[string]bool, len(ids))
	for _, id := range ids {
		want[id] = true
	}
	topic := sysTopic(sysSeen, seenTopic)
	deleted := 0
	for {
		stored, vals, err := adp.GetWithIDs(sysContract, topic+retireBatch)
		if err != nil {
			return deleted, err
		}
		if len(stored) == 0 {
			return deleted, nil
		}
		round := 0
		for i, v := range vals {
			if want[string(v)] {
				if err := adp.Delete(sysContract, stored[i], topic); err != nil {
					return deleted, err
				}
				round++
				deleted++
			}
		}
		if err := adp.Flush(); err != nil {
			return deleted, err
		}
		if round == 0 {
			return deleted, nil
		}
	}
}

// sessionRowContractOffset is where a session row names its contract, in the
// 16-byte rows written since retirement was introduced: session id (4),
// owner (8), contract (4). Rows of 12 bytes, written before, do not.
const sessionRowContractOffset = 12

// contractSessions scans the session rows naming the contract: their keys and
// session ids (blocks). A row written before rows named their contract is not
// attributable and stays.
func contractSessions(contract uint32) (rowKeys []uint64, blocks map[uint32]bool) {
	blocks = make(map[uint32]bool)
	for _, key := range adp.Keys() {
		if key&(1<<63) == 0 {
			continue // a log key, not a row
		}
		raw, err := adp.GetMessage(key)
		if err != nil || len(raw) < sessionRowContractOffset+4 {
			continue
		}
		if binary.LittleEndian.Uint32(raw[sessionRowContractOffset:]) == contract {
			rowKeys = append(rowKeys, key)
			blocks[binary.LittleEndian.Uint32(raw[:4])] = true
		}
	}
	return rowKeys, blocks
}

// RetireSessions deletes the contract's session rows, and the logs of the
// sessions they name, and returns the sessions (blocks) they named, with how
// many rows and logs it deleted. Logs are deleted after the rows: deleting a
// row first never leaves a log taken for a session again.
func RetireSessions(contract uint32) (blocks map[uint32]bool, rows, logs int, err error) {
	rowKeys, blocks := contractSessions(contract)
	for _, key := range rowKeys {
		if err := adp.DeleteMessage(key); err != nil {
			return blocks, rows, logs, err
		}
		rows++
	}
	if len(blocks) > 0 {
		for _, key := range adp.Keys() {
			if key&(1<<63) == 0 && blocks[uint32(key)] {
				if err := adp.DeleteMessage(key); err != nil {
					return blocks, rows, logs, err
				}
				logs++
			}
		}
	}
	return blocks, rows, logs, adp.Flush()
}

// countSessions counts the contract's session rows and the logs of the
// sessions they name.
func countSessions(contract uint32) (rows, logs int) {
	rowKeys, blocks := contractSessions(contract)
	rows = len(rowKeys)
	if len(blocks) > 0 {
		for _, key := range adp.Keys() {
			if key&(1<<63) == 0 && blocks[uint32(key)] {
				logs++
			}
		}
	}
	return rows, logs
}

// RetireProgress is this node's progress through one contract's retirement:
// the generation it is for, the phase it confirmed (1 barred, 2 drained,
// 3 purged), and the sessions whose logs the purge removed, kept so that an
// interrupted purge still finds their hints after their rows are gone.
type RetireProgress struct {
	Contract   uint32   `json:"contract"`
	Generation uint64   `json:"generation"`
	Phase      int      `json:"phase"`
	Blocks     []uint32 `json:"blocks,omitempty"`
}

// ContractSessionBlocks returns the ids of the sessions the contract's
// session rows name, so its logs and their hints can be removed with the
// rows.
func ContractSessionBlocks(contract uint32) []uint32 {
	_, blocks := contractSessions(contract)
	out := make([]uint32, 0, len(blocks))
	for b := range blocks {
		out = append(out, b)
	}
	return out
}

func retireProgressTopic() string {
	return sysTopic(sysRetire, "progress")
}

// SaveRetireProgress writes the node's phase of the contract's retirement and
// waits for it to reach the store's log, so a restart resumes from it. An
// earlier record of the same contract is removed; a crash in between leaves
// both and RetireProgressAll takes the further phase.
func SaveRetireProgress(p RetireProgress) error {
	id, err := adp.NewID()
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(p); err != nil {
		return err
	}
	if err := adp.PutWithID(sysContract, id, retireProgressTopic(), buf.Bytes(), ""); err != nil {
		return err
	}
	if err := adp.Flush(); err != nil {
		return err
	}
	// Remove earlier records of this contract.
	old, recs, err := adp.GetWithIDs(sysContract, retireProgressTopic()+retireBatch)
	if err != nil {
		return err
	}
	for i, b := range recs {
		var prev RetireProgress
		if gob.NewDecoder(bytes.NewReader(b)).Decode(&prev) == nil && prev.Contract == p.Contract && !bytes.Equal(old[i], id) {
			adp.Delete(sysContract, old[i], retireProgressTopic())
		}
	}
	return nil
}

// RetireProgressAll reads every progress record kept, the furthest phase per
// contract.
func RetireProgressAll() ([]RetireProgress, error) {
	_, raw, err := adp.GetWithIDs(sysContract, retireProgressTopic()+retireBatch)
	if err != nil {
		return nil, err
	}
	byContract := make(map[uint32]RetireProgress)
	for _, b := range raw {
		var p RetireProgress
		if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&p); err != nil {
			log.ErrLogger.Error().Err(err).Str("context", "store.retire").Msg("unreadable retirement progress: skipped")
			continue
		}
		if cur, ok := byContract[p.Contract]; !ok || p.Phase > cur.Phase {
			byContract[p.Contract] = p
		}
	}
	out := make([]RetireProgress, 0, len(byContract))
	for _, p := range byContract {
		out = append(out, p)
	}
	return out, nil
}

// hintNodes are the nodes hints may be kept for: the configured cluster's
// members, or none on a standalone server. Set by the cluster on start; nil
// leaves nothing to scan, as a standalone node keeps no hints.
var hintNodesFunc func() []string

func hintNodes() []string {
	if hintNodesFunc != nil {
		return hintNodesFunc()
	}
	return nil
}

// SetHintNodes sets how the store lists the nodes hints may be kept for.
func SetHintNodes(fn func() []string) {
	hintNodesFunc = fn
}

// HintNodes lists the nodes hints may be kept for.
func HintNodes() []string {
	return hintNodes()
}

// RetireContract runs every local removal of a contract's retirement, and
// returns what the node still holds afterwards, which is nothing on success.
// blocks, when the session rows were already purged, names the sessions the
// hints still refer to, so those hints are not left behind.
func RetireContract(contract uint32, blocks map[uint32]bool) (RetireLeftovers, error) {
	var left RetireLeftovers
	if err := Message.Retire(contract); err != nil {
		return left, err
	}
	if _, err := Subscription.Retire(contract); err != nil {
		return left, err
	}
	sessBlocks, _, _, err := RetireSessions(contract)
	if err != nil {
		return left, err
	}
	if blocks == nil {
		blocks = sessBlocks
	} else {
		for b := range sessBlocks {
			blocks[b] = true
		}
	}
	nodes := hintNodes()
	seenIDs, err := RetireHints(contract, nodes, blocks)
	if err != nil {
		return left, err
	}
	if _, err := RetireSeen(seenIDs); err != nil {
		return left, err
	}
	left = ScanRetire(contract, nodes, blocks)
	return left, nil
}

// ScanRetire reports what the node still holds of a retiring contract,
// deleting nothing: messages, replicas and the index, subscriptions, hints
// and sessions. blocks names sessions whose rows were already purged, so
// their hints still count.
func ScanRetire(contract uint32, nodes []string, blocks map[uint32]bool) RetireLeftovers {
	var left RetireLeftovers
	for _, ref := range Message.Topics() {
		if ref.Contract != contract {
			continue
		}
		left.Messages += countAt(contract, ref.Topic)
		left.Replicas += countAt(contract, sysTopic(sysReplicas, ref.Topic))
	}
	left.Index = countIndex(contract)
	n, err := adp.CountMatching(contract, retireSubTopic)
	if err != nil {
		log.ErrLogger.Error().Err(err).Str("context", "store.ScanRetire").Msg("unable to scan subscriptions")
	}
	left.Subscriptions = n
	left.Hints = countHints(contract, nodes, blocks)
	left.SessionRows, left.SessionLogs = countSessions(contract)
	return left
}
