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

package unitdb

// DeleteMatching and CountMatching walk the topic trie for the topics a
// wildcard query matches and delete, or count, their entries. The trie's own
// lookup matches a wildcard against topics stored as wildcards, not against
// arbitrary descendants, so it cannot enumerate, for example, every
// "$sys.sub.<topic>" of a contract, whose full names are not known. A
// contract retirement needs exactly that.

import (
	"github.com/unit-io/unitdb/message"
)

// matchingTopics returns the stored topics whose parts start with the query's
// fixed parts: all their descendants when the query ends in the multi
// wildcard, or exactly them otherwise.
func (db *DB) matchingTopics(q *Query) []_Topic {
	parts := q.internal.parts
	prefix := len(parts)
	descendants := false
	if prefix > 0 && parts[prefix-1].Hash == message.Wildcard {
		prefix--
		descendants = true
	}
	db.internal.trie.RLock()
	defer db.internal.trie.RUnlock()
	var out []_Topic
	for _, node := range db.internal.trie.topicTrie.summary {
		// The node's parts, root first; the chain includes the contract
		// part AddContract prepends, which node.depth does not count.
		depth := partsDepth(node)
		if depth < prefix {
			continue
		}
		nodeParts := make([]_Part, depth)
		n := node
		for i := depth - 1; i >= 0; i-- {
			nodeParts[i] = n.part
			n = n.parent
		}
		matches := true
		for i := 0; i < prefix; i++ {
			if nodeParts[i].hash != parts[i].Hash {
				matches = false
				break
			}
		}
		if !matches || (!descendants && depth != len(parts)) {
			continue
		}
		out = append(out, node.topics...)
	}
	return out
}

// partsDepth counts a node's parts up to the root.
func partsDepth(n *_Node) int {
	d := 0
	for x := n; x.parent != nil; x = x.parent {
		d++
	}
	return d
}

// topicEntries reads the live entries of a topic the query matches, once
// each: an entry can be found in a window both in memory and on disk.
func (db *DB) topicEntries(q *Query, topic _Topic) ([]_Query, error) {
	limit := db.opts.queryOptions.maxQueryLimit
	wins := db.internal.timeWindow.lookup(db.fs, topic.hash, topic.offset, q.internal.cutoff, limit)
	out := make([]_Query, 0, len(wins))
	seen := make(map[uint64]bool, len(wins))
	for _, we := range wins {
		seq := we.seq()
		cand := _Query{topicHash: topic.hash, seq: seq}
		if _, _, _, ok, err := db.readValue(q, cand); err != nil {
			return nil, err
		} else if ok && !seen[seq] {
			seen[seq] = true
			out = append(out, cand)
		}
	}
	return out, nil
}

// CountMatching counts the live entries the query matches.
func (db *DB) CountMatching(q *Query) (int, error) {
	if err := db.ok(); err != nil {
		return 0, err
	}
	if len(q.Topic) == 0 {
		return 0, errTopicEmpty
	}
	q.internal.opts = &_QueryOptions{defaultQueryLimit: db.opts.queryOptions.defaultQueryLimit, maxQueryLimit: db.opts.queryOptions.maxQueryLimit}
	if err := q.parse(); err != nil {
		return 0, err
	}
	n := 0
	for _, topic := range db.matchingTopics(q) {
		entries, err := db.topicEntries(q, topic)
		if err != nil {
			return n, err
		}
		n += len(entries)
	}
	return n, nil
}

// DeleteMatching deletes every live entry the query matches, wildcards
// included, and returns how many it deleted. The matched set shrinks as
// entries are deleted, so the scan repeats until none are left.
func (db *DB) DeleteMatching(q *Query) (deleted int, err error) {
	if err := db.ok(); err != nil {
		return 0, err
	}
	switch {
	case len(q.Topic) == 0:
		return 0, errTopicEmpty
	case len(q.Topic) > maxTopicLength:
		return 0, errTopicTooLarge
	}
	q.internal.opts = &_QueryOptions{defaultQueryLimit: db.opts.queryOptions.defaultQueryLimit, maxQueryLimit: db.opts.queryOptions.maxQueryLimit}
	if err := q.parse(); err != nil {
		return 0, err
	}
	for {
		round := 0
		for _, topic := range db.matchingTopics(q) {
			entries, err := db.topicEntries(q, topic)
			if err != nil {
				return deleted, err
			}
			for _, e := range entries {
				if err := db.delete(e.topicHash, e.seq); err != nil {
					return deleted, err
				}
				round++
				deleted++
			}
		}
		if round == 0 {
			return deleted, nil
		}
	}
}
