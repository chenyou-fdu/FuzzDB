package org.chenyou.fuzzdb.db;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.chenyou.fuzzdb.util.Random;
import java.util.Iterator;
/**
 * Created by ChenYou on 2018/1/1.
 * 1. SkipList simple tutorial:
 *   level: 2 head -> 1 -> 4 -----------> null
 *   level: 1 head -> 1 -> 2 -> 3 -> 4 -> null
 *   search key '3' will start from level 2 (node 'head'), until both '1' and '4' are found,
 *     turn to level 1 (node '2'), and found target key '3'
 *   notice that there will be NO SUCH CASE as presented below:
 *    level: 2 head -----------> 3 -> null
 *    level: 1 head -> 1 -> 2 -> 3 -> null
 */

public class SkipList<Key> implements Iterable<Key> {
    private Comparator<Key> cmp;
    private Node<Key> head;
    private AtomicReference<Integer> maxHeight;
    private Random rnd;
    private final Integer kMaxHeight = 12;

    public SkipList(Comparator<Key> cmp) {
        this.cmp = cmp;
        this.maxHeight = new AtomicReference<>(1);
        this.rnd = new Random(0xdeadbeef);
        // any key is ok, so use null here?
        this.head = newNode(null, kMaxHeight);
        for(Integer i = 0; i < kMaxHeight; i++) {
            this.head.setNext(i, null);
        }
    }

    // insert key into the list.
    // REQUIRES: nothing that compares equal to key is currently in the list.
    public void insert(final Key key) throws UnsupportedOperationException {
        List<Node<Key>> prev = new ArrayList<>(kMaxHeight);
        for(int i = 0; i < kMaxHeight; i++) {
            prev.add(null);
        }
        Node<Key> x = findGreaterOrEqual(key, prev);

        // skiplist does not allow duplicate insertion
        if(x != null && equal(x.key, key))
            throw new UnsupportedOperationException("duplicate insertion is not allowed");

        Integer height = randomHeight();
        if (height > getMaxHeight()) {
            // levels that higher than all current level
            // set their prev nodes to head
            for(Integer i = getMaxHeight(); i < height; i++){
                prev.set(i, this.head);
            }
            // why is ok not to lock this part?
            // need ref to comments in leveldb again...
            this.maxHeight.set(height);
        }
        x = newNode(key, height);
        for(Integer i = 0; i < height; i++) {
            x.setNext(i, prev.get(i).next(i));
            prev.get(i).setNext(i, x);
        }
    }
    public Boolean contains(final Key key) {
        Node<Key> x = findGreaterOrEqual(key, null);
        return (x != null && equal(key, x.key));
    }

    @Override
    public SkipListIterator<Key> iterator() {
        return new SkipListIterator<>(this);
    }

    // implements refer to JDK LinkedList
    public static final class SkipListIterator<Key> implements Iterator<Key> {
        private Node<Key> node;
        private SkipList<Key> skipList;

        private SkipListIterator(final SkipList<Key> skipList) {
            this.skipList = skipList;
            this.node = null;
        }

        @Override
        public Key next() {
            if(!valid()) throw new NullPointerException();
            this.node = this.node.next(0);
            if(!valid()) throw new NullPointerException();
            return this.node.key;
        }

        @Override
        public boolean hasNext() {
            if(!valid()) throw new NullPointerException();
            return this.node.next(0) != null;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        public Boolean valid() {
            return this.node != null;
        }

        public void iterNext() {
            if(!valid()) throw new NullPointerException();
            this.node = this.node.next(0);
        }

        public Key key() {
            if(!valid()) throw new NullPointerException();
            return this.node.key;
        }

        public void prev() {
            if(!valid()) throw new NullPointerException();
            this.node = this.skipList.findLessThan(this.node.key);
            if(this.node == this.skipList.head) {
                this.node = null;
            }
        }

        public void seek(final Key target) {
            this.node = this.skipList.findGreaterOrEqual(target, null);
        }

        public void seekToFirst() {
            this.node = this.skipList.head.next(0);
        }

        public void seekToLast() {
            this.node = this.skipList.findLast();
            if(this.node == this.skipList.head) {
                this.node = null;
            }
        }



    }

    private Integer getMaxHeight() {
        return this.maxHeight.get();
    }

    private Node<Key> newNode(final Key key, Integer height) {
        return new Node<>(key, height);
    }

    private Integer randomHeight() {
        final Integer kBranching = 4;
        Integer height = 1;
        // increase height iff random value divides four
        // so higher level in skiplist will has less likely to occur
        // height has to be [1, kMaxHeight]
        while(height <= kMaxHeight && ((rnd.next() % kBranching) == 0)) {
            height++;
        }
        return height;
    }

    private Boolean equal(final Key a, final Key b) {
        return this.cmp.compare(a, b) == 0;
    }

    // return true if key is greater than the data stored in "n"
    private Boolean keyIsAfterNode(final Key key, Node<Key> n) {
        return ((n != null) && (this.cmp.compare(n.key, key) < 0));
    }

    // return the earliest node that comes at or after key.
    // return null if there is no such node.
    // If prev is not empty, fills prev[level] with previous
    // node at "level" for every level in [0, maxHeight-1].
    private Node<Key> findGreaterOrEqual(final Key key, List<Node<Key>> prev) {
        Node<Key> x = this.head;
        // search from the highest level
        Integer level = getMaxHeight() - 1;
        while(true) {
            Node<Key> next = x.next(level);
            if(keyIsAfterNode(key, next)) {
                // keep searching in this level
                //   iff key in next node is less than target key
                x = next;
            } else {
                if(prev != null && !prev.isEmpty()) prev.set(level, x);
                if(level == 0) return next;
                // search in the next lower level
                // next round of search starts at same node, but different level
                else level--;
            }
        }
    }

    // return the latest node with a key < key.
    // return head if there is no such node.
    private Node<Key> findLessThan(final Key key) {
        Node<Key> x = this.head;
        // search from the highest level
        Integer level = getMaxHeight() - 1;
        while(true) {
            Node<Key> next = x.next(level);
            // search all nodes in this level
            if(next == null || this.cmp.compare(next.key, key) >= 0) {
                if(level == 0) return x;
                else level--;
            // keep search less node in this level
            } else {
                x = next;
            }
        }
    }

    // return the last node in the list.
    // return head if list is empty.
    private Node<Key> findLast() {
        Node<Key> x = this.head;
        // search from the highest level
        Integer level = getMaxHeight() - 1;
        while(true) {
            Node<Key> next = x.next(level);
            if(next == null) {
                if(level == 0) return x;
                else level--;
            } else {
                x = next;
            }
        }
    }

    private static final class Node<Key> {
        private Node(final Key k, Integer height) {
            this.key = k;
            // size for next list is 1 (lowest level link) plus level height (height - 1)
            this.next = new ArrayList<>(height);
            for(Integer i = 0; i < height; i++)
                this.next.add(new AtomicReference<>());
        }
        private Node<Key> next(Integer n) {
            return next.get(n).get();
        }
        private void setNext(Integer n, Node<Key> x) {
            this.next.get(n).set(x);
        }
        private Key key;
        private List<AtomicReference<Node<Key>>> next;
    }
}
