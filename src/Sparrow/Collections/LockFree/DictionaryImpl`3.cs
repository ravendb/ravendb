// Copyright (c) Vladimir Sadov. All rights reserved.
//
// This file is distributed under the MIT License. See LICENSE.md for details.

//
// Core algorithms are based on NonBlockingHashMap, 
// written and released to the public domain by Dr.Cliff Click.
// A good overview is here https://www.youtube.com/watch?v=HJ-719EGIts
//

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Sparrow.Collections.LockFree
{
    internal abstract partial class DictionaryImpl<TKey, TKeyStore, TValue>
        : DictionaryImpl<TKey, TValue>
    {
        private readonly Entry[] _entries;
        internal DictionaryImpl<TKey, TKeyStore, TValue> _newTable;

        protected readonly LockFreeConcurrentDictionary<TKey, TValue> _topDict;
        protected readonly Counter32 allocatedSlotCount = new Counter32();
        private Counter32 _size;

        // Sometimes many threads race to create a new very large table.  Only 1
        // wins the race, but the losers all allocate a junk large table with
        // hefty allocation costs.  Attempt to control the overkill here by
        // throttling attempts to create a new table.  I cannot really block here
        // (lest I lose the non-blocking property) but late-arriving threads can
        // give the initial resizing thread a little time to allocate the initial
        // new table.
        //
        // count of threads attempting an initial resize
        private int _resizers;

        // The next part of the table to copy.  It monotonically transits from zero
        // to table.length.  Visitors to the table can claim 'work chunks' by
        // CAS'ing this field up, then copying the indicated indices from the old
        // table to the new table.  Workers are not required to finish any chunk;
        // the counter simply wraps and work is copied duplicately until somebody
        // somewhere completes the count.
        private int _claimedChunk = 0;

        // Work-done reporting.  Used to efficiently signal when we can move to
        // the new table.  From 0 to length of old table refers to copying from the old
        // table to the new.
        private int _copyDone = 0;

        public struct Entry
        {
            internal int hash;
            internal TKeyStore key;
            internal object value;
        }

        private const int MIN_SIZE = 8;

        // targeted time span between resizes.
        // if resizing more often than this, try expanding.
        const uint RESIZE_MILLIS_TARGET = (uint)1000;

        // create an empty dictionary
        protected abstract DictionaryImpl<TKey, TKeyStore, TValue> CreateNew(int capacity);

        // convert key from its storage form (noop or unboxing) used in Key enumarators
        protected abstract TKey keyFromEntry(TKeyStore entryKey);

        // compares key with another in its storage form
        protected abstract bool keyEqual(TKey key, TKeyStore entryKey);

        // claiming (by writing atomically to the entryKey location) 
        // or getting existing slot suitable for storing a given key.
        protected abstract bool TryClaimSlotForPut(ref TKeyStore entryKey, TKey key);

        // claiming (by writing atomically to the entryKey location) 
        // or getting existing slot suitable for storing a given key in its store form (could be boxed).
        protected abstract bool TryClaimSlotForCopy(ref TKeyStore entryKey, TKeyStore key);

        internal DictionaryImpl(int capacity, LockFreeConcurrentDictionary<TKey, TValue> topDict)
        {
            capacity = Math.Max(capacity, MIN_SIZE);

            capacity = AlignToPowerOfTwo(capacity);
            this._entries = new Entry[capacity];
            this._size = new Counter32();
            this._topDict = topDict;
        }

        protected DictionaryImpl(int capacity, DictionaryImpl<TKey, TKeyStore, TValue> other)
        {
            capacity = AlignToPowerOfTwo(capacity);
            this._entries = new Entry[capacity];
            this._size = other._size;
            this._topDict = other._topDict;
            this._keyComparer = other._keyComparer;
        }


        private static uint CurrentTickMillis()
        {
            return (uint)Environment.TickCount;
        }

        private static int AlignToPowerOfTwo(int size)
        {
            Debug.Assert(size > 0);

            size--;
            size |= size >> 1;
            size |= size >> 2;
            size |= size >> 4;
            size |= size >> 8;
            size |= size >> 16;
            return size + 1;
        }

        protected virtual int hash(TKey key)
        {
            if (key == null)
            {
                throw new ArgumentNullException("key");
            }

            int h = _keyComparer.GetHashCode(key);

            // ensure that hash never matches 0, TOMBPRIMEHASH or ZEROHASH
            return h | REGULAR_HASH_BITS;
        }

        internal sealed override int Count
        {
            get
            {
                return this.Size;
            }
        }

        internal sealed override void Clear()
        {
            var newTable = CreateNew(MIN_SIZE);
            newTable._size = new Counter32();
            _topDict._table = newTable;
        }

        /// <summary>
        /// returns null if value is not present in the table
        /// otherwise returns the actual value or NULLVALUE if null is the actual value 
        /// </summary>
        internal sealed override object TryGetValue(TKey key)
        {
            int fullHash = this.hash(key);
            var curTable = this;

            TRY_WITH_NEW_TABLE:

            var curEntries = curTable._entries;
            var lenMask = curEntries.Length - 1;
            int idx = ReduceHashToIndex(fullHash, lenMask);

            // Main spin/reprobe loop
            int reprobeCnt = 0;
            while (true)
            {
                ref var entry = ref curEntries[idx];

                // hash, key and value are all CAS-ed down and follow a specific sequence of states.
                // hence the order of these reads is irrelevant and they do not need to be volatile
                var entryHash = entry.hash;
                if (entryHash == 0)
                {
                    // the slot has not been claimed - a clear miss
                    break;
                }

                // is this our slot?
                if (fullHash == entryHash &&
                    curTable.keyEqual(key, entry.key))
                {
                    var entryValue = entry.value;
                    if (EntryValueNullOrDead(entryValue))
                    {
                        break;
                    }

                    if (!(entryValue is Prime))
                    {
                        return entryValue;
                    }

                    // found a prime, that means copying has started 
                    // and all new values go to the new table
                    // help with copying and retry in the new table
                    curTable = curTable.CopySlotAndGetNewTable(idx, shouldHelp: true);

                    // return this.TryGet(newTable, entryKey, hash, out value); 
                    goto TRY_WITH_NEW_TABLE;
                }

                // get and put must have the same key lookup logic.
                // But only 'put' needs to force a table-resize for a too-long key-reprobe sequence
                // hitting reprobe limit or finding TOMBPRIMEHASH here means that the key is not in this table, 
                // but there could be more in the new table
                if (entryHash == TOMBPRIMEHASH | 
                    reprobeCnt >= ReprobeLimit(lenMask))
                {
                    var newTable = curTable._newTable;
                    if (newTable != null)
                    {
                        curTable.HelpCopy();
                        curTable = newTable;
                        goto TRY_WITH_NEW_TABLE;
                    }

                    // no new table, so this is a miss
                    break;
                }

                curTable.ReprobeResizeCheck(reprobeCnt, lenMask);

                // quadratic reprobe
                reprobeCnt++;
                idx = (idx + reprobeCnt) & lenMask;
            }

            return null;
        }

        // check once in a while if a table might benefit from resizing.
        // one reason for this is that crowdedness check uses estimated counts 
        // so we do not always catch this on key inserts.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ReprobeResizeCheck(int reprobeCnt, int lenMask)
        {
            // must be ^2 - 1
            const int reprobeCheckPeriod = 16 - 1;

            // every reprobeCheckPeriod reprobes, check if table is crowded
            // and initiale a resize
            if ((reprobeCnt & reprobeCheckPeriod) == reprobeCheckPeriod && 
                this.TableIsCrowded(lenMask))
            {
                this.Resize();
                this.HelpCopy();
            }
        }

        // 1) finds or creates a slot for the key
        // 2) sets the slot value to the putval if original value meets expVal condition
        // 3) returns true if the value was actually changed 
        // Note that pre-existence of the slot is irrelevant 
        // since slot without a value is as good as no slot at all
        internal sealed override bool PutIfMatch(TKey key, object newVal, ref object oldVal, ValueMatch match)
        {
            var curTable = this;
            int fullHash = curTable.hash(key);

            TRY_WITH_NEW_TABLE:

            Debug.Assert(newVal != null);
            Debug.Assert(!(newVal is Prime));

            var curEntries = curTable._entries;
            int lenMask = curEntries.Length - 1;
            int idx = ReduceHashToIndex(fullHash, lenMask);

            // Spin till we get a slot for the key or force a resizing.
            int reprobeCnt = 0;
            while (true)
            {
                // hash, key and value are all CAS-ed down and follow a specific sequence of states.
                // hence the order of their reads is irrelevant and they do not need to be volatile    
                var entryHash = curEntries[idx].hash;
                if (entryHash == 0)
                {
                    // Found an unassigned slot - which means this 
                    // key has never been in this table.
                    if (newVal == TOMBSTONE)
                    {
                        Debug.Assert(match == ValueMatch.NotNullOrDead || match == ValueMatch.OldValue);
                        oldVal = null;
                        goto FAILED;
                    }
                    else
                    {
                        // Slot is completely clean, claim the hash first
                        Debug.Assert(fullHash != 0);
                        entryHash = Interlocked.CompareExchange(ref curEntries[idx].hash, fullHash, 0);
                        if (entryHash == 0)
                        {
                            entryHash = fullHash;
                            if (entryHash == ZEROHASH)
                            {
                                // "added" entry for zero key
                                curTable.allocatedSlotCount.Increment();
                                break;
                            }
                        }
                    }
                }

                if (entryHash == fullHash)
                {
                    // hash is good, one way or another, 
                    // try claiming the slot for the key
                    if (curTable.TryClaimSlotForPut(ref curEntries[idx].key, key))
                    {
                        break;
                    }
                }

                // here we know that this slot does not map to our key
                // and must reprobe or resize
                // hitting reprobe limit or finding TOMBPRIMEHASH here means that the key is not in this table, 
                // but there could be more in the new table
                if (entryHash == TOMBPRIMEHASH |
                    reprobeCnt >= ReprobeLimit(lenMask))
                {
                    // start resize or get new table if resize is already in progress
                    var newTable1 = curTable.Resize();
                    // help along an existing copy
                    curTable.HelpCopy();
                    curTable = newTable1;
                    goto TRY_WITH_NEW_TABLE;
                }

                curTable.ReprobeResizeCheck(reprobeCnt, lenMask);

                // quadratic reprobing
                reprobeCnt++;
                idx = (idx + reprobeCnt) & lenMask;
            }

            // Found the proper Key slot, now update the Value.  
            // We never put a null, so Value slots monotonically move from null to
            // not-null (deleted Values use Tombstone).

            // volatile read to make sure we read the element before we read the _newTable
            // that would guarantee that as long as _newTable == null, entryValue cannot be a Prime.
            var entryValue = Volatile.Read(ref curEntries[idx].value);

            // See if we want to move to a new table (to avoid high average re-probe counts).  
            // We only check on the initial set of a Value from null to
            // not-null (i.e., once per key-insert).
            var newTable = curTable._newTable;

            // newTable == entryValue only when both are nulls
            if ((object)newTable == (object)entryValue &&
                curTable.TableIsCrowded(lenMask))
            {
                // Force the new table copy to start
                newTable = curTable.Resize();
                Debug.Assert(curTable._newTable != null && newTable == curTable._newTable);
            }

            // See if we are moving to a new table.
            // If so, copy our slot and retry in the new table.
            if (newTable != null)
            {
                var newTable1 = curTable.CopySlotAndGetNewTable(idx, shouldHelp: true);
                Debug.Assert(newTable == newTable1);
                curTable = newTable;
                goto TRY_WITH_NEW_TABLE;
            }

            // We are finally prepared to update the existing table
            while (true)
            {
                Debug.Assert(!(entryValue is Prime));
                var entryValueNullOrDead = EntryValueNullOrDead(entryValue);

                switch (match)
                {
                    case ValueMatch.Any:
                        if (newVal == entryValue)
                        {
                            // Do not update!
                            goto FAILED;
                        }
                        break;

                    case ValueMatch.NullOrDead:
                        if (entryValueNullOrDead)
                        {
                            break;
                        }

                        oldVal = entryValue;
                        goto FAILED;

                    case ValueMatch.NotNullOrDead:
                        if (entryValueNullOrDead)
                        {
                            goto FAILED;
                        }
                        break;
                    case ValueMatch.OldValue:
                        Debug.Assert(oldVal != null);
                        if (!oldVal.Equals(entryValue))
                        {
                            oldVal = entryValue;
                            goto FAILED;
                        }
                        break;
                }

                // Actually change the Value 
                var prev = Interlocked.CompareExchange(ref curEntries[idx].value, newVal, entryValue);
                if (prev == entryValue)
                {
                    // CAS succeeded - we did the update!
                    // Adjust sizes
                    if (entryValueNullOrDead)
                    {
                        oldVal = null;
                        if (newVal != TOMBSTONE)
                        {
                            curTable._size.Increment();
                        }
                    }
                    else
                    {
                        oldVal = prev;
                        if (newVal == TOMBSTONE)
                        {
                            curTable._size.Decrement();
                        }
                    }

                    return true;
                }
                // Else CAS failed

                // If a Prime'd value got installed, we need to re-run the put on the new table.  
                if (prev is Prime)
                {
                    curTable = curTable.CopySlotAndGetNewTable(idx, shouldHelp: true);
                    goto TRY_WITH_NEW_TABLE;
                }

                // Otherwise we lost the CAS to another racing put.
                // Simply retry from the start.
                entryValue = prev;
            }

            FAILED:
            return false;
        }

        internal sealed override TValue GetOrAdd(TKey key, Func<TKey, TValue> valueFactory)
        {
            if (valueFactory == null)
            {
                throw new ArgumentNullException("valueFactory");
            }

            object newValObj = null;
            TValue result = default(TValue);

            var curTable = this;
            int fullHash = curTable.hash(key);

            TRY_WITH_NEW_TABLE:

            var curEntries = curTable._entries;
            int lenMask = curEntries.Length - 1;
            int idx = ReduceHashToIndex(fullHash, lenMask);

            // Spin till we get a slot for the key or force a resizing.
            int reprobeCnt = 0;
            while (true)
            {
                // hash, key and value are all CAS-ed down and follow a specific sequence of states.
                // hence the order of their reads is irrelevant and they do not need to be volatile    
                var entryHash = curEntries[idx].hash;
                if (entryHash == 0)
                {
                    // Found an unassigned slot - which means this 
                    // key has never been in this table.
                    // Slot is completely clean, claim the hash first
                    Debug.Assert(fullHash != 0);
                    entryHash = Interlocked.CompareExchange(ref curEntries[idx].hash, fullHash, 0);
                    if (entryHash == 0)
                    {
                        entryHash = fullHash;
                        if (entryHash == ZEROHASH)
                        {
                            // "added" entry for zero key
                            curTable.allocatedSlotCount.Increment();
                            break;
                        }
                    }
                }

                if (entryHash == fullHash)
                {
                    // hash is good, one way or another, 
                    // try claiming the slot for the key
                    if (curTable.TryClaimSlotForPut(ref curEntries[idx].key, key))
                    {
                        break;
                    }
                }

                // here we know that this slot does not map to our key
                // and must reprobe or resize
                // hitting reprobe limit or finding TOMBPRIMEHASH here means that the key is not in this table, 
                // but there could be more in the new table
                if (entryHash == TOMBPRIMEHASH |
                    reprobeCnt >= ReprobeLimit(lenMask))
                {
                    // start resize or get new table if resize is already in progress
                    var newTable1 = curTable.Resize();
                    // help along an existing copy
                    curTable.HelpCopy();
                    curTable = newTable1;
                    goto TRY_WITH_NEW_TABLE;
                }

                curTable.ReprobeResizeCheck(reprobeCnt, lenMask);

                // quadratic reprobing
                reprobeCnt++;
                idx = (idx + reprobeCnt) & lenMask;
            }

            // Found the proper Key slot, now update the Value.  
            // We never put a null, so Value slots monotonically move from null to
            // not-null (deleted Values use Tombstone).

            // volatile read to make sure we read the element before we read the _newTable
            // that would guarantee that as long as _newTable == null, entryValue cannot be a Prime.
            var entryValue = Volatile.Read(ref curEntries[idx].value);

            // See if we want to move to a new table (to avoid high average re-probe counts).  
            // We only check on the initial set of a Value from null to
            // not-null (i.e., once per key-insert).
            var newTable = curTable._newTable;

            // newTable == entryValue only when both are nulls
            if ((object)newTable == (object)entryValue &&
                curTable.TableIsCrowded(lenMask))
            {
                // Force the new table copy to start
                newTable = curTable.Resize();
                Debug.Assert(curTable._newTable != null && curTable._newTable == newTable);
            }

            // See if we are moving to a new table.
            // If so, copy our slot and retry in the new table.
            if (newTable != null)
            {
                var newTable1 = curTable.CopySlotAndGetNewTable(idx, shouldHelp: true);
                Debug.Assert(newTable == newTable1);
                curTable = newTable;
                goto TRY_WITH_NEW_TABLE;
            }

            if (!EntryValueNullOrDead(entryValue))
            {
                goto GOT_PREV_VALUE;
            }

            // prev value is not null, dead or prime.
            // let's try install new value
            newValObj = newValObj ?? ToObjectValue(result = valueFactory(key));
            while (true)
            {
                Debug.Assert(!(entryValue is Prime));

                // Actually change the Value 
                var prev = Interlocked.CompareExchange(ref curEntries[idx].value, newValObj, entryValue);
                if (prev == entryValue)
                {
                    // CAS succeeded - we did the update!
                    // Adjust sizes
                    curTable._size.Increment();
                    goto DONE;
                }
                // Else CAS failed

                // If a Prime'd value got installed, we need to re-run the put on the new table.
                if (prev is Prime)
                {
                    curTable = curTable.CopySlotAndGetNewTable(idx, shouldHelp: true);
                    goto TRY_WITH_NEW_TABLE;
                }

                // Otherwise we lost the CAS to another racing put.
                entryValue = prev;
                if (!EntryValueNullOrDead(entryValue))
                {
                    goto GOT_PREV_VALUE;
                }
            }

            GOT_PREV_VALUE:

            // PERF: this would be nice to have as a helper, 
            // but it does not get inlined

            // regular value type
            if (default(TValue) != null)
            {
                result = (TValue)entryValue;
            }
            else
            {
                // null
                if (entryValue == NULLVALUE)
                {
                    result = default(TValue);
                }
                else
                {
                    // not null, dispatch ref types and nullables
                    result = _topDict.objToValue(entryValue);
                }
            }

            DONE:
            return result;
        }

        private bool PutSlotCopy(TKeyStore key, object value, int fullHash)
        {
            Debug.Assert(key != null);
            Debug.Assert(value != TOMBSTONE);
            Debug.Assert(value != null);
            Debug.Assert(!(value is Prime));

            var curTable = this;

            TRY_WITH_NEW_TABLE:

            var curEntries = curTable._entries;
            int lenMask = curEntries.Length - 1;
            int idx = ReduceHashToIndex(fullHash, lenMask);

            // Spin till we get a slot for the key or force a resizing.
            int reprobeCnt = 0;
            while (true)
            {
                var entryHash = curEntries[idx].hash;
                if (entryHash == 0)
                {
                    // Slot is completely clean, claim the hash
                    Debug.Assert(fullHash != 0);
                    entryHash = Interlocked.CompareExchange(ref curEntries[idx].hash, fullHash, 0);
                    if (entryHash == 0)
                    {
                        entryHash = fullHash;
                        if (entryHash == ZEROHASH)
                        {
                            // "added" entry for zero key
                            curTable.allocatedSlotCount.Increment();
                            break;
                        }
                    }
                }

                if (entryHash == fullHash)
                {
                    // hash is good, one way or another, claim the key
                    if (curTable.TryClaimSlotForCopy(ref curEntries[idx].key, key))
                    {
                        break;
                    }
                }

                // this slot contains a different key

                // here we know that this slot does not map to our key
                // and must reprobe or resize
                // hitting reprobe limit or finding TOMBPRIMEHASH here means that 
                // we will not find an appropriate slot in this table
                // but there could be more in the new one
                if (entryHash == TOMBPRIMEHASH |
                    reprobeCnt >= ReprobeLimit(lenMask))
                {
                    var resized = curTable.Resize();
                    curTable = resized;
                    goto TRY_WITH_NEW_TABLE;
                }

                // quadratic reprobing
                reprobeCnt++;
                idx = (idx + reprobeCnt) & lenMask; // Reprobe!    

            } // End of spinning till we get a Key slot

            // Found the proper Key slot, now update the Value. 
            var entryValue = curEntries[idx].value;

            // See if we want to move to a new table (to avoid high average re-probe counts).  
            // We only check on the initial set of a Value from null to
            // not-null (i.e., once per key-insert).
            var newTable = curTable._newTable;

            // newTable == entryValue only when both are nulls
            if ((object)newTable == (object)entryValue &&
                curTable.TableIsCrowded(lenMask))
            {
                // Force the new table copy to start
                newTable = curTable.Resize();
                Debug.Assert(curTable._newTable != null && curTable._newTable == newTable);
            }

            // See if we are moving to a new table.
            // If so, copy our slot and retry in the new table.
            if (newTable != null)
            {
                var newTable1 = curTable.CopySlotAndGetNewTable(idx, shouldHelp: false);
                Debug.Assert(newTable == newTable1);
                curTable = newTable;
                goto TRY_WITH_NEW_TABLE;
            }

            // We are finally prepared to update the existing table
            // if entry value is null and our CAS succeeds - we did the update!
            // otherwise someone else copied the value
            // table-copy does not (effectively) increase the number of live k/v pairs
            // so no need to update size
            return curEntries[idx].value == null &&
                   Interlocked.CompareExchange(ref curEntries[idx].value, value, null) == null;
        }

        ///////////////////////////////////////////////////////////
        // Resize support
        ///////////////////////////////////////////////////////////

        internal int Size
        {
            get
            {
                // counter does not lose counts, but reports of increments/decrements can be delayed
                // it might be confusing if we ever report negative size.
                var size = _size.Value;
                var negMask = ~(size >> 31);
                return size & negMask;
            }
        }

        internal int EstimatedSlotsUsed
        {
            get
            {
                return (int)allocatedSlotCount.EstimatedValue;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TableIsCrowded(int len)
        {
            // 80% utilization, switch to a bigger table
            return EstimatedSlotsUsed > (len >> 2) * 3;
        }


        // Help along an existing resize operation.  This is just a fast cut-out
        // wrapper, to encourage inlining for the fast no-copy-in-progress case.
        private void HelpCopy()
        {
            if (this._newTable != null)
            {
                this.HelpCopyImpl(copy_all: false);
            }
        }

        // Help along an existing resize operation.
        internal void HelpCopyImpl(bool copy_all)
        {
            var newTable = this._newTable;
            var oldEntries = this._entries;
            int toCopy = oldEntries.Length;

#if DEBUG
            const int CHUNK_SIZE = 16;
#else
            const int CHUNK_SIZE = 1024;
#endif
            int MIN_COPY_WORK = Math.Min(toCopy, CHUNK_SIZE); // Limit per-thread work

            bool panic = false;
            int claimedChunk = -1;

            while (this._copyDone < toCopy)
            {
                // Still needing to copy?
                // Carve out a chunk of work.
                if (!panic)
                {
                    claimedChunk = this._claimedChunk;

                    for (;;)
                    {
                        // panic check
                        // We "panic" if we have tried TWICE to copy every slot - and it still
                        // has not happened.  i.e., twice some thread somewhere claimed they
                        // would copy 'slot X' (by bumping _copyIdx) but they never claimed to
                        // have finished (by bumping _copyDone).  Our choices become limited:
                        // we can wait for the work-claimers to finish (and become a blocking
                        // algorithm) or do the copy work ourselves.  Tiny tables with huge
                        // thread counts trying to copy the table often 'panic'. 
                        if (claimedChunk > (toCopy / (CHUNK_SIZE / 2)))
                        {
                            panic = true;
                            //System.Console.WriteLine("panic");
                            break;
                        }

                        var alreadyClaimed = Interlocked.CompareExchange(ref this._claimedChunk, claimedChunk + 1, claimedChunk);
                        if (alreadyClaimed == claimedChunk)
                        {
                            break;
                        }

                        claimedChunk = alreadyClaimed;
                    }
                }
                else
                {
                    // we went through the whole table in panic mode
                    // there cannot be possibly anything left to copy.
                    if (claimedChunk > ((toCopy / (CHUNK_SIZE / 2)) + toCopy / CHUNK_SIZE))
                    {
                        _copyDone = toCopy;
                        PromoteNewTable();
                        return;
                    }

                    claimedChunk++;
                }

                // We now know what to copy.  Try to copy.
                int workdone = 0;
                int copyStart = claimedChunk * CHUNK_SIZE;
                for (int i = 0; i < MIN_COPY_WORK; i++)
                {
                    if (this._copyDone >= toCopy)
                    {
                        PromoteNewTable();
                        return;
                    }

                    if (CopySlot(ref oldEntries[(copyStart + i) & (toCopy - 1)], newTable))
                    {
                        workdone++;
                    }
                }

                if (workdone > 0)
                {
                    // See if we can promote
                    var copyDone = Interlocked.Add(ref this._copyDone, workdone);

                    // Check for copy being ALL done, and promote.  
                    if (copyDone >= toCopy)
                    {
                        PromoteNewTable();
                    }
                }

                if (!(copy_all | panic))
                {
                    return;
                }
            }

            // Extra promotion check, in case another thread finished all copying
            // then got stalled before promoting.
            PromoteNewTable();
        }

        private void PromoteNewTable()
        {
            // Looking at the top-level table?
            // Note that we might have
            // nested in-progress copies and manage to finish a nested copy before
            // finishing the top-level copy.  We only promote top-level copies.
            if (_topDict._table == this)
            {
                // Attempt to promote
                if (Interlocked.CompareExchange(ref _topDict._table, this._newTable, this) == this)
                {
                    // System.Console.WriteLine("size: " + _newTable.Length);
                    _topDict._lastResizeTickMillis = CurrentTickMillis();
                }
            }
        }

        // Copy slot 'idx' from the old table to the new table.  If this thread
        // confirmed the copy, update the counters and check for promotion.
        //
        // Returns the result of reading the new table, mostly as a
        // convenience to callers.  We come here with 1-shot copy requests
        // typically because the caller has found a Prime, and has not yet read
        // the new table - which must have changed from null-to-not-null
        // before any Prime appears.  So the caller needs to read the new table
        // field to retry his operation in the new table, but probably has not
        // read it yet.
        internal DictionaryImpl<TKey, TKeyStore, TValue> CopySlotAndGetNewTable(int idx, bool shouldHelp)
        {
            var newTable = this._newTable;

            // We're only here because the caller saw a Prime, which implies a
            // table-copy is in progress.
            Debug.Assert(newTable != null);

            if (CopySlot(ref this._entries[idx], newTable))
            {
                // Record the slot copied
                var copyDone = Interlocked.Increment(ref this._copyDone);

                // Check for copy being ALL done, and promote.  
                if (copyDone >= this._entries.Length)
                {
                    PromoteNewTable();
                }
            }

            // Generically help along any copy (except if called recursively from a helper)
            if (shouldHelp)
            {
                this.HelpCopy();
            }

            return newTable;
        }

        // Copy one K/V pair from old table to new table. 
        // Returns true if we actually did the copy.
        // Regardless, once this returns, the copy is available in the new table and 
        // slot in the old table is no longer usable.
        private static bool CopySlot(ref Entry oldEntry, DictionaryImpl<TKey, TKeyStore, TValue> newTable)
        {
            Debug.Assert(newTable != null);

            // Blindly set the hash from 0 to TOMBPRIMEHASH, to eagerly stop
            // fresh put's from claiming new slots in the old table when the old
            // table is mid-resize.
            var hash = oldEntry.hash;
            if (hash == 0)
            {
                hash = Interlocked.CompareExchange(ref oldEntry.hash, TOMBPRIMEHASH, 0);
                if (hash == 0)
                {
                    // slot was not claimed, copy is done here
                    return true;
                }
            }

            if (hash == TOMBPRIMEHASH)
            {
                // slot was trivially copied, but not by us
                return false;
            }

            // Prevent new values from appearing in the old table.
            // Box what we see in the old table, to prevent further updates.
            // NOTE: Read of the value below must happen before reading of the key, 
            // however this read does not need to be volatile since we will have 
            // some fences in between reads.
            object oldval = oldEntry.value;

            // already boxed?
            Prime box = oldval as Prime;
            if (box != null)
            {
                // volatile read here since we need to make sure 
                // that the key read below happens after we have read oldval above
                Volatile.Read(ref box.originalValue);
            }
            else
            {
                do
                {
                    box = EntryValueNullOrDead(oldval) ?
                        TOMBPRIME :
                        new Prime(oldval);

                    // CAS down a box'd version of oldval
                    // also works as a complete fence between reading the value and the key
                    object prev = Interlocked.CompareExchange(ref oldEntry.value, box, oldval);

                    if (prev == oldval)
                    {
                        // If we made the Value slot hold a TOMBPRIME, then we both
                        // prevented further updates here but also the (absent)
                        // oldval is vacuously available in the new table.  We
                        // return with true here: any thread looking for a value for
                        // this key can correctly go straight to the new table and
                        // skip looking in the old table.
                        if (box == TOMBPRIME)
                        {
                            return true;
                        }

                        // Break loop; oldval is now boxed by us
                        // it still needs to be copied into the new table.
                        break;
                    }

                    oldval = prev;
                    box = oldval as Prime;
                }
                while (box == null);
            }

            if (box == TOMBPRIME)
            {
                // Copy already complete here, but not by us.
                return false;
            }

            // Copy the value into the new table, but only if we overwrite a null.
            // If another value is already in the new table, then somebody else
            // wrote something there and that write is happens-after any value that
            // appears in the old table.  If putIfMatch does not find a null in the
            // new table - somebody else should have recorded the null-not_null
            // transition in this copy.
            object originalValue = box.originalValue;
            Debug.Assert(originalValue != TOMBSTONE);

            // since we have a real value, there must be a nontrivial key in the table
            // regular read is ok because value is always CASed down after the key
            // and we ensured that we read the key after the value with fences above
            var key = oldEntry.key;
            bool copiedIntoNew = newTable.PutSlotCopy(key, originalValue, hash);

            // Finally, now that any old value is exposed in the new table, we can
            // forever hide the old-table value by gently inserting TOMBPRIME value.  
            // This will stop other threads from uselessly attempting to copy this slot
            // (i.e., it's a speed optimization not a correctness issue).
            // Check if we are not too late though, to not pay for MESI RFO and 
            // GC fence needlessly.
            if (oldEntry.value != TOMBPRIME)
            {
                oldEntry.value = TOMBPRIME;
            }

            // if we failed to copy, it means something has already appeared in
            // the new table and old value should have been copied before that (not by us).
            return copiedIntoNew;
        }

        // kick off resizing, if not started already, and return the new table.
        private DictionaryImpl<TKey, TKeyStore, TValue> Resize()
        {
            // Check for resize already in progress, probably triggered by another thread
            // reads of this._newTable in Resize are not volatile
            // we are just opportunistically checking if a new table has arrived.
            return this._newTable ?? ResizeImpl();
        }

        // Resizing after too many probes.  "How Big???" heuristics are here.
        // Callers will (not this routine) help any in-progress copy.
        // Since this routine has a fast cutout for copy-already-started, callers
        // MUST 'help_copy' lest we have a path which forever runs through
        // 'resize' only to discover a copy-in-progress which never progresses.
        private DictionaryImpl<TKey, TKeyStore, TValue> ResizeImpl()
        {
            // No copy in-progress, so start one.  
            //First up: compute new table size.
            int oldlen = this._entries.Length;

            const int MAX_SIZE = 1 << 30;
            const int MAX_CHURN_SIZE = 1 << 15;

            // First size estimate is roughly inverse of ProbeLimit
            int sz = Size + (MIN_SIZE >> REPROBE_LIMIT_SHIFT);
            int newsz = sz < (MAX_SIZE >> REPROBE_LIMIT_SHIFT) ?
                                            sz << REPROBE_LIMIT_SHIFT :
                                            sz;

            // if new table would shrink or hold steady, 
            // we must be resizing because of churn.
            // target churn based resize rate to be about 1 per RESIZE_TICKS_TARGET
            if (newsz <= oldlen)
            {
                var resizeSpan = CurrentTickMillis() - _topDict._lastResizeTickMillis;

                // note that CurrentTicks() will wrap around every 50 days.
                // For our purposes that is tolerable since it just 
                // adds a possibility that in some rare cases a churning resize will not be 
                // considered a churning one.
                if (resizeSpan < RESIZE_MILLIS_TARGET)
                {
                    // last resize too recent, expand
                    newsz = oldlen < MAX_CHURN_SIZE ? oldlen << 1 : oldlen;
                }
                else
                {
                    // do not allow shrink too fast
                    newsz = Math.Max(newsz, (int)((long)oldlen * RESIZE_MILLIS_TARGET / resizeSpan));
                }
            }

            // Align up to a power of 2
            newsz = AlignToPowerOfTwo(newsz);

            // Size calculation: 2 words (K+V) per table entry, plus a handful.  We
            // guess at 32-bit pointers; 64-bit pointers screws up the size calc by
            // 2x but does not screw up the heuristic very much.
            //
            // TODO: VS some tuning may be needed
            int kBs4 = (((newsz << 1) + 4) << 3/*word to bytes*/) >> 12/*kBs4*/;

            var newTable = this._newTable;

            // Now, if allocation is big enough,
            // limit the number of threads actually allocating memory to a
            // handful - lest we have 750 threads all trying to allocate a giant
            // resized array.
            // conveniently, Increment is also a full fence
            if (kBs4 > 0 && Interlocked.Increment(ref _resizers) >= 2)
            {
                // Already 2 guys trying; wait and see
                // See if resize is already in progress
                if (newTable != null)
                {
                    return newTable;         // Use the new table already
                }

                SpinWait.SpinUntil(() => this._newTable != null, 8 * kBs4);
                newTable = this._newTable;
            }

            // Last check, since the 'new' below is expensive and there is a chance
            // that another thread slipped in a new table while we ran the heuristic.
            newTable = this._newTable;
            // See if resize is already in progress
            if (newTable != null)
            {
                return newTable;          // Use the new table already
            }

            newTable = this.CreateNew(newsz);

            // The new table must be CAS'd in to ensure only 1 winner
            var prev = this._newTable ??
                        Interlocked.CompareExchange(ref this._newTable, newTable, null);

            if (prev != null)
            {
                return prev;
            }
            else
            {
                //Console.WriteLine("resized: " + newsz);
                return newTable;
            }
        }
    }
}
