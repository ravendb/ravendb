using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Sparrow;
using Sparrow.Server.Utils;
using Voron.Impl.Paging;
using Voron.Util;

namespace Voron.Impl.Scratch
{
    /// <summary>
    /// Maps a data file page number to its current location in the scratch buffers - the page translation
    /// table that sits on every page access of a write transaction and, through snapshots, of every read
    /// transaction.
    ///
    /// The goals here, in order: capturing the committed state must be O(1), steady-state operation must 
    /// not allocate, and the table must stay invisible to the GC no matter how many millions of pages sit
    /// in scratch. For that reason, the table is built from arrays of blittable structs and primitive 
    /// types, with no references.
    ///
    /// The structure is an open-addressing hash table (linear probing, Fibonacci hashing) split into
    /// parallel arrays(SoA), with the version chains stored inline:
    /// - _keys holds the page number per slot (NoKey marks an empty slot - page 0 is a valid key). Probing
    ///   touches only this array, 8 bytes per step .
    /// - _heads holds the index of the page's newest version in the entries array, NoEntry for none.
    /// - _entries holds the version chain nodes, blittable and linked by index, newest first.
    /// - _refs holds the only object references a version needs (the scratch file and the pager state
    ///   mapping it), deduplicated to one entry per scratch file growth rather than one per page.
    /// 
    /// The four arrays form a generation: append-only between rebuilds, replaced together by Rebuild.
    /// An index is therefore never repurposed while its generation is published, which is what makes
    /// index-following safe with no interlocking. The arrays are sized to be born on the LOH - gen2 from
    /// birth and never copied by a collection - and retired generations recycle through a pool
    /// (ScratchTableBuffers) once no snapshot can be holding them, so rebuilds allocate nothing at
    /// steady state.
    ///
    /// Concurrency: a single writer (the write transaction, serialized by the environment's write lock)
    /// and wait-free readers. A snapshot is O(1) - the current generation plus this session's publish
    /// sequence as the visibility bound. 
    /// 
    /// Visibility runs on the table's own sequence rather than the transaction id: the sequence advances 
    /// on every write session, so a book-keeping commit that never touches the journal (the flush-state 
    /// update) still publishes its removals atomically with the rest of the environment state record. 
    /// 
    /// A reader never observes a version above its bound - versions are
    /// stamped with their session's sequence, and the publish order (key, then head, then chain links,
    /// all volatile) makes a half-inserted version indistinguishable from an absent one. 
    /// 
    /// Versions are unlinked only when no active or future snapshot can observe them: the prune floor is
    /// the minimum over the active transactions' bounds, with the last published sequence as a sentinel
    /// covering readers that are registering concurrently. Unlinking alone is not enough to recycle,
    /// though: a reader whose bound is below an entry's sequence walks *through* that entry - it loads the
    /// link to it, then its sequence, then its next link - and there is a window where the reader stands on
    /// an entry the pruner has just unlinked. Rewriting the entry there would send the reader into another
    /// page's chain. So an unlinked entry is never freed directly: it is parked untouched, tagged with the
    /// unlinking session's sequence, and moves to the free list only once the prune floor passes that
    /// sequence - the same rule retired generations follow. This covers the session's own entries too:
    /// they hang off published chain heads, and a reader skipping them by sequence transits them the
    /// same way.
    ///
    /// On the hot paths this buys: O(1) commit capture, rollback proportional to what the transaction
    /// actually touched, flushing proportional to the delta, and reads that cost one probe run over a
    /// cache-dense key array plus a chain walk that pruning keeps short.
    /// </summary>
    public sealed class ScratchPagesTable
    {
        internal const int NoEntry = -1;

        internal const long NoKey = -1;

        internal const long FreeSeq = long.MinValue;

        private const int LohThresholdBytes = 85_000;

        private static int EnsureFitsInLargeObjectHeap<T>() where T : unmanaged =>
            Math.Max(1024, (int)BitOperations.RoundUpToPowerOf2((uint)(LohThresholdBytes / Unsafe.SizeOf<T>()) + 1));

        private static readonly int MinSlots = EnsureFitsInLargeObjectHeap<long>();

        private static readonly int MinEntries = EnsureFitsInLargeObjectHeap<ScratchEntry>();

        private const int InitialRefs = 16;

        private const int ChainDepthPruneThreshold = 8;

        private readonly ActiveTransactions _activeTransactions;

        private long[] _keys;
        private int[] _heads;
        private int _usedSlots;

        private ScratchEntry[] _entries = GC.AllocateUninitializedArray<ScratchEntry>(MinEntries);
        private int _usedEntries;

        private int _freeHead = NoEntry;
        private int _freeEntries;

        private ScratchRef[] _refs = new ScratchRef[InitialRefs];
        private int _usedRefs;

        private int _visibleCount;

        private long _seqCounter;
        private long _seq;
        private long _lastPublishedSeq;

        private readonly List<long> _activeSnapshots = new();
        private bool _activeSnapshotsFetched;

        private long _prunedUpToSeq;

        private readonly Queue<(long Seq, int Index)> _pendingFree = new();

        private readonly List<long> _undo = [];

        private ScratchTableBuffers _buffers = new();

        private struct ScratchTableBuffers
        {
            private const int MaxMissesBeforeDiscard = 8;

            private const int MaxEntriesOversizeFactor = 4;

            private readonly Queue<(long RetiredAtSeq, long[] Keys, int[] Heads, ScratchEntry[] Entries)> _retiredGenerations = new();
            private readonly Queue<(long[] Keys, int[] Heads, int Misses)> _slotsPool = new();
            private readonly Queue<(ScratchEntry[] Entries, int Misses)> _entriesPool = new();

            public ScratchTableBuffers()
            {
            }

            public static (long[] Keys, int[] Heads) AllocateSlots(int size)
            {
                var keys = GC.AllocateUninitializedArray<long>(size);
                Array.Fill(keys, NoKey);
                var heads = GC.AllocateUninitializedArray<int>(size);
                Array.Fill(heads, NoEntry);
                return (keys, heads);
            }

            public (long[] Keys, int[] Heads) RentSlots(int size)
            {
                for (var remaining = _slotsPool.Count; remaining > 0; remaining--)
                {
                    var (keys, heads, misses) = _slotsPool.Dequeue();
                    if (keys.Length == size)
                    {
                        Array.Fill(keys, NoKey);
                        Array.Fill(heads, NoEntry);
                        return (keys, heads);
                    }

                    // We gave it a few chance to find a fit, but afterward, we discard it to the GC
                    if (misses + 1 < MaxMissesBeforeDiscard)
                        _slotsPool.Enqueue((keys, heads, misses + 1));
                }

                return AllocateSlots(size);
            }

            public ScratchEntry[] RentEntries(int size)
            {
                for (var remaining = _entriesPool.Count; remaining > 0; remaining--)
                {
                    var (entries, misses) = _entriesPool.Dequeue();
                    if (entries.Length >= size && entries.Length <= (long)MaxEntriesOversizeFactor * size)
                        return entries;
                        
                    // if it doesn't fit, we'll give a few more changes, but then let the GC collect it
                    if (misses + 1 < MaxMissesBeforeDiscard) 
                        _entriesPool.Enqueue((entries, misses + 1));
                }

                return GC.AllocateUninitializedArray<ScratchEntry>(size);
            }

            public void Retire(long retiredAtSeq, long[] keys, int[] heads, ScratchEntry[] entries)
            {
                Debug.Assert(_retiredGenerations.Count == 0 || _retiredGenerations.Peek().RetiredAtSeq <= retiredAtSeq,
                    "Retirement sequences must be monotone, reclamation assumes the oldest generation is at the head");
                _retiredGenerations.Enqueue((retiredAtSeq, keys, heads, entries));
            }

            public void ReclaimRetiredGenerations(long floor)
            {
                while (_retiredGenerations.TryPeek(out var retired) && retired.RetiredAtSeq <= floor)
                {
                    _retiredGenerations.Dequeue();
                    _slotsPool.Enqueue((retired.Keys, retired.Heads, 0));
                    _entriesPool.Enqueue((retired.Entries, 0));
                }
            }
        }

        public ScratchPagesTable(ActiveTransactions activeTransactions)
        {
            Debug.Assert(RuntimeHelpers.IsReferenceOrContainsReferences<ScratchEntry>() == false,
                "ScratchEntry must stay blittable - a reference field would put every page version back into the GC's scan");

            _activeTransactions = activeTransactions;
            (_keys, _heads) = ScratchTableBuffers.AllocateSlots(MinSlots);
        }

        public int VisibleCount => _visibleCount;

        internal (int UsedSlots, int VisibleCount, int UsedEntries, int FreeEntries) GetStateForTests() =>
            (_usedSlots, _visibleCount, _usedEntries, _freeEntries);

        internal void ForceRebuildForTests() => Rebuild();

        public void BeginWriteTransaction(long lastPublishedSeq)
        {
            Debug.Assert(lastPublishedSeq <= _seqCounter, "a published bound cannot come from a session that never began");

            _undo.Clear();
            _seq = ++_seqCounter;
            _lastPublishedSeq = lastPublishedSeq;
            _activeSnapshotsFetched = false;
        }

        [Conditional("DEBUG")]
        private void AssertVisibleCountMatches()
        {
            var actual = 0;
            for (var i = 0; i < _keys.Length; i++)
            {
                if (_keys[i] == NoKey)
                    continue;

                var head = _heads[i];
                if (head != NoEntry && _entries[head].IsRemoved == false)
                    actual++;
            }

            Debug.Assert(actual == _visibleCount,
                $"Scratch pages table reports {_visibleCount} visible pages but the slots hold {actual}");
        }

        public ScratchPagesSnapshot CaptureSnapshot()
        {
            AssertVisibleCountMatches();
            return new ScratchPagesSnapshot(_keys, _heads, _entries, _refs, _seq, _visibleCount);
        }

        public bool TryGetValue(long pageNumber, out PageFromScratchBuffer value)
        {
            var keys = _keys;
            var mask = keys.Length - 1;
            var i = GetIndex(keys.Length, pageNumber);
            while (true)
            {
                var k = keys[i];
                if (k == pageNumber)
                {
                    var head = _heads[i];
                    if (head == NoEntry || _entries[head].IsRemoved)
                        break;
                    value = Materialize(_entries, _refs, head);
                    return true;
                }

                if (k == NoKey)
                    break;

                i = (i + 1) & mask;
            }

            value = default;
            return false;
        }

        public bool ContainsKey(long pageNumber) => TryGetValue(pageNumber, out _);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static PageFromScratchBuffer Materialize(ScratchEntry[] entries, ScratchRef[] refs, int index)
        {
            ref var entry = ref entries[index];
            var refIndex = entry.RefIndex;
            if (refIndex < 0)
                return default;

            ref var scratchRef = ref refs[refIndex];
            return new PageFromScratchBuffer(
                scratchRef.File,
                scratchRef.State,
                entry.AllocatedInTransaction,
                entry.PositionInScratchBuffer,
                entry.PageNumberInDataFile,
                entry.PreviousVersion,
                entry.Size,
                entry.NumberOfPages);
        }

        public bool RacyIdleCleanupRequired()
        {
            var keys = _keys;
            var usedSlots = _usedSlots;

            var deadKeys = usedSlots - _visibleCount;
            if (deadKeys > usedSlots / 2 && usedSlots > MinSlots / 2)
                return true;

            if (_usedEntries - _freeEntries > Math.Max(MinEntries, _visibleCount * 4))
                return true;

            var targetSize = Math.Max(MinSlots, (int)BitOperations.RoundUpToPowerOf2((uint)(_visibleCount * 2 + 1)));
            return targetSize < keys.Length;
        }

        public void IdleCleanup()
        {
            if (RacyIdleCleanupRequired()) // this is done in under the write lock, the check is not racy here
                Rebuild();
        }

        public void Set(long pageNumber, in PageFromScratchBuffer value)
        {
            Debug.Assert(value.IsValid, "Set() must not be used to push tombstones");

            EnsureRoomForOneMoreEntry();
            var slotIndex = GetSlotForWrite(pageNumber);
            var entryIndex = AllocateEntry();
            var head = _heads[slotIndex];
            _undo.Add(pageNumber);

            var refIndex = GetOrAddRef(value.File, value.State);

            ref var entry = ref _entries[entryIndex];
            entry.PositionInScratchBuffer = value.PositionInScratchBuffer;
            entry.PageNumberInDataFile = value.PageNumberInDataFile;
            entry.AllocatedInTransaction = value.AllocatedInTransaction;
            entry.Size = value.Size;
            entry.PreviousVersion = value.PreviousVersion;
            entry.NumberOfPages = value.NumberOfPages;
            entry.RefIndex = refIndex;
            entry.Seq = _seq;

            if (head != NoEntry && _entries[head].Seq == _seq && SurvivesRollback(head) == false)
            {
                entry.OlderIndex = _entries[head].OlderIndex;
                if (_entries[head].IsRemoved)
                    _visibleCount++;
                Volatile.Write(ref _heads[slotIndex], entryIndex);
                ParkEntry(head);
                return;
            }

            entry.OlderIndex = head;
            if (head == NoEntry || _entries[head].IsRemoved)
                _visibleCount++;
            Volatile.Write(ref _heads[slotIndex], entryIndex);
            PruneChain(entryIndex);
        }

        public bool Remove(long pageNumber, out PageFromScratchBuffer removed)
        {
            return RemoveInternal(pageNumber, survivesRollback: false, out removed);
        }

        public void RemoveFlushed(long pageNumber)
        {
            RemoveInternal(pageNumber, survivesRollback: true, out _);
        }

        private bool RemoveInternal(long pageNumber, bool survivesRollback, out PageFromScratchBuffer removed)
        {
            EnsureRoomForOneMoreEntry();

            if (TryFindSlot(pageNumber, out var index) == false)
            {
                removed = default;
                return false;
            }

            var head = _heads[index];
            if (head == NoEntry || _entries[head].IsRemoved)
            {
                removed = default;
                return false;
            }

            Debug.Assert(survivesRollback == false || _entries[head].Seq != _seq,
                "A journal flush must never remove a version created by the session applying it");

            removed = Materialize(_entries, _refs, head);
            if (survivesRollback == false)
                _undo.Add(pageNumber);
            _visibleCount--;

            if (_entries[head].Seq == _seq)
            {
                var older = _entries[head].OlderIndex;
                if (older == NoEntry || _entries[older].IsRemoved)
                {
                    Volatile.Write(ref _heads[index], older);
                    ParkEntry(head);
                    return true;
                }

                var replacement = CreateTombstone(pageNumber, survivesRollback, older);
                Volatile.Write(ref _heads[index], replacement);
                ParkEntry(head);
                return true;
            }

            var tombstone = CreateTombstone(pageNumber, survivesRollback, head);
            Volatile.Write(ref _heads[index], tombstone);
            PruneChain(tombstone);
            return true;
        }

        private int CreateTombstone(long pageNumberInDataFile, bool survivesRollback, int olderIndex)
        {
            var index = AllocateEntry();
            ref var entry = ref _entries[index];
            entry = default;
            entry.PageNumberInDataFile = pageNumberInDataFile;
            entry.AllocatedInTransaction = survivesRollback
                ? PageFromScratchBuffer.SurvivingTombstoneTx
                : PageFromScratchBuffer.TombstoneTx;
            entry.PositionInScratchBuffer = -1;
            entry.RefIndex = NoEntry;
            entry.OlderIndex = olderIndex;
            entry.Seq = _seq;
            return index;
        }

        private bool SurvivesRollback(int index) =>
            _entries[index].AllocatedInTransaction == PageFromScratchBuffer.SurvivingTombstoneTx;

        public void RollbackCurrentTransaction()
        {
            var pages = CollectionsMarshal.AsSpan(_undo);
            for (var i = 0; i < pages.Length; i++)
            {
                if (TryFindSlot(pages[i], out var index) == false)
                    continue;

                var head = _heads[index];
                var restored = head;
                while (restored != NoEntry && _entries[restored].Seq == _seq && SurvivesRollback(restored) == false)
                    restored = _entries[restored].OlderIndex;

                if (restored == head)
                    continue;

                var currentLive = head != NoEntry && _entries[head].IsRemoved == false;
                var restoredLive = restored != NoEntry && _entries[restored].IsRemoved == false;
                _visibleCount += restoredLive.ToInt32() - currentLive.ToInt32();
                Volatile.Write(ref _heads[index], restored);

                for (var node = head; node != restored; )
                {
                    var next = _entries[node].OlderIndex;
                    ParkEntry(node);
                    node = next;
                }
            }

            _undo.Clear();
        }

        private void PruneChain(int headIndex)
        {
            var node = _entries[headIndex].OlderIndex;
            if (node == NoEntry)
                return;

            EnsureActiveSnapshotsFetched();
            var pruneFloor = _activeSnapshots[0];

            var prev = headIndex;
            var depth = 1;
            while (_entries[node].Seq > pruneFloor)
            {
                prev = node;
                node = _entries[node].OlderIndex;
                depth++;
                if (node == NoEntry)
                    return;

                if (depth >= ChainDepthPruneThreshold)
                {
                    PruneChainPrecise(headIndex);
                    return;
                }
            }

            if (_entries[node].IsRemoved)
            {
                Volatile.Write(ref _entries[prev].OlderIndex, NoEntry);
                ParkChainFrom(node);
                return;
            }

            var stranded = _entries[node].OlderIndex;
            Volatile.Write(ref _entries[node].OlderIndex, NoEntry);
            ParkChainFrom(stranded);
        }

        private void PruneChainPrecise(int headIndex)
        {
            EnsureActiveSnapshotsFetched();

            var bounds = CollectionsMarshal.AsSpan(_activeSnapshots);
            var si = bounds.Length - 1;

            var prev = headIndex;
            while (_entries[prev].Seq > _lastPublishedSeq && _entries[prev].OlderIndex != NoEntry &&
                   _entries[_entries[prev].OlderIndex].Seq > _lastPublishedSeq)
                prev = _entries[prev].OlderIndex;

            int node;
            if (_entries[prev].Seq > _lastPublishedSeq)
            {
                node = _entries[prev].OlderIndex;
            }
            else
            {
                Debug.Assert(prev == headIndex, "a published entry below the head must enter the loop, not be skipped");
                var headSeq = _entries[headIndex].Seq;
                while (si >= 0 && bounds[si] >= headSeq)
                    si--;

                node = _entries[headIndex].OlderIndex;

                if (_entries[headIndex].IsRemoved && si < 0)
                {
                    Volatile.Write(ref _entries[headIndex].OlderIndex, NoEntry);
                    ParkChainFrom(node);
                    return;
                }
            }

            while (node != NoEntry)
            {
                var seq = _entries[node].Seq;

                if (si < 0 || bounds[si] < seq)
                {
                    var dropped = node;
                    node = _entries[node].OlderIndex;
                    Volatile.Write(ref _entries[prev].OlderIndex, node);
                    ParkEntry(dropped);
                    continue;
                }

                while (si >= 0 && bounds[si] >= seq)
                    si--;

                if (_entries[node].IsRemoved && si < 0)
                {
                    var stranded = _entries[node].OlderIndex;
                    Volatile.Write(ref _entries[node].OlderIndex, NoEntry);
                    ParkChainFrom(stranded);
                    return;
                }

                prev = node;
                node = _entries[node].OlderIndex;
            }
        }

        private void EnsureActiveSnapshotsFetched()
        {
            if (_activeSnapshotsFetched)
                return;
            _activeSnapshotsFetched = true;

            _activeSnapshots.Clear();
            foreach (var tx in _activeTransactions.Enumerate())
            {
                var seq = tx.ScratchSnapshotSeq;
                if (seq > _lastPublishedSeq)
                    continue;

                _activeSnapshots.Add(seq);
            }

            _activeSnapshots.Add(_lastPublishedSeq);

            var unique = Sorting.SortAndRemoveDuplicates(CollectionsMarshal.AsSpan(_activeSnapshots));
            CollectionsMarshal.SetCount(_activeSnapshots, unique);

            Volatile.Write(ref _prunedUpToSeq, _activeSnapshots[0]);

            _buffers.ReclaimRetiredGenerations(_activeSnapshots[0]);
            ReclaimParkedEntries(_activeSnapshots[0]);
        }

        private void EnsureRoomForOneMoreEntry()
        {
            if (_freeHead == NoEntry && _usedEntries >= _entries.Length)
                Rebuild();
        }

        private int AllocateEntry()
        {
            if (_freeHead != NoEntry)
            {
                var recycled = _freeHead;
                _freeHead = _entries[recycled].OlderIndex;
                _freeEntries--;
                return recycled;
            }

            Debug.Assert(_usedEntries < _entries.Length, "EnsureRoomForOneMoreEntry must run before the slot is resolved");
            return _usedEntries++;
        }

        private void FreeEntry(int index)
        {
            ref var entry = ref _entries[index];
            entry.RefIndex = NoEntry;
            entry.Seq = FreeSeq;
            entry.OlderIndex = _freeHead;
            _freeHead = index;
            _freeEntries++;
        }

        private void ParkEntry(int index)
        {
            Debug.Assert(_entries[index].Seq != FreeSeq, "an entry cannot be parked twice");
            _pendingFree.Enqueue((_seq, index));
        }

        private void ParkChainFrom(int index)
        {
            while (index != NoEntry)
            {
                ParkEntry(index);
                index = _entries[index].OlderIndex;
            }
        }

        private void ReclaimParkedEntries(long floor)
        {
            while (_pendingFree.TryPeek(out var parked) && parked.Seq <= floor)
            {
                _pendingFree.Dequeue();
                FreeEntry(parked.Index);
            }
        }

        private int GetOrAddRef(ScratchBufferFile file, Pager.State state)
        {
            for (var i = 0; i < _usedRefs; i++)
            {
                if (ReferenceEquals(_refs[i].File, file) && ReferenceEquals(_refs[i].State, state))
                    return i;
            }

            if (_usedRefs == _refs.Length)
            {
                var grown = new ScratchRef[_refs.Length * 2];
                Array.Copy(_refs, grown, _usedRefs);
                _refs = grown;
            }
            
            ref var r = ref _refs[_usedRefs];
            r.File = file;
            r.State = state;
            return _usedRefs++;
        }

        private bool TryFindSlot(long pageNumber, out int index)
        {
            var keys = _keys;
            var mask = keys.Length - 1;
            var i = GetIndex(keys.Length, pageNumber);
            while (true)
            {
                var k = keys[i];
                if (k == pageNumber)
                {
                    index = i;
                    return true;
                }

                if (k == NoKey)
                {
                    index = -1;
                    return false;
                }

                i = (i + 1) & mask;
            }
        }

        private int GetSlotForWrite(long pageNumber)
        {
            while (true)
            {
                var keys = _keys;
                var mask = keys.Length - 1;
                var i = GetIndex(keys.Length, pageNumber);
                while (true)
                {
                    var k = keys[i];
                    if (k == pageNumber)
                        return i;

                    if (k == NoKey)
                    {
                        if (_usedSlots + 1 > keys.Length - (keys.Length >> 2))
                            break;

                        _usedSlots++;
                        Volatile.Write(ref keys[i], pageNumber);
                        return i;
                    }

                    i = (i + 1) & mask;
                }

                Rebuild();
            }
        }

        private void Rebuild()
        {
            EnsureActiveSnapshotsFetched();

            var oldKeys = _keys;
            var oldHeads = _heads;
            var oldEntries = _entries;
            var live = 0;
            var liveEntries = 0;
            for (var i = 0; i < oldKeys.Length; i++)
            {
                if (oldKeys[i] == NoKey || oldHeads[i] == NoEntry)
                    continue;

                PruneChainPrecise(oldHeads[i]);
                if (IsDeadChain(oldHeads[i]))
                    continue;

                live++;
                for (var node = oldHeads[i]; node != NoEntry; node = oldEntries[node].OlderIndex)
                    liveEntries++;
            }

            var newSize = Math.Max(MinSlots, (int)BitOperations.RoundUpToPowerOf2((uint)(live * 2 + 1)));
            newSize = Math.Max(newSize, oldKeys.Length / 2);
            var (newKeys, newHeads) = _buffers.RentSlots(newSize);
            var newMask = newSize - 1;

            var newEntriesSize = Math.Max(MinEntries, (int)BitOperations.RoundUpToPowerOf2((uint)(liveEntries * 2 + 1)));
            var newEntries = _buffers.RentEntries(newEntriesSize);
            var newRefs = new ScratchRef[_refs.Length];
            var refMap = new int[_usedRefs];
            Array.Fill(refMap, NoEntry);

            var usedEntries = 0;
            var usedRefs = 0;

            for (var i = 0; i < oldKeys.Length; i++)
            {
                if (oldKeys[i] == NoKey || oldHeads[i] == NoEntry || IsDeadChain(oldHeads[i]))
                    continue;

                var j = GetIndex(newSize, oldKeys[i]);
                while (newKeys[j] != NoKey)
                    j = (j + 1) & newMask;

                newKeys[j] = oldKeys[i];

                var previous = NoEntry;
                for (var node = oldHeads[i]; node != NoEntry; node = oldEntries[node].OlderIndex)
                {
                    var target = usedEntries++;
                    newEntries[target] = oldEntries[node];
                    newEntries[target].OlderIndex = NoEntry;

                    var refIndex = oldEntries[node].RefIndex;
                    if (refIndex >= 0)
                    {
                        if (refMap[refIndex] == NoEntry)
                        {
                            refMap[refIndex] = usedRefs;
                            newRefs[usedRefs] = _refs[refIndex];
                            usedRefs++;
                        }

                        newEntries[target].RefIndex = refMap[refIndex];
                    }

                    if (previous == NoEntry)
                        newHeads[j] = target;
                    else
                        newEntries[previous].OlderIndex = target;

                    previous = target;
                }
            }

            _buffers.Retire(_seq, oldKeys, oldHeads, oldEntries);
            _pendingFree.Clear();

            _keys = newKeys;
            _heads = newHeads;
            _entries = newEntries;
            _refs = newRefs;
            _usedSlots = live;
            _usedEntries = usedEntries;
            _usedRefs = usedRefs;
            _freeHead = NoEntry;
            _freeEntries = 0;
        }

        private bool IsDeadChain(int headIndex)
        {
            return _entries[headIndex].IsRemoved && _entries[headIndex].OlderIndex == NoEntry;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int GetIndex(int slotsLength, long pageNumber)
        {
            var shift = 64 - BitOperations.Log2((uint)slotsLength);
            return (int)(unchecked((ulong)(pageNumber * -7046029254386353131L)) >> shift);
        }
    }

    public readonly struct ScratchPagesSnapshot : IEnumerable<KeyValuePair<long, PageFromScratchBuffer>>
    {
        private readonly long[] _keys;
        private readonly int[] _heads;
        private readonly ScratchEntry[] _entries;
        private readonly ScratchRef[] _refs;
        public readonly long VisibleAsOfSeq;
        public readonly int Count;

        internal ScratchPagesSnapshot(long[] keys, int[] heads, ScratchEntry[] entries, ScratchRef[] refs, long visibleAsOfSeq, int count)
        {
            _keys = keys;
            _heads = heads;
            _entries = entries;
            _refs = refs;
            VisibleAsOfSeq = visibleAsOfSeq;
            Count = count;
        }

        public static ScratchPagesSnapshot Empty => new([], [], [], [], 0, 0);

        public bool IsValid => _keys != null;

        public bool TryGetValue(long pageNumber, out PageFromScratchBuffer value)
        {
            if (Count == 0)
            {
                value = default;
                return false;
            }

            var keys = _keys;
            var heads = _heads;
            var entries = _entries;
            var mask = keys.Length - 1;
            var i = ScratchPagesTable.GetIndex(keys.Length, pageNumber);
            while (true)
            {
                var k = Volatile.Read(ref keys[i]);
                if (k == pageNumber)
                {
                    var node = Volatile.Read(ref heads[i]);
                    while (node != ScratchPagesTable.NoEntry && entries[node].Seq > VisibleAsOfSeq)
                    {
                        AssertNotFreed(entries, node);
                        node = Volatile.Read(ref entries[node].OlderIndex);
                    }
                    if (node != ScratchPagesTable.NoEntry)
                        AssertNotFreed(entries, node);

                    if (node == ScratchPagesTable.NoEntry || entries[node].IsRemoved)
                        break;

                    value = ScratchPagesTable.Materialize(entries, _refs, node);
                    return true;
                }

                if (k == ScratchPagesTable.NoKey)
                    break;

                i = (i + 1) & mask;
            }

            value = default;
            return false;
        }

        public bool ContainsKey(long pageNumber) => TryGetValue(pageNumber, out _);

        [Conditional("DEBUG")]
        private static void AssertNotFreed(ScratchEntry[] entries, int index)
        {
            Debug.Assert(entries[index].Seq != ScratchPagesTable.FreeSeq,
                $"A snapshot traversal reached entry {index}, which is on the free list. " +
                "It was recycled while still reachable.");
        }

        public Enumerator GetEnumerator() => new(this);

        IEnumerator<KeyValuePair<long, PageFromScratchBuffer>> IEnumerable<KeyValuePair<long, PageFromScratchBuffer>>.GetEnumerator() => GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public struct Enumerator : IEnumerator<KeyValuePair<long, PageFromScratchBuffer>>
        {
            private readonly ScratchPagesSnapshot _snapshot;
            private int _index;
            private KeyValuePair<long, PageFromScratchBuffer> _current;

            internal Enumerator(ScratchPagesSnapshot snapshot)
            {
                _snapshot = snapshot;
                _index = -1;
                _current = default;
            }

            public bool MoveNext()
            {
                var keys = _snapshot._keys;
                if (keys == null || _snapshot.Count == 0)
                    return false;

                var heads = _snapshot._heads;
                var entries = _snapshot._entries;
                while (++_index < keys.Length)
                {
                    var k = Volatile.Read(ref keys[_index]);
                    if (k == ScratchPagesTable.NoKey)
                        continue;

                    var node = Volatile.Read(ref heads[_index]);
                    while (node != ScratchPagesTable.NoEntry && entries[node].Seq > _snapshot.VisibleAsOfSeq)
                    {
                        AssertNotFreed(entries, node);
                        node = Volatile.Read(ref entries[node].OlderIndex);
                    }
                    if (node != ScratchPagesTable.NoEntry)
                        AssertNotFreed(entries, node);

                    if (node == ScratchPagesTable.NoEntry || entries[node].IsRemoved)
                        continue;

                    _current = new KeyValuePair<long, PageFromScratchBuffer>(
                        k,
                        ScratchPagesTable.Materialize(entries, _snapshot._refs, node));
                    return true;
                }

                return false;
            }

            public KeyValuePair<long, PageFromScratchBuffer> Current => _current;

            object IEnumerator.Current => _current;

            public void Reset()
            {
                _index = -1;
                _current = default;
            }

            public void Dispose()
            {
            }
        }
    }
}
