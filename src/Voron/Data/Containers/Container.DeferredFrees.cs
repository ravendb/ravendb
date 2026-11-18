using System;
using System.Collections.Generic;
using Sparrow.Server;
using Voron.Data.Fixed;
using Voron.Data.PostingLists;
using Voron.Exceptions;
using Voron.Global;
using Voron.Impl;
using Voron.Util;

namespace Voron.Data.Containers;

public readonly unsafe ref partial struct Container
{
    private const int FreeNowThreshold = 8;
    private const int ReclaimOnAllocateMinItems = 8;

    private const int MaxPendingRatio = 16;
    internal const int MinPendingCap = 4096;

    private const int ReclaimScanTries = 16;

    public static void DeleteOrDefer(LowLevelTransaction llt, ContainerId containerId, ContainerEntryId id)
    {
        var txState = llt.Transaction.GetContainerState(containerId);
        if (txState._deferredFreesProcessed)
            throw new VoronErrorException($"Attempt to defer a free of item {id} in container {containerId} after the deferred frees were already processed");

        long pageNum = (long)id / Constants.Storage.PageSize;
        if (llt.IsDirty(pageNum) || llt.GetPage(pageNum).IsOverflow)
        {
            Delete(llt, containerId, id);
            return;
        }

        if (txState._deferredFrees.HasContext == false)
            txState._deferredFrees = new ContextBoundNativeList<long>(llt.Allocator);
        txState._deferredFrees.Add((long)id);
        txState.EnsureDeferredFreesHook(llt.Transaction);
    }

    internal static PostingListState GetPendingFreesState(LowLevelTransaction llt, ContainerId containerId)
    {
        return llt.Transaction.GetContainerState(containerId).GetPendingFrees(llt, create: false)?.State ?? default;
    }

    internal static long GetNumberOfPendingFrees(LowLevelTransaction llt, ContainerId containerId) => GetPendingFreesState(llt, containerId).NumberOfEntries;

    private static bool TryReclaimPendingFreesPage(LowLevelTransaction llt, ContainerId containerId, Container rootContainer, TransactionState txState,
        long pageLevelMetadata, int size, out Container reclaimed)
    {
        reclaimed = default;

        if (txState._deferredFreesProcessed)
            return false;

        var pending = txState.GetPendingFrees(llt, create: false);
        if (pending == null || pending.State.NumberOfEntries == 0)
            return false;

        txState._pendingFreesScanPosition ??= txState.ReadPendingFreesScanPosition(llt);
        txState.EnsureDeferredFreesHook(llt.Transaction);
        using var _ = llt.Allocator.Allocate(MaxItemsPerPage, out Span<long> buffer);
        bool wrapped = false;

        for (int tries = 0; tries < ReclaimScanTries; tries++)
        {
            long page = txState.NextPendingFreesPage(txState._pendingFreesScanPosition.Value);
            if (page < 0)
            {
                if (wrapped) // this is circular buffer. It means we started from the beginning and reached the end again.
                    break;
                wrapped = true;
                txState._pendingFreesScanPosition = 0;
                continue;
            }

            txState._pendingFreesScanPosition = (page + 1) * Constants.Storage.PageSize;

            // reclaimed, we need to skip it.
            if (txState._reclaimedPendingPages?.Contains(page) == true)
                continue;

            // gather all pending ids of this page
            var ids = buffer[..txState.ReadPendingFreesOfPage(page, buffer)];

            // it's better to reclaim pages in batches, so we're looking for a page that has enough pending frees to reclaim and it's metadata matches
            if (ids.Length < ReclaimOnAllocateMinItems || PageMetadataMatch(new Container(llt.GetPage(page)), pageLevelMetadata) == false)
                continue;

            if (TryReclaimPage(llt, containerId, rootContainer, txState, page, ids, size, out reclaimed))
                return true;
        }

        return false;
    }

    private static bool TryReclaimPage(LowLevelTransaction llt, ContainerId containerId, Container rootContainer, TransactionState txState,
        long page, scoped ReadOnlySpan<long> ids, int size, out Container reclaimed)
    {
        reclaimed = default;

        foreach (long id in ids)
            Delete(llt, containerId, new ContainerEntryId(id));


        // Store what we have done in current Tx in order to update deferred frees
        if (txState._reclaimedPendingFrees.HasContext == false)
            txState._reclaimedPendingFrees = new ContextBoundNativeList<long>(llt.Allocator);
        txState._reclaimedPendingFrees.AddRange(ids);

        (txState._reclaimedPendingPages ??= new HashSet<long>()).Add(page);

        // Actually we removed the page because all the items of the page were pending, the page itself was freed
        if (txState.Removals.Contains(page))
            return false;

        var candidate = new Container(llt.ModifyPage(page));
        if (candidate.HasEnoughSpaceFor(size + MinimumAdditionalFreeSpaceToConsider) == false)
        {
            candidate.Defrag(llt);
            if (candidate.HasEnoughSpaceFor(size + MinimumAdditionalFreeSpaceToConsider) == false)
                return false;
        }

        rootContainer.UpdateNextFreePage(page);
        reclaimed = candidate;
        return true;
    }

    public partial class TransactionState
    {
        internal ContextBoundNativeList<long> _deferredFrees;
        internal ContextBoundNativeList<long> _reclaimedPendingFrees;
        internal HashSet<long> _reclaimedPendingPages;
        internal long? _pendingFreesScanPosition;
        internal bool _deferredFreesProcessed;

        private PendingFreesState _storedPendingFrees;
        private bool _deferredFreesHook, _pendingFreesLoaded, _hasPendingIterator;
        private PostingList _pendingFrees;
        private PostingList.Iterator _pendingIterator;

        private struct PendingFreesState
        {
            public PostingListState PostingList;
            public long ScanPosition;
        }

        internal void EnsureDeferredFreesHook(Transaction tx)
        {
            if (_deferredFreesHook)
                return;
            _deferredFreesHook = true;
            tx.OnBeforeCommit += ProcessDeferredFrees;
        }

        private static FixedSizeTree PendingFreesTree(LowLevelTransaction llt) => llt.Transaction.FixedTreeFor(Constants.Container.PendingFrees, (ushort)sizeof(PendingFreesState));

        internal PostingList GetPendingFrees(LowLevelTransaction llt, bool create)
        {
            if (_pendingFreesLoaded == false)
            {
                _pendingFreesLoaded = true;
                byte* stored = PendingFreesTree(llt).ReadPtr((long)ContainerId, out _);
                if (stored != null)
                {
                    _storedPendingFrees = *(PendingFreesState*)stored;
                    _pendingFrees = new PostingList(llt, Slices.Empty, _storedPendingFrees.PostingList);
                }
            }

            if (_pendingFrees == null && create)
            {
                PostingListState state = default;
                PostingList.Create(llt, ref state);
                _pendingFrees = new PostingList(llt, Slices.Empty, state);
            }

            return _pendingFrees;
        }

        internal long ReadPendingFreesScanPosition(LowLevelTransaction llt)
        {
            GetPendingFrees(llt, create: false);
            return _storedPendingFrees.ScanPosition;
        }

        private void StorePendingFrees(LowLevelTransaction llt, bool modified)
        {
            long position = _pendingFreesScanPosition ?? _storedPendingFrees.ScanPosition;
            if (_pendingFrees == null || (modified == false && position == _storedPendingFrees.ScanPosition))
                return;

            _pendingFrees.PrepareForCommit();
            var state = new PendingFreesState { PostingList = _pendingFrees.State, ScanPosition = position };
            using (Slice.External(llt.Allocator, (byte*)&state, sizeof(PendingFreesState), out Slice value))
                PendingFreesTree(llt).Add((long)ContainerId, value);
        }

        // the posting list is modified only at commit, so it can be read during the transaction
        private ref PostingList.Iterator PendingIterator()
        {
            if (_hasPendingIterator == false)
            {
                _pendingIterator = _pendingFrees.Iterate();
                _hasPendingIterator = true;
            }

            return ref _pendingIterator;
        }

        internal long NextPendingFreesPage(long from)
        {
            ref var it = ref PendingIterator();
            if (it.Seek(from) == false)
                return -1;

            Span<long> buffer = stackalloc long[64];
            while (it.Fill(buffer, out int read) && read > 0)
            {
                foreach (long id in buffer[..read])
                {
                    if (id >= from)
                        return id / Constants.Storage.PageSize;
                }
            }

            return -1;
        }

        internal int ReadPendingFreesOfPage(long page, Span<long> output)
        {
            int count = 0;
            long from = page * Constants.Storage.PageSize, to = from + Constants.Storage.PageSize;
            ref var it = ref PendingIterator();
            if (it.Seek(from) == false)
                return count;

            // the ids are sorted, so the ones of the page are a single range of what Fill read: move it right after the previous ones
            while (count < output.Length && it.Fill(output[count..], out int read, pruneGreaterThanOptimization: to))
            {
                var chunk = output.Slice(count, read);
                int start = 0;
                while (start < read && chunk[start] < from)
                    start++;
                int end = start;
                while (end < read && chunk[end] < to)
                    end++;

                chunk[start..end].CopyTo(output[count..]);
                count += end - start;
                if (end < read)
                    break;
            }

            return count;
        }

        private void ProcessDeferredFrees(Transaction tx)
        {
            var llt = tx.LowLevelTransaction;
            var pending = GetPendingFrees(llt, create: false);
            var handledPages = _reclaimedPendingPages ?? new HashSet<long>();
            using var freeNow = new ContextBoundNativeList<long>(llt.Allocator);
            using var pendingFreeNow = new ContextBoundNativeList<long>(llt.Allocator);
            using var pendingToAdd = new ContextBoundNativeList<long>(llt.Allocator);
            using var _ = llt.Allocator.Allocate(MaxItemsPerPage * sizeof(long), out ByteString pageFreesMemory);
            var pageFrees = pageFreesMemory.ToSpan<long>();

            if (_deferredFrees.Count > 0)
                _deferredFrees.Sort();
            var frees = _deferredFrees.ToSpan();
            for (int start = 0, end; start < frees.Length; start = end)
            {
                long page = frees[start] / Constants.Storage.PageSize;
                for (end = start + 1; end < frees.Length && frees[end] / Constants.Storage.PageSize == page; end++)
                {
                }

                if (llt.IsDirty(page) == false && end - start < FreeNowThreshold)
                {
                    pendingToAdd.AddRange(frees[start..end]);
                    continue;
                }

                freeNow.AddRange(frees[start..end]);
                // the page is written anyway, so the pending frees of the page from previous transactions are done now as well
                if (pending != null && handledPages.Add(page))
                {
                    pendingFreeNow.AddRange(pageFrees[..ReadPendingFreesOfPage(page, pageFrees)]);
                }
            }

            long pendingCount = (pending?.State.NumberOfEntries ?? 0) - _reclaimedPendingFrees.Count - pendingFreeNow.Count + pendingToAdd.Count;
            long cap = Math.Max(MinPendingCap, new Container(llt.GetPage((long)ContainerId)).GetNumberOfEntries() / MaxPendingRatio);
            if (pendingCount > cap)
            {
                var existing = pending?.DumpAllValues() ?? new();
                List<long> candidates =
                [
                    .. existing,
                    .. pendingToAdd.ToSpan()
                ];
                candidates.Sort();
                var pages = ReclaimDensestPages(candidates, pendingCount - cap * 3 / 4, handledPages);

                foreach (long id in existing)
                {
                    if (pages.Contains(id / Constants.Storage.PageSize))
                        pendingFreeNow.Add(id);
                }

                int kept = 0;
                foreach (long id in pendingToAdd.ToSpan())
                {
                    if (pages.Contains(id / Constants.Storage.PageSize))
                        freeNow.Add(id);
                    else
                        pendingToAdd[kept++] = id;
                }

                pendingToAdd.Shrink(kept);
            }

            foreach (long id in freeNow)
                Delete(llt, ContainerId, new ContainerEntryId(id));
            foreach (long id in pendingFreeNow)
                Delete(llt, ContainerId, new ContainerEntryId(id));

            bool modified = pendingToAdd.Count > 0 || pendingFreeNow.Count > 0 || _reclaimedPendingFrees.Count > 0;
            if (modified)
            {
                pending ??= GetPendingFrees(llt, create: true);
                foreach (long id in pendingToAdd)
                    pending.Add(id);
                foreach (long id in pendingFreeNow)
                    pending.Remove(id);
                foreach (long id in _reclaimedPendingFrees)
                    pending.Remove(id);
            }

            StorePendingFrees(llt, modified);
            DisposeDeferredFrees();
            _reclaimedPendingPages = null;
            _deferredFreesProcessed = true;
        }

        internal void DisposeDeferredFrees()
        {
            _deferredFrees.Dispose();
            _deferredFrees = default;
            _reclaimedPendingFrees.Dispose();
            _reclaimedPendingFrees = default;
        }

        internal void ExcludePendingFrees(LowLevelTransaction llt, List<long> ids)
        {
            var pending = GetPendingFrees(llt, create: false)?.DumpAllValues() ?? new HashSet<long>();
            foreach (long id in _reclaimedPendingFrees)
                pending.Remove(id); // freed already, new items may have taken these ids
            foreach (long id in _deferredFrees)
                pending.Add(id);
            if (pending.Count > 0)
                ids.RemoveAll(pending.Contains);
        }

        private static HashSet<long> ReclaimDensestPages(List<long> all, long target, HashSet<long> handledPages)
        {
            var groups = new List<(long Page, int Count)>();
            for (int start = 0, end; start < all.Count; start = end)
            {
                long page = all[start] / Constants.Storage.PageSize;
                for (end = start + 1; end < all.Count && all[end] / Constants.Storage.PageSize == page; end++)
                {
                }

                if (handledPages.Contains(page) == false)
                    groups.Add((page, end - start));
            }

            groups.Sort((x, y) => y.Count.CompareTo(x.Count));
            var pages = new HashSet<long>();
            long reclaimed = 0;
            foreach (var group in groups)
            {
                if (reclaimed >= target)
                    break;
                handledPages.Add(group.Page);
                pages.Add(group.Page);
                reclaimed += group.Count;
            }

            return pages;
        }
    }
}
