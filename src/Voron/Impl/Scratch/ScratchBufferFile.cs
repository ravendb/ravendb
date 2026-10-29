using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using Sparrow.Threading;
using Voron.Global;
using Voron.Impl.Paging;
using Sparrow.Server.Utils;
using System.Diagnostics.CodeAnalysis;
using Sparrow.Binary;
using Sparrow.Server.LowMemory;

namespace Voron.Impl.Scratch
{
    public sealed class ScratchBufferFile : IDisposable
    {
        private sealed class PendingPage
        {
            public long Page;
            public long ValidAfterTransactionId;
            public long AllocatedInTransaction;
        }

        private readonly Pager _scratchPager;
        private Pager.State _scratchPagerState;
        private readonly int _scratchNumber;

        private readonly Dictionary<long, LinkedList<PendingPage>> _freePagesBySize = new();
        private readonly DisposeOnce<SingleAttempt> _disposeOnceRunner;

#if DEBUG
        // Bookkeeping for the debug assertions only, concurrent because VerifyMatch runs read transactions
        private readonly ConcurrentDictionary<long, PageFromScratchBuffer> _allocatedPages = new();
#endif

        private int _numberOfAllocations;
        private long _allocatedPagesCount;
        private long _lastUsedPage;
        private long _txIdAfterWhichLatestFreePagesBecomeAvailable = -1;
        private StrongReference<Func<long>> _strongRefToAllocateInBytesFunc;

        public long LastUsedPage => _lastUsedPage;

        public ScratchBufferFile(Pager scratchPager,  Pager.State scratchPagerState, int scratchNumber)
        {
            _scratchPager = scratchPager;
            _scratchPagerState = scratchPagerState;
            _scratchNumber = scratchNumber;
            _allocatedPagesCount = 0;

            _strongRefToAllocateInBytesFunc = new StrongReference<Func<long>>
            {
                Value = () => AllocatedPagesCount * Constants.Storage.PageSize
            };
            MemoryInformation.DirtyMemoryObjects.TryAdd(_strongRefToAllocateInBytesFunc);

            DebugInfo = new ScratchFileDebugInfo(this);

            _disposeOnceRunner = new DisposeOnce<SingleAttempt>(DisposeImpl);
        }

        private void DisposeImpl()
        {
            _strongRefToAllocateInBytesFunc.Value = null; // remove ref (so if there's a left over refs in DirtyMemoryObjects but also function as _disposed = true for racy func invoke)
            MemoryInformation.DirtyMemoryObjects.TryRemove(_strongRefToAllocateInBytesFunc);
            _strongRefToAllocateInBytesFunc = null;

            _scratchPager.Dispose();
            ClearDictionaries();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ClearDictionaries()
        {
            _freePagesBySize.Clear();
            _numberOfAllocations = 0;
#if DEBUG
            _allocatedPages.Clear();
#endif
        }

        public void Reset()
        {
            _scratchPager.DiscardWholeFile(_scratchPagerState);

            ClearDictionaries();
            _txIdAfterWhichLatestFreePagesBecomeAvailable = -1;
            _lastUsedPage = 0;
            _allocatedPagesCount = 0;

            DebugInfo.NumberOfResets++;
            DebugInfo.LastResetTime = DateTime.UtcNow;
        }

        internal (Pager, Pager.State) GetPagerAndState() => (_scratchPager, _scratchPagerState);
        
        public Pager Pager => _scratchPager;

        public int Number => _scratchNumber;

        public int NumberOfAllocations => _numberOfAllocations;

        public long Size => _scratchPagerState.NumberOfAllocatedPages * Constants.Storage.PageSize;

        public long NumberOfAllocatedPages => _scratchPagerState.NumberOfAllocatedPages;

        public long AllocatedPagesCount => _allocatedPagesCount;

        public long TxIdAfterWhichLatestFreePagesBecomeAvailable => _txIdAfterWhichLatestFreePagesBecomeAvailable;

        public ScratchFileDebugInfo DebugInfo { get; }

        public PageFromScratchBuffer Allocate(LowLevelTransaction tx, int numberOfPages, int sizeToAllocate, long pageNumber, Page previousVersion)
        {
            _scratchPager.EnsureContinuous(ref _scratchPagerState, _lastUsedPage, sizeToAllocate);
            
            var result = new PageFromScratchBuffer(this,_scratchPagerState, tx.Id, _lastUsedPage, pageNumber, previousVersion, sizeToAllocate, numberOfPages);

            _allocatedPagesCount += numberOfPages;
            _numberOfAllocations++;
            TrackAllocation(_lastUsedPage, result);
            _lastUsedPage += sizeToAllocate;

            return result;
        }

        public bool TryGettingFromAllocatedBuffer(LowLevelTransaction tx, int numberOfPages, int size, long pageNumber, Page previousVersion, out PageFromScratchBuffer result)
        {
            result = default;

            if (_freePagesBySize.TryGetValue(size, out LinkedList<PendingPage> list) == false || list.Count <= 0)
                return false;

            var val = list.Last!.Value;

            if (val.ValidAfterTransactionId >= tx.Environment.PossibleOldestReadTransaction(tx))
                return false;

            list.RemoveLast();

#if VALIDATE
            byte* freePageBySizePointer = _scratchPager.AcquirePagePointer(tx, val.Page, PagerState);
            ulong freePageBySizeSize = (ulong)size * Constants.Storage.PageSize;
            // This has to be forced, as the list of available pages should be protected by default, but this
            // is a policy we implement inside the ScratchBufferFile only.
            _scratchPager.UnprotectPageRange(freePageBySizePointer, freePageBySizeSize, true);
#endif

            result = new PageFromScratchBuffer(this, _scratchPagerState, tx.Id,val.Page, pageNumber, previousVersion, size, numberOfPages);

            _allocatedPagesCount += numberOfPages;
            _numberOfAllocations++;
            TrackAllocation(val.Page, result);
            return true;
        }

        public bool HasActivelyUsedBytes(long oldestActiveTransaction)
        {
            return _allocatedPagesCount > 0 || oldestActiveTransaction <= _txIdAfterWhichLatestFreePagesBecomeAvailable;
        }

        public bool Free(LowLevelTransaction tx, in PageFromScratchBuffer value)
        {
            return Free(tx, tx.Id, value);
        }

        public bool Free(LowLevelTransaction tx, long asOfTxId, in PageFromScratchBuffer value)
        {
#if VALIDATE
            // If we have encryption enabled, then VALIDATE calls are handled by the EncryptionBufferPool
            if (Pager.Options.Encryption.IsEnabled == false)
            {
                using (var tempTx = new TempPagerTransaction())
                {
                    var pagePointer = _scratchPager.AcquirePagePointer(tempTx, value.PositionInScratchBuffer, PagerState);
                    var freedPage = new Page(pagePointer);
                    var pageSize = (ulong)(freedPage.IsOverflow ? VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(freedPage.OverflowSize) : 1) *
                                   Constants.Storage.PageSize;
                    _scratchPager.ProtectPageRange(pagePointer, pageSize, true);
                }
            }
#endif

            AssertAllocated(value);

            tx.ForgetAboutScratchPage(value);
            DebugInfo.LastFreeTime = DateTime.UtcNow;
            // use current write tx id to prevent from overriding a scratch page by write tx 
            // while there might be old read tx looking at it, so we'll only allocate from it
            // _after_ all transactions are past the _current_ write transaction
            DebugInfo.LastAsOfTxIdWhenFree = asOfTxId;

            _allocatedPagesCount -= value.NumberOfPages;
            _numberOfAllocations--;
            ForgetAllocation(value.PositionInScratchBuffer);

            Debug.Assert(value.NumberOfPages > 0);

            if (_freePagesBySize.TryGetValue(value.Size, out LinkedList<PendingPage> list) == false)
            {
                list = new LinkedList<PendingPage>();
                _freePagesBySize[value.Size] = list;
            }

            var pending = new PendingPage
            {
                Page = value.PositionInScratchBuffer,
                ValidAfterTransactionId = asOfTxId,
                AllocatedInTransaction = value.AllocatedInTransaction,
            };

            if (asOfTxId >= 0)
            {
                _txIdAfterWhichLatestFreePagesBecomeAvailable = asOfTxId;
                list.AddFirst(pending);
            }
            else
            { 
                // -1 indicates that this is visible to all transactions, so make it the first available in the queue 
                list.AddLast(pending);
            }


            return NumberOfAllocations == 0;
        }

        public ref Pager.State GetStateRef() => ref _scratchPagerState;

        [Conditional("DEBUG")]
        private void AssertAllocated(in PageFromScratchBuffer value)
        {
#if DEBUG
            if (_allocatedPages.TryGetValue(value.PositionInScratchBuffer, out var tracked) == false)
                throw new InvalidOperationException("Attempt to free page that wasn't currently allocated: " + value.PositionInScratchBuffer);

            // the caller hands us the entry to release, so a stale copy - one taken before ShrinkOverflowPage
            // replaced it - would take the wrong number of pages off the allocation count
            if (tracked.NumberOfPages != value.NumberOfPages || tracked.PageNumberInDataFile != value.PageNumberInDataFile)
                throw new InvalidOperationException(
                    $"Attempt to free scratch page {value.PositionInScratchBuffer} with a stale entry: freeing page {value.PageNumberInDataFile} " +
                    $"({value.NumberOfPages} pages) while the scratch file holds page {tracked.PageNumberInDataFile} ({tracked.NumberOfPages} pages)");
#endif
        }

        public void Dispose()
        {
            _disposeOnceRunner.Dispose();
        }

        public bool IsDisposed => _disposeOnceRunner.Disposed;

        public PageFromScratchBuffer ShrinkOverflowPage(in PageFromScratchBuffer value, int newNumberOfPages)
        {
            AssertAllocatedForShrink(value);

            Debug.Assert(value.NumberOfPages > 1);
            Debug.Assert(value.NumberOfPages > newNumberOfPages);

            var shrinked = value with
            {
                NumberOfPages = newNumberOfPages, 
                PreviousVersion = value.PreviousVersion
            }; 

            TrackAllocation(shrinked.PositionInScratchBuffer, shrinked);

            _allocatedPagesCount -= value.NumberOfPages - newNumberOfPages;

            return shrinked;
        }

        [Conditional("DEBUG")]
        private void AssertAllocatedForShrink(in PageFromScratchBuffer value)
        {
#if DEBUG
            if (_allocatedPages.ContainsKey(value.PositionInScratchBuffer) == false)
                throw new InvalidOperationException($"Attempt to shrink a page that wasn't currently allocated: {value.PositionInScratchBuffer}");
#endif
        }

        [Conditional("DEBUG")]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void TrackAllocation(long positionInScratchBuffer, in PageFromScratchBuffer allocated)
        {
#if DEBUG
            _allocatedPages[positionInScratchBuffer] = allocated;
#endif
        }

        [Conditional("DEBUG")]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ForgetAllocation(long positionInScratchBuffer)
        {
#if DEBUG
            _allocatedPages.TryRemove(positionInScratchBuffer, out _);
#endif
        }

        public sealed class ScratchFileDebugInfo
        {
            private readonly ScratchBufferFile _parent;

            public ScratchFileDebugInfo(ScratchBufferFile parent)
            {
                _parent = parent;
            }

            public DateTime? LastResetTime { get; set; }

            public int NumberOfResets { get; set; }

            public DateTime? LastFreeTime { get; set; }

            public long LastAsOfTxIdWhenFree { get; set; }

            internal Dictionary<long, (long ValidAfterTransactionId, long AllocatedInTransaction)> GetMostAvailableFreePagesBySize()
            {
                return _parent._freePagesBySize.Keys.ToDictionary(size => size, size =>
                {
                    if (_parent._freePagesBySize.TryGetValue(size, out var pendingPages) == false)
                        return (-1, -1);

                    var value = pendingPages.Last?.Value;
                    if (value == null)
                        return (-1, -1);

                    return (value.ValidAfterTransactionId, value.AllocatedInTransaction);
                });
            }

        }

        [Conditional("DEBUG")]
        public void VerifyMatch(long pageNumberInDataFile, long positionInScratchBuffer, int numberOfPages)
        {
#if DEBUG
            if (_allocatedPages.TryGetValue(positionInScratchBuffer, out var allocated) is false)
                return;
            
            if(allocated.PageNumberInDataFile != pageNumberInDataFile || 
               allocated.NumberOfPages != numberOfPages)
                throw new InvalidOperationException(
                    $"Failed to verify page {pageNumberInDataFile} when reading scratch page {positionInScratchBuffer}, values different!" +
                    $"Page: {pageNumberInDataFile} vs. {allocated.PageNumberInDataFile} ({numberOfPages} vs {allocated.NumberOfPages})!");
#endif
        }

        [Conditional("DEBUG")]
        public void AssertNoPagesAllocatedInTransactionOlderThan(long txId)
        {
#if DEBUG
            foreach (PageFromScratchBuffer p in _allocatedPages.Values)
            {
                if (p.AllocatedInTransaction < txId)
                {
                    var message =
                        $"Found page #{p.PageNumberInDataFile} allocated in tx {p.AllocatedInTransaction} (scratch {p.File.Number}, pos in scratch: {p.PositionInScratchBuffer}) while we freed up to tx {txId}";

                    throw new InvalidOperationException(message);
                }
            }
#endif
        }
    }
}
