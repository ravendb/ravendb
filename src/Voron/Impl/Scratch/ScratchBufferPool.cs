using Sparrow;
using Sparrow.Binary;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using Voron.Global;
using Voron.Impl.Paging;

namespace Voron.Impl.Scratch
{
    /// <summary>
    /// This class implements the page pool for in flight transaction information
    /// Pages allocated from here are expected to live after the write transaction that 
    /// created them. The pages will be kept around until the flush for the journals
    /// send them to the data file.
    /// 
    /// This class relies on external synchronization and is not meant to be used in multiple
    /// threads at the same time
    /// </summary>
    public unsafe class ScratchBufferPool : IDisposable
    {
        private readonly StorageEnvironment _env;
        // Immutable state. 
        private readonly StorageEnvironmentOptions _options;

        // Local per scratch file potentially read delayed inconsistent (need guards). All must be modified atomically (but it wont necessarily require a memory barrier)
        private ScratchBufferItem _current;

        // Local writable state. Can perform multiple reads, but must never do multiple writes simultaneously.
        private int _currentScratchNumber = -1;

        private Dictionary<int, PagerState> _pagerStatesAllScratchesCache;

        private readonly ConcurrentDictionary<int, ScratchBufferItem> _scratchBuffers =
            new ConcurrentDictionary<int, ScratchBufferItem>(NumericEqualityComparer.Instance);

        private readonly LinkedList<ScratchBufferItem> _recycleArea = new LinkedList<ScratchBufferItem>();

        public ScratchBufferPool(StorageEnvironment env)
        {
            _env = env;
            _options = env.Options;
            _current = NextFile(_options.InitialLogFileSize, null);
            UpdateCacheForPagerStatesOfAllScratches();
        }

        public Dictionary<int, PagerState> GetPagerStatesOfAllScratches()
        {
            return _pagerStatesAllScratchesCache;
        }

        public void UpdateCacheForPagerStatesOfAllScratches()
        {
            var dic = new Dictionary<int, PagerState>(NumericEqualityComparer.Instance);
            foreach (var scratchBufferItem in _scratchBuffers)
            {
                dic[scratchBufferItem.Key] = scratchBufferItem.Value.File.PagerState;
            }

            // for the lifetime of this cache, we have to hold a reference to the 
            // pager state, to avoid handing out garbage to transactions
            // note that this call is protected from running concurrently with the 
            // call to GetPagerStatesOfAllScratches()

            foreach (var pagerState in dic)
            {
                pagerState.Value.AddRef();
            }
            var old = _pagerStatesAllScratchesCache;
            _pagerStatesAllScratchesCache = dic;
            if (old == null)
                return;

            // release the references for the previous instance
            foreach (var pagerState in old)
            {
                pagerState.Value.Release();
            }
        }

        internal long GetNumberOfAllocations(int scratchNumber)
        {
            // While used only in tests, there is no multithread risk. 
            return _scratchBuffers[scratchNumber].File.NumberOfAllocations;
        }

        private ScratchBufferItem NextFile(long minSize, long? requestedSize)
        {
            var current = _recycleArea.Last;
            while (current != null)
            {
                var recycled = current.Value;

                if (recycled.File.Size >= Math.Max(minSize, requestedSize ?? 0) &&
                    // even though this is in the recyle bin, there might still be some transactions looking at it
                    // so we have to make sure that this is really unused before actually reusing it
                    recycled.File.HasActivelyUsedBytes(_env.PossibleOldestReadTransaction) == false)
                {
                    recycled.File.Reset();
                    recycled.RecycledAt = default(DateTime);
                    _recycleArea.Remove(current);
                    AddScratchBufferFile(recycled);

                    return recycled;
                }

                current = current.Previous;
            }
            
            _currentScratchNumber++;
            AbstractPager scratchPager;
            if (requestedSize != null)
            {
                try
                {
                    scratchPager =
                        _options.CreateScratchPager(StorageEnvironmentOptions.ScratchBufferName(_currentScratchNumber),
                            requestedSize.Value);
                }
                catch (Exception)
                {
                    // this can fail because of disk space issue, let us just ignore it
                    // we'll allocate the minimum amount in a bit anyway
                    return NextFile(minSize, null);
                }
            }
            else
            {
                scratchPager = _options.CreateScratchPager(StorageEnvironmentOptions.ScratchBufferName(_currentScratchNumber),
                    Math.Max(_options.InitialLogFileSize, minSize));
            }

            var scratchFile = new ScratchBufferFile(scratchPager, _currentScratchNumber);
            var item = new ScratchBufferItem(scratchFile.Number, scratchFile);

            AddScratchBufferFile(item);

            return item;
        }
        public PagerState GetPagerState(int scratchNumber)
        {
            // Not thread-safe but only called by a single writer.
            var bufferFile = _scratchBuffers[scratchNumber].File;
            return bufferFile.PagerState;
        }

        public PageFromScratchBuffer Allocate(LowLevelTransaction tx, int numberOfPages)
        {
            if (tx == null)
                throw new ArgumentNullException(nameof(tx));
            var size = Bits.NextPowerOf2(numberOfPages);

            var current = _current;

            PageFromScratchBuffer result;
            if (current.File.TryGettingFromAllocatedBuffer(tx, numberOfPages, size, out result))
                return result;

            // we can allocate from the end of the file directly
            if (current.File.LastUsedPage + size <= current.File.NumberOfAllocatedPages)
                return current.File.Allocate(tx, numberOfPages, size);

            if (current.File.Size < _options.MaxScratchBufferSize)
                return current.File.Allocate(tx, numberOfPages, size);

            var minSize = numberOfPages * Constants.Storage.PageSize;
            var requestedSize = Math.Max(minSize, Math.Min(_current.File.Size * 2, _options.MaxScratchBufferSize));
            // We need to ensure that _current stays constant through the codepath until return. 
            current = NextFile(minSize, requestedSize);

            try
            {
                tx.EnsurePagerStateReference(current.File.PagerState);

                return current.File.Allocate(tx, numberOfPages, size);
            }
            finally
            {
                // That's why we update only after exiting. 
                _current = current;
            }
        }

        public void Free(int scratchNumber, long page, long? txId)
        {
            var scratch = _scratchBuffers[scratchNumber];
            scratch.File.Free(page, txId);
            if (scratch.File.AllocatedPagesCount != 0)
                return;

            while (_recycleArea.First != null)
            {
                var recycledScratch = _recycleArea.First.Value;

                if (DateTime.UtcNow - recycledScratch.RecycledAt <= TimeSpan.FromMinutes(1))
                    break;

                _recycleArea.RemoveFirst();

                if (recycledScratch.File.HasActivelyUsedBytes(_env.PossibleOldestReadTransaction))
                {
                    // even though this was in the recycle area, there might still be some transactions looking at it
                    // so we cannot dispose it right now, the disposal will happen in RemoveInactiveScratches 
                    // when we are sure it's really no longer in use
                    continue;
                }

                ScratchBufferItem _;
                _scratchBuffers.TryRemove(recycledScratch.Number, out _);
                recycledScratch.File.Dispose();
            }

            if (scratch == _current)
            {
                if (scratch.File.Size <= _options.MaxScratchBufferSize)
                {
                    // we'll take the chance that no one is using us to reset the memory allocations
                    // and avoid fragmentation, we can only do that if no transaction is looking at us
                    if (scratch.File.HasActivelyUsedBytes(_env.PossibleOldestReadTransaction) == false)
                        scratch.File.Reset();

                    return;
                }

                // this is the current one, but the size is too big, let us trim it
                var newCurrent = NextFile(_options.InitialLogFileSize, _options.MaxScratchBufferSize);
                newCurrent.File.PagerState.AddRef();
                _current = newCurrent;
            }

            TryRecyleScratchFile(scratch);
        }

        private void TryRecyleScratchFile(ScratchBufferItem scratch)
        {
            if (scratch.File.Size != _current.File.Size)
                return;

            scratch.RecycledAt = DateTime.UtcNow;
            _recycleArea.AddLast(scratch);
        }

        public void Dispose()
        {
            if (_pagerStatesAllScratchesCache != null)
            {
                foreach (var pagerState in _pagerStatesAllScratchesCache)
                {
                    pagerState.Value.Release();
                }
            }
            foreach (var scratch in _scratchBuffers)
            {
                scratch.Value.File.Dispose();
            }
            _scratchBuffers.Clear();
        }

        private class ScratchBufferItem
        {
            public readonly int Number;
            public readonly ScratchBufferFile File;

            public ScratchBufferItem(int number, ScratchBufferFile file)
            {
                Number = number;
                File = file;
            }

            public DateTime RecycledAt;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public virtual int CopyPage(I4KbBatchWrites destI4KbBatchWrites, int scratchNumber, long p, PagerState pagerState)
        {
            var item = GetScratchBufferFile(scratchNumber);

            ScratchBufferFile bufferFile = item.File;
            return bufferFile.CopyPage(destI4KbBatchWrites, p, pagerState);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Page ReadPage(LowLevelTransaction tx, int scratchNumber, long p, PagerState pagerState = null, LowLevelTransaction.PagerRef pagerRef = null)
        {
            var item = GetScratchBufferFile(scratchNumber);

            ScratchBufferFile bufferFile = item.File;
            return bufferFile.ReadPage(tx, p, pagerState, pagerRef);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte* AcquirePagePointerForNewPage(LowLevelTransaction tx, int scratchNumber, long p, int numberOfPages)
        {
            var item = GetScratchBufferFile(scratchNumber);

            ScratchBufferFile bufferFile = item.File;
            return bufferFile.AcquirePagePointerForNewPage(tx, p, numberOfPages);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte* AcquirePagePointerWithOverflowHandling(LowLevelTransaction tx, int scratchNumber, long p)
        {
            var item = GetScratchBufferFile(scratchNumber);

            ScratchBufferFile bufferFile = item.File;
            return bufferFile.AcquirePagePointerWithOverflowHandling(tx, p);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ScratchBufferItem GetScratchBufferFile(int scratchNumber)
        {
            var currentScratchFile = _current;
            if (scratchNumber == currentScratchFile.Number)
                return currentScratchFile;
            // if we can avoid the dictionary lookup for the common case of 
            // looking at the latest scratch, that is great
            return _scratchBuffers[scratchNumber];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AddScratchBufferFile(ScratchBufferItem scratch)
        {
            RemoveInactiveScratches(scratch);

            _scratchBuffers.AddOrUpdate(scratch.Number, scratch, (_, __) => scratch);
        }

        private void RemoveInactiveScratches(ScratchBufferItem except)
        {
            foreach (var item in _scratchBuffers)
            {
                var scratchBufferItem = item.Value;

                if (scratchBufferItem.File.HasActivelyUsedBytes(_env.PossibleOldestReadTransaction) ||
                    scratchBufferItem == except)
                    continue;

                ScratchBufferItem _;
                if (_scratchBuffers.TryRemove(scratchBufferItem.Number, out _) == false)
                    ThrowUnableToRemoveScratch(scratchBufferItem);

                if (_recycleArea.Contains(scratchBufferItem) == false)
                    scratchBufferItem.File.Dispose();
            }
        }

        private static void ThrowUnableToRemoveScratch(ScratchBufferItem scratchBufferItem)
        {
            throw new InvalidOperationException(
                $"Could not remove a scratch file from the scratch buffers collection. Number: {scratchBufferItem.Number}");
        }

        public void BreakLargeAllocationToSeparatePages(LowLevelTransaction tx, PageFromScratchBuffer value)
        {
            var item = GetScratchBufferFile(value.ScratchFileNumber);
            item.File.BreakLargeAllocationToSeparatePages(tx, value);
        }

        public long GetAvailablePagesCount()
        {
            return _current.File.NumberOfAllocatedPages - _current.File.AllocatedPagesCount;
        }

        public void Cleanup()
        {
            if (_recycleArea.Count == 0 && _scratchBuffers.Count == 1)
                return;

            long txIdAllowingToReleaseOldScratches = -1;

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var scratchBufferItem in _scratchBuffers)
            {
                if (scratchBufferItem.Value == _current)
                    continue;

                txIdAllowingToReleaseOldScratches = Math.Max(txIdAllowingToReleaseOldScratches,
                    scratchBufferItem.Value.File.TxIdAfterWhichLatestFreePagesBecomeAvailable);
            }
            
            while (_env.CurrentReadTransactionId <= txIdAllowingToReleaseOldScratches)
            {
                // we've just flushed and had no more writes after that, let us bump id of next read transactions to ensure
                // that nobody will attempt to read old scratches so we will be able to release more files

                try
                {
                    using (var tx = _env.NewLowLevelTransaction(new TransactionPersistentContext(),
                            TransactionFlags.ReadWrite, timeout: TimeSpan.FromMilliseconds(500)))
                    {
                        tx.ModifyPage(0);
                        tx.Commit();
                    }
                }
                catch (TimeoutException)
                {
                    break;
                }
            }

            // we need to ensure that no access to _recycleArea and _scratchBuffers will take place in the same time
            // and only methods that access this are used within write transaction
            using (_env.WriteTransaction())
            {
                RemoveInactiveScratches(_current);

                if (_recycleArea.Count == 0)
                    return;

                while (_recycleArea.First != null)
                {
                    var recycledScratch = _recycleArea.First.Value;

                    ScratchBufferItem _;
                    _scratchBuffers.TryRemove(recycledScratch.Number, out _);
                    recycledScratch.File.Dispose();
                    _recycleArea.RemoveFirst();
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void EnsureMapped(LowLevelTransaction tx,int scratchNumber, long positionInScratchBuffer, int numberOfPages)
        {
            var item = GetScratchBufferFile(scratchNumber);

            ScratchBufferFile bufferFile = item.File;
            bufferFile.EnsureMapped(tx, positionInScratchBuffer, numberOfPages);
        }

        public ScratchBufferPoolInfo InfoForDebug(long oldestActiveTransaction)
        {
            var currentFile = _current.File;
            var scratchBufferPoolInfo = new ScratchBufferPoolInfo
            {
                OldestActiveTransaction = oldestActiveTransaction,
                NumberOfScratchFiles = _scratchBuffers.Count,
                CurrentFileSizeInMB = currentFile.Size / 1024L / 1024L,
                PerScratchFileSizeLimitInMB = _options.MaxScratchBufferSize / 1024L / 1024L
            };

            foreach (var scratchBufferItem in _scratchBuffers.Values.OrderBy(x => x.Number))
            {
                var current = _current;
                var scratchFileUsage = new ScratchFileUsage
                {
                    Name = StorageEnvironmentOptions.ScratchBufferName(scratchBufferItem.File.Number),
                    SizeInKB = scratchBufferItem.File.Size / 1024,
                    NumberOfAllocations = scratchBufferItem.File.NumberOfAllocations,
                    CanBeDeleted = scratchBufferItem != current && scratchBufferItem.File.HasActivelyUsedBytes(oldestActiveTransaction) == false,
                    TxIdAfterWhichLatestFreePagesBecomeAvailable = scratchBufferItem.File.TxIdAfterWhichLatestFreePagesBecomeAvailable
                };

                foreach (var freePage in scratchBufferItem.File.GetMostAvailableFreePagesBySize())
                {
                    scratchFileUsage.MostAvailableFreePages.Add(new MostAvailableFreePagesBySize
                    {
                        Size = freePage.Key,
                        ValidAfterTransactionId = freePage.Value
                    });
                }

                scratchBufferPoolInfo.ScratchFilesUsage.Add(scratchFileUsage);
            }

            return scratchBufferPoolInfo;
        }
    }
}