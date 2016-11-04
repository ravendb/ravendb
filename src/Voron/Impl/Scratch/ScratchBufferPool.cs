using Sparrow;
using Sparrow.Binary;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
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

        private readonly LinkedList<Tuple<DateTime, ScratchBufferItem>> _recycleArea = new LinkedList<Tuple<DateTime, ScratchBufferItem>>();

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
            while(current != null)
            {
                var recycled = current.Value.Item2;
                
                if (recycled.File.Size <= Math.Max(minSize, requestedSize ?? 0) && 
                    // even though this is in the recyle bin, there might still be some transactions looking at it
                    // so we have to make sure that this is realy unused before actually reusing it
                    recycled.File.HasActivelyUsedBytes(_env.ActiveTransactions.OldestTransaction) == false)
                {
                    recycled.File.Reset();
                    _recycleArea.Remove(current);
                    _scratchBuffers.TryAdd(recycled.Number, recycled);
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
                    // we'll allocate the minimum amount in a bit anway
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

            _scratchBuffers.TryAdd(item.Number, item);

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

            var minSize = numberOfPages * _options.PageSize;
            var requestedSize = Math.Max(minSize, Math.Min(_current.File.Size * 2, _options.MaxScratchBufferSize));
            // We need to ensure that _current stays constant through the codepath until return. 
            current = NextFile(minSize, requestedSize);

            try
            {
                current.File.PagerState.AddRef();
                tx.EnsurePagerStateReference(current.File.PagerState);

                return current.File.Allocate(tx, numberOfPages, size);
            }
            finally
            {
                // That's why we update only after exiting. 
                _current = current;
            }
        }

        public void Free(int scratchNumber, long page, long asOfTxId)
        {
            var scratch = _scratchBuffers[scratchNumber];
            scratch.File.Free(page, asOfTxId);
            if (scratch.File.CurrentlyAllocatedBytes != 0)
                return;

            while (_recycleArea.First != null)
            {
                if (DateTime.UtcNow - _recycleArea.First.Value.Item1 <= TimeSpan.FromMinutes(1))
                {
                    break;
                }

                _recycleArea.First.Value.Item2.File.Dispose();
                _recycleArea.RemoveFirst();
            }

            if (scratch == _current)
            {
                if (scratch.File.Size <= _options.MaxScratchBufferSize)
                {
                    // called by tx commit
                    if (asOfTxId == -1)
                        return;

                    // we'll take the chance that no one is using us to reset the memory allocations
                    // and avoid fragmentation, we can only do that if no transaction is looking at us
                    if(scratch.File.HasActivelyUsedBytes(asOfTxId) == false)
                        scratch.File.Reset();

                    return;
                }

                // this is the current one, but the size is too big, let us trim it
                var newCurrent = NextFile(_options.InitialLogFileSize, _options.MaxScratchBufferSize);
                newCurrent.File.PagerState.AddRef();
                _current = newCurrent;
            }

            RecyleScratchFile(scratch);
        }

        private void RecyleScratchFile(ScratchBufferItem scratch)
        {
            ScratchBufferItem _;
            if (_scratchBuffers.TryRemove(scratch.Number, out _) == false)
            {
                scratch.File.Dispose();
                return;
            }

            if (scratch.File.Size == _current.File.Size)
            {
                _recycleArea.AddLast(Tuple.Create(DateTime.UtcNow, scratch));
                return;
            }
            scratch.File.Dispose();
        }

        public void Dispose()
        {
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
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Page ReadPage(LowLevelTransaction tx, int scratchNumber, long p, PagerState pagerState = null)
        {
            var item = _scratchBuffers[scratchNumber];

            ScratchBufferFile bufferFile = item.File;
            return bufferFile.ReadPage(tx, p, pagerState);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte* AcquirePagePointer(LowLevelTransaction tx, int scratchNumber, long p)
        {
            var item = _scratchBuffers[scratchNumber];

            ScratchBufferFile bufferFile = item.File;
            return bufferFile.AcquirePagePointer(tx, p);
        }

        public void BreakLargeAllocationToSeparatePages(PageFromScratchBuffer value)
        {
            var item = _scratchBuffers[value.ScratchFileNumber];
            item.File.BreakLargeAllocationToSeparatePages(value);
        }

        public long GetAvailablePagesCount()
        {
            return _current.File.NumberOfAllocatedPages - _current.File.AllocatedPagesUsedSize;
        }

        public void Cleanup()
        {
        }
    }
}