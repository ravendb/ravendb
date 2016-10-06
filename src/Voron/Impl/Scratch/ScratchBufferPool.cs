using Sparrow;
using Sparrow.Binary;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using Voron.Exceptions;
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
        // Immutable state. 
        private readonly StorageEnvironmentOptions _options;

        // Local per scratch file potentially read delayed inconsistent (need guards). All must be modified atomically (but it wont necessarily require a memory barrier)
        private ScratchBufferItem _current;

        // Local writable state. Can perform multiple reads, but must never do multiple writes simultaneously.
        private int _currentScratchNumber = -1;

        private readonly ConcurrentDictionary<int, ScratchBufferItem> _scratchBuffers =
            new ConcurrentDictionary<int, ScratchBufferItem>(NumericEqualityComparer.Instance);

        private int _numberOfPagesForScratchFile;


        public ScratchBufferPool(StorageEnvironment env)
        {
            _options = env.Options;
            _numberOfPagesForScratchFile = (int)(Math.Max(_options.InitialFileSize ?? 0, _options.InitialLogFileSize) / _options.PageSize);
            _current = NextFile();
        }

        public Dictionary<int, PagerState> GetPagerStatesOfAllScratches()
        {
            // This is not risky anymore, but the caller must understand this is a monotonically incrementing snapshot. 
            return _scratchBuffers.ToDictionary(x => x.Key, y => y.Value.File.PagerState,
                NumericEqualityComparer.Instance);
        }

        internal long GetNumberOfAllocations(int scratchNumber)
        {
            // While used only in tests, there is no multithread risk. 
            return _scratchBuffers[scratchNumber].File.NumberOfAllocations;
        }

        private ScratchBufferItem NextFile()
        {
            _currentScratchNumber++;
            AbstractPager scratchPager;
            try
            {
                scratchPager =
                    _options.CreateScratchPager(StorageEnvironmentOptions.ScratchBufferName(_currentScratchNumber),
                        _numberOfPagesForScratchFile * _options.PageSize);
            }
            catch (Exception)
            {
                // this can fail because of disk space issue, let us just ignore it
                // we'll allocate the minimum amount in a bit anway
                scratchPager =
                    _options.CreateScratchPager(StorageEnvironmentOptions.ScratchBufferName(_currentScratchNumber),
                        _options.InitialFileSize ?? _options.InitialLogFileSize
                    );
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

            // We run out of scratch space, increasing the size of the file will require to allocate
            // all the memory again, so we'll just double the size (until 1 GB, at which point we'll 
            // grow by 1 GB each time)

            CalculateNextSize(numberOfPages);

            // We need to ensure that _current stays constant through the codepath until return. 
            current = NextFile();
            tx.EnsurePagerStateReference(current.File.PagerState);

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

        private void CalculateNextSize(int numberOfPages)
        {
            var oldPages = _numberOfPagesForScratchFile;
            var pagesInGb = Constants.Size.Gigabyte/_options.PageSize;
            _numberOfPagesForScratchFile = Math.Max(Bits.NextPowerOf2(numberOfPages), _numberOfPagesForScratchFile*2);

            if (oldPages + pagesInGb < _numberOfPagesForScratchFile ||
                _numberOfPagesForScratchFile < 0)
            {
                _numberOfPagesForScratchFile = Math.Max(oldPages + pagesInGb, numberOfPages);
            }
        }

        public void Free(int scratchNumber, long page, long asOfTxId)
        {
            var scratch = _scratchBuffers[scratchNumber];
            scratch.File.Free(page, asOfTxId);
            if (scratch != _current && scratch.File.ActivelyUsedBytes == 0)
            {
                ScratchBufferItem _;
                _scratchBuffers.TryRemove(scratchNumber, out _);
                scratch.File.Dispose();
            }
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
    }
}