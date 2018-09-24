using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow.Platform;
using Sparrow.Platform.Posix;
using Voron.Data.BTrees;
using Voron.Global;
using Voron.Impl;
using Voron.Impl.Paging;
using Voron.Platform.Win32;

namespace Voron.Platform.Posix
{
    public abstract class PosixAbstractPager : AbstractPager
    {
        private const byte EvenPrefetchCountMask = 0x70;
        private const byte EvenPrefetchMaskShift = 4;
        private const byte OddPrefetchCountMask = 0x07;
        private const byte AlreadyPrefetch = 7;

        private readonly int _prefetchSegmentSize;
        private readonly int _prefetchResetThreshold;

        private readonly int _segmentShift;
        private int _refreshCounter = 0;
        private byte[] _prefetchTable;

        public override int CopyPage(I4KbBatchWrites destwI4KbBatchWrites, long p, PagerState pagerState)
        {
            return CopyPageImpl(destwI4KbBatchWrites, p, pagerState);
        }

        private unsafe void PrefetchRanges(Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY* list, int count)
        {
            for (int i = 0; i < count; i++)
                Syscall.madvise(new IntPtr(list[i].VirtualAddress), (UIntPtr)list[i].NumberOfBytes.ToPointer(), MAdvFlags.MADV_WILLNEED);
        }

        private unsafe void PrefetchRanges(List<Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY> ranges)
        {
            foreach (var range in ranges)
            {
                if (Syscall.madvise(new IntPtr(range.VirtualAddress), (UIntPtr)range.NumberOfBytes.ToPointer(), MAdvFlags.MADV_WILLNEED) == -1)
                {
                    // ignore this error.
                }
            }
        }        

        public override unsafe void TryPrefetchingWholeFile()
        {
            if (PlatformDetails.CanPrefetch == false)
                return; // not supported

            long size = 0;
            void* baseAddress = null;
            var pagerState = PagerState;
            var range = new List<Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY>(pagerState.AllocationInfos.Length);
            for (int i = 0; i < pagerState.AllocationInfos.Length; i++)
            {
                var allocInfo = pagerState.AllocationInfos[i];
                size += allocInfo.Size;

                if (baseAddress == null)
                    baseAddress = allocInfo.BaseAddress;

                if (i != pagerState.AllocationInfos.Length - 1 &&
                    pagerState.AllocationInfos[i + 1].BaseAddress == allocInfo.BaseAddress + allocInfo.Size)
                {
                    continue; // if adjacent ranges make one syscall
                }

                range.Add(new Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY
                {
                    VirtualAddress = baseAddress,
                    NumberOfBytes = new IntPtr(size)
                });

                size = 0;
                baseAddress = null;
            }
            PrefetchRanges(range);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int GetSegmentState(long segment)
        {
            if (segment < 0)
                return AlreadyPrefetch;

            byte value = this._prefetchTable[segment / 2];
            if (segment % 2 == 0)
            {
                // The actual value is in the high byte.
                value = (byte)(value >> EvenPrefetchMaskShift);
            }
            else
            {
                value = (byte)(value & OddPrefetchCountMask);
            }

            return value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SetSegmentState(long segment, int state)
        {
            byte value = this._prefetchTable[segment / 2];
            if (segment % 2 == 0)
            {
                // The actual value is in the high byte.
                value = (byte)((value & OddPrefetchCountMask) | (state << EvenPrefetchMaskShift));
            }
            else
            {
                value = (byte)((value & EvenPrefetchCountMask) | state);
            }

            this._prefetchTable[segment / 2] = value;
        }

        private void ResetPrefetchTable()
        {
            this._refreshCounter = 0;

            // We will zero out the whole table to reset the prefetching behavior. 
            Array.Clear(this._prefetchTable, 0, this._prefetchTable.Length);
        }

        protected void InitializePrefetchTable(long totalAllocationSize)
        {
            long numberOfAllocatedSegments = (totalAllocationSize / _prefetchSegmentSize) + 1;
            long expectedTableSize = (numberOfAllocatedSegments / 2) + 1;

            if ( this._prefetchTable == null || numberOfAllocatedSegments != expectedTableSize)
                this._prefetchTable = new byte[expectedTableSize];
        }

        public override unsafe void MaybePrefetchMemory<T>(T pagesToPrefetch)
        {
            if (PlatformDetails.CanPrefetch == false)
                return; // not supported


            if (pagesToPrefetch.MoveNext() == false)
                return;

            // PERF: We dont acquire pointer here to avoid all the overhead of doing so; instead we calculate the proper place based on 
            //       base address from the pager.
            byte* baseAddress = this._pagerState.MapBase;

            const int StackSpace = 16;

            int prefetchIdx = 0;
            Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY* toPrefetch = stackalloc Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY[StackSpace];

            do
            {
                long pageNumber = pagesToPrefetch.Current;

                long segmentNumber = (pageNumber * Constants.Storage.PageSize) >> this._segmentShift;

                int segmentState = GetSegmentState(segmentNumber);
                if (segmentState < AlreadyPrefetch)
                {
                    // We update the current segment counter
                    segmentState++;

                    int previousSegmentState = GetSegmentState(segmentNumber - 1);
                    if (previousSegmentState == AlreadyPrefetch)
                    {
                        segmentState = AlreadyPrefetch;
                    }

                    SetSegmentState(segmentNumber, segmentState);

                    if (segmentState == AlreadyPrefetch)
                    {
                        // Prepare the segment information. 
                        toPrefetch[prefetchIdx].NumberOfBytes = (IntPtr)_prefetchSegmentSize;
                        toPrefetch[prefetchIdx].VirtualAddress = baseAddress + segmentNumber * _prefetchSegmentSize;
                        prefetchIdx++;
                        _refreshCounter++;

                        if (prefetchIdx >= StackSpace)
                        {
                            // We dont have enough space, so we send the batch to the kernel
                            PrefetchRanges(toPrefetch, StackSpace);
                            prefetchIdx = 0;
                        }
                    }
                }
            }
            while (pagesToPrefetch.MoveNext());

            if (prefetchIdx != 0)
            {
                // We dont have enough space, so we send the batch to the kernel       
                PrefetchRanges(toPrefetch, prefetchIdx);
            }

            if (_refreshCounter > _prefetchResetThreshold)
                ResetPrefetchTable();
        }

        public override unsafe byte* AcquirePagePointer(IPagerLevelTransactionState tx, long pageNumber, PagerState pagerState = null)
        {
            // We need to decide what pager we are going to use right now or risk inconsistencies when performing prefetches from disk.
            var state = pagerState ?? _pagerState;

            if (PlatformDetails.CanPrefetch)
            {
                long segmentNumber = (pageNumber * Constants.Storage.PageSize) >> this._segmentShift;

                int segmentState = GetSegmentState(segmentNumber);
                if (segmentState < AlreadyPrefetch)
                {
                    // We update the current segment counter
                    segmentState++;

                    int previousSegmentState = GetSegmentState(segmentNumber - 1);
                    if (previousSegmentState == AlreadyPrefetch)
                    {
                        segmentState = AlreadyPrefetch;
                    }

                    SetSegmentState(segmentNumber, segmentState);

                    if (segmentState == AlreadyPrefetch)
                    {
                        Syscall.madvise(new IntPtr(state.MapBase), (UIntPtr)_prefetchSegmentSize, MAdvFlags.MADV_WILLNEED);
                        _refreshCounter++;

                        if (_refreshCounter > _prefetchResetThreshold)
                            ResetPrefetchTable();
                    }
                }
            }

            return base.AcquirePagePointer(tx, pageNumber, state);
        }

        protected PosixAbstractPager(StorageEnvironmentOptions options, bool usePageProtection = false) : base(options, usePageProtection: usePageProtection)
        {
            this._prefetchSegmentSize = 1 << this._segmentShift;
            this._prefetchResetThreshold = (int)((float)options.PrefetchResetThreshold / this._prefetchSegmentSize);

            Debug.Assert((_prefetchSegmentSize - 1) >> this._segmentShift == 0);
            Debug.Assert(_prefetchSegmentSize >> this._segmentShift == 1);
        }
    }
}
