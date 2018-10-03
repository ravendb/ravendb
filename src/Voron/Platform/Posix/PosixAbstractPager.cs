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

        public override unsafe void MaybePrefetchMemory<T>(T pagesToPrefetch)
        {
            if (PlatformDetails.CanPrefetch == false)
                return; // not supported

            if (pagesToPrefetch.MoveNext() == false)
                return;

            const int StackSpace = 16;

            int prefetchIdx = 0;
            Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY* toPrefetch = stackalloc Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY[StackSpace];

            do
            {
                long pageNumber = pagesToPrefetch.Current;
                if (this._pagerState.ShouldPrefetchSegment(pageNumber, out void* virtualAddress, out long bytes))
                {
                    // Prepare the segment information. 
                    toPrefetch[prefetchIdx].NumberOfBytes = (IntPtr)bytes; // _prefetchSegmentSize;
                    toPrefetch[prefetchIdx].VirtualAddress = virtualAddress; // baseAddress + segmentNumber * _prefetchSegmentSize;
                    prefetchIdx++;

                    if (prefetchIdx >= StackSpace)
                    {
                        // We dont have enough space, so we send the batch to the kernel
                        PrefetchRanges(toPrefetch, StackSpace);
                        prefetchIdx = 0;
                    }
                }
            }
            while (pagesToPrefetch.MoveNext());

            if (prefetchIdx != 0)
            {
                PrefetchRanges(toPrefetch, prefetchIdx);
            }

            this._pagerState.CheckResetPrefetchTable();
        }

        public override unsafe byte* AcquirePagePointer(IPagerLevelTransactionState tx, long pageNumber, PagerState pagerState = null)
        {
            // We need to decide what pager we are going to use right now or risk inconsistencies when performing prefetches from disk.
            var state = pagerState ?? _pagerState;

            if (PlatformDetails.CanPrefetch)
            {
                if (this._pagerState.ShouldPrefetchSegment(pageNumber, out void* virtualAddress, out long bytes))
                {
                    Syscall.madvise(new IntPtr(virtualAddress), (UIntPtr)bytes, MAdvFlags.MADV_WILLNEED);
                }
            }

            return base.AcquirePagePointer(tx, pageNumber, state);
        }

        protected PosixAbstractPager(StorageEnvironmentOptions options, bool usePageProtection = false) : base(options, usePageProtection: usePageProtection)
        {
        }
    }
}
