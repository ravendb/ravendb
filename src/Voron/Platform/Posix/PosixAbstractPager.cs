using System;
using System.Collections.Generic;
using Voron.Data.BTrees;
using Voron.Impl.Paging;
using Voron.Platform.Win32;

namespace Voron.Platform.Posix
{
    public abstract class PosixAbstractPager : AbstractPager
    {

        private unsafe void PrefetchRanges(List<Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY> ranges)
        {
            foreach (var range in ranges)
            {
                if (Syscall.madvise(new IntPtr(range.VirtualAddress), (UIntPtr)range.NumberOfBytes.ToPointer(), MAdvFlags.MADV_WILLNEED) == -1)
                {
                    // ignore this error. TODO : Log ?
                }
            }
        }

        public override unsafe void TryPrefetchingWholeFile()
        {
            if (Sparrow.Platform.CanPrefetch == false)
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

        public override void MaybePrefetchMemory(List<TreePage> sortedPages)
        {
            if (Sparrow.Platform.CanPrefetch == false)
                return; // not supported

            if (sortedPages.Count == 0)
                return;

            var ranges = SortedPagesToList(sortedPages);
            PrefetchRanges(ranges);
        }

        public override unsafe void MaybePrefetchMemory(List<long> pagesToPrefetch)
        {
            if (Sparrow.Platform.CanPrefetch == false)
                return; // not supported

            if (pagesToPrefetch.Count == 0)
                return;

            long sizeToPrefetch = 4 * PageSize;

            long size = 0;
            void* baseAddress = null;
            var range = new List<Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY>(pagesToPrefetch.Count);
            for (int i = 0; i < pagesToPrefetch.Count; i++)
            {
                var addressToPrefetch = pagesToPrefetch[i];
                size += sizeToPrefetch;

                if (baseAddress == null)
                    baseAddress = (void*)addressToPrefetch;

                if (i != pagesToPrefetch.Count - 1 &&
                    pagesToPrefetch[i + 1] == addressToPrefetch + sizeToPrefetch)
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

        public PosixAbstractPager(StorageEnvironmentOptions options) : base(options)
        {
        }
    }
}