using System;
using System.Collections.Generic;
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

        public override unsafe void MaybePrefetchMemory(Span<long> pagesToPrefetch)
        {
            if (PlatformDetails.CanPrefetch == false)
                return; // not supported

            if (pagesToPrefetch.Length == 0)
                return;

            long sizeToPrefetch = 4 * Constants.Storage.PageSize;

            long size = 0;
            void* baseAddress = null;
            var range = new List<Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY>(pagesToPrefetch.Length);
            for (int i = 0; i < pagesToPrefetch.Length; i++)
            {
                var addressToPrefetch = pagesToPrefetch[i];
                size += sizeToPrefetch;

                if (baseAddress == null)
                    baseAddress = (void*)addressToPrefetch;

                if (i != pagesToPrefetch.Length - 1 && pagesToPrefetch[i + 1] == addressToPrefetch + sizeToPrefetch)
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

        protected PosixAbstractPager(StorageEnvironmentOptions options, bool usePageProtection = false) : base(options, usePageProtection: usePageProtection)
        {
        }
    }
}
