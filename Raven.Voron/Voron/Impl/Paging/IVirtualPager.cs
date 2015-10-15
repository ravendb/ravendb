using System;
using System.Collections.Generic;
using System.ComponentModel;
using Voron.Platform.Win32;
using Voron.Trees;

namespace Voron.Impl.Paging
{
    public unsafe interface IVirtualPager : IDisposable
    {
        PagerState PagerState { get; }
        bool Disposed { get; }
        long NumberOfAllocatedPages { get; }
        int PageMinSpace { get; }
        bool DeleteOnClose { get; set; }
        int PageSize { get; }
        int NodeMaxSize { get; }
        int PageMaxSpace { get; }
        string DebugInfo { get; }
        byte* AcquirePagePointer(long pageNumber, PagerState pagerState = null);
        void Sync();
        PagerState EnsureContinuous(long requestedPageNumber, int numberOfPages);
        int WriteDirect(byte* p, long pagePosition, int pagesToWrite);
    }

    public static unsafe class VirtualPagerExtensions
    {
        public static TreePage Read(this IVirtualPager pager, long pageNumber, PagerState pagerState = null)
        {
            return new TreePage(pager.AcquirePagePointer(pageNumber, pagerState), pager.DebugInfo, pager.PageSize);
        }

        public static bool WillRequireExtension(this IVirtualPager pager, long requestedPageNumber, int numberOfPages)
        {
            return requestedPageNumber + numberOfPages > pager.NumberOfAllocatedPages;
        }

        public static int Write(this IVirtualPager pager, TreePage page, long? pageNumber = null)
        {
            var startPage = pageNumber ?? page.PageNumber;

            var toWrite = page.IsOverflow ? pager.GetNumberOfOverflowPages(page.OverflowSize) : 1;

            return pager.WriteDirect(page.Base, startPage, toWrite);
        }

        public static int GetNumberOfOverflowPages(this IVirtualPager pager, int overflowSize)
        {
            overflowSize += Constants.PageHeaderSize;
            return (overflowSize/pager.PageSize) + (overflowSize%pager.PageSize == 0 ? 0 : 1);
        }
    }

    public static unsafe class VirtualPagerWin32Extensions
    {
        private static bool IsWindows8OrNewer()
        {
            var os = Environment.OSVersion;
            return os.Platform == PlatformID.Win32NT &&
                   (os.Version.Major > 6 || (os.Version.Major == 6 && os.Version.Minor >= 2));
        }

        public static void TryPrefetchingWholeFile(this IVirtualPager pager)
        {
            if (IsWindows8OrNewer() == false)
                return; // not supported

            var pagerState = pager.PagerState;
            var entries =
                stackalloc Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY[pagerState.AllocationInfos.Length];

            for (var i = 0; i < pagerState.AllocationInfos.Length; i++)
            {
                entries[i].VirtualAddress = pagerState.AllocationInfos[i].BaseAddress;
                entries[i].NumberOfBytes = (IntPtr) pagerState.AllocationInfos[i].Size;
            }


            if (Win32MemoryMapNativeMethods.PrefetchVirtualMemory(Win32NativeMethods.GetCurrentProcess(),
                (UIntPtr) pagerState.AllocationInfos.Length, entries, 0) == false)
                throw new Win32Exception();
        }

        public static void MaybePrefetchMemory(this IVirtualPager pager, List<TreePage> sortedPages)
        {
            if (sortedPages.Count == 0)
                return;

            if (IsWindows8OrNewer() == false)
                return; // not supported

            var list = new List<Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY>();

            long lastPage = -1;
            const int numberOfPagesInBatch = 8;
            var sizeInPages = numberOfPagesInBatch; // OS uses 32K when you touch a page, let us reuse this
            foreach (var page in sortedPages)
            {
                if (lastPage == -1)
                {
                    lastPage = page.PageNumber;
                }

                var numberOfPagesInLastPage = page.IsOverflow == false
                    ? 1
                    : pager.GetNumberOfOverflowPages(page.OverflowSize);

                var endPage = page.PageNumber + numberOfPagesInLastPage - 1;

                if (endPage <= lastPage + sizeInPages)
                    continue; // already within the allocation granularity we have

                if (page.PageNumber <= lastPage + sizeInPages + numberOfPagesInBatch)
                {
                    while (endPage > lastPage + sizeInPages)
                    {
                        sizeInPages += numberOfPagesInBatch;
                    }

                    continue;
                }

                list.Add(new Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY
                {
                    NumberOfBytes = (IntPtr) (sizeInPages*pager.PageSize),
                    VirtualAddress = pager.AcquirePagePointer(lastPage)
                });
                lastPage = page.PageNumber;
                sizeInPages = numberOfPagesInBatch;
                while (endPage > lastPage + sizeInPages)
                {
                    sizeInPages += numberOfPagesInBatch;
                }
            }
            list.Add(new Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY
            {
                NumberOfBytes = (IntPtr) (sizeInPages*pager.PageSize),
                VirtualAddress = pager.AcquirePagePointer(lastPage)
            });

            fixed (Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY* entries = list.ToArray())
            {
                Win32MemoryMapNativeMethods.PrefetchVirtualMemory(Win32NativeMethods.GetCurrentProcess(),
                    (UIntPtr) list.Count,
                    entries, 0);
            }
        }
    }
}