using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Runtime.InteropServices;
using Voron.Data.BTrees;
using Voron.Platform.Win32;

namespace Voron.Impl.Paging
{
    public static unsafe class VirtualPagerLegacyExtensions
    {
        public static Page ReadPage(this AbstractPager pager, LowLevelTransaction tx, long pageNumber, PagerState pagerState = null)
        {
            return new Page(pager.AcquirePagePointer(tx, pageNumber, pagerState), pager);
        }

        public static TreePage Read(this AbstractPager pager, LowLevelTransaction tx, long pageNumber, PagerState pagerState = null)
        {
            return new TreePage(pager.AcquirePagePointer(tx, pageNumber, pagerState), pager.DebugInfo, pager.PageSize);
        }

        public static bool WillRequireExtension(this AbstractPager pager, long requestedPageNumber, int numberOfPages)
        {
            return requestedPageNumber + numberOfPages > pager.NumberOfAllocatedPages;
        }

        public static int Write(this AbstractPager pager, TreePage page, long? pageNumber = null)
        {
            var startPage = pageNumber ?? page.PageNumber;

            var toWrite = page.IsOverflow ? pager.GetNumberOfOverflowPages(page.OverflowSize) : 1;

            return pager.WriteDirect(page.Base, startPage, toWrite);
        }



        public static int WritePage(this AbstractPager pager, Page page, long? pageNumber = null)
        {
            var startPage = pageNumber ?? page.PageNumber;

            var toWrite = page.IsOverflow ? pager.GetNumberOfOverflowPages(page.OverflowSize) : 1;

            return pager.WriteDirect(page.Pointer, startPage, toWrite);
        }
        public static int GetNumberOfOverflowPages(this AbstractPager pager, int overflowSize)
        {
            overflowSize += Constants.TreePageHeaderSize;
            return (overflowSize/pager.PageSize) + (overflowSize%pager.PageSize == 0 ? 0 : 1);
        }
    }

    public static unsafe class VirtualPagerWin32Extensions
    {
        public static void TryPrefetchingWholeFile(this AbstractPager pager)
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows) == false)
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

        public static void MaybePrefetchMemory(this AbstractPager pager, List<TreePage> sortedPages)
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows) == false)
                return; // not supported

            if (sortedPages.Count == 0)
                return;

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
                    VirtualAddress = pager.AcquirePagePointer(null, lastPage)
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
                VirtualAddress = pager.AcquirePagePointer(null, lastPage)
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