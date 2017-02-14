using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Sparrow;
using Voron.Data;
using Voron.Global;
using Voron.Data.BTrees;
using Voron.Platform.Win32;

namespace Voron.Impl.Paging
{
    public static unsafe class VirtualPagerLegacyExtensions
    {
        public static Page ReadPage(this AbstractPager pager, IPagerLevelTransactionState tx, long pageNumber, PagerState pagerState = null)
        {
            return new Page(AcquirePagePointerWithOverflowHandling(pager, tx, pageNumber, pagerState));
        }

        public static TreePage Read(this AbstractPager pager, IPagerLevelTransactionState tx, long pageNumber, PagerState pagerState = null)
        {
            return new TreePage(AcquirePagePointerWithOverflowHandling(pager, tx, pageNumber, pagerState), Constants.Storage.PageSize);
        }
        
        public static byte* AcquirePagePointerWithOverflowHandling(this AbstractPager pager, IPagerLevelTransactionState tx, long pageNumber, PagerState pagerState = null)
        {
            // Case 1: Page is not overflow ==> no problem, returning a pointer to existing mapping
            var pageHeader = (PageHeader*)pager.AcquirePagePointer(tx, pageNumber, pagerState);
            if ((pageHeader->Flags & PageFlags.Overflow) != PageFlags.Overflow)
                return (byte*)pageHeader;

            // Case 2: Page is overflow and already mapped large enough ==> no problem, returning a pointer to existing mapping
            if (pager.EnsureMapped(tx, pageNumber, pager.GetNumberOfOverflowPages(pageHeader->OverflowSize)) == false)
                return (byte*) pageHeader;

            // Case 3: Page is overflow and was ensuredMapped above, view was re-mapped so we need to acquire a pointer to the new mapping.
            return pager.AcquirePagePointer(tx, pageNumber, pagerState);
        }

        public static bool WillRequireExtension(this AbstractPager pager, long requestedPageNumber, int numberOfPages)
        {
            return requestedPageNumber + numberOfPages > pager.NumberOfAllocatedPages;
        }

        public static int GetNumberOfPages(this AbstractPager pager, Page page)
        {
            return page.IsOverflow ? pager.GetNumberOfOverflowPages(page.OverflowSize) : 1;
        }

        public static int GetNumberOfOverflowPages(this AbstractPager pager, long overflowSize)
        {
            overflowSize += Constants.Tree.PageHeaderSize;
            return checked((int)(overflowSize / Constants.Storage.PageSize) + (overflowSize % Constants.Storage.PageSize == 0 ? 0 : 1));
        }

        public static int GetNumberOfOverflowPages(long overflowSize)
        {
            overflowSize += Constants.Tree.PageHeaderSize;
            return checked((int)(overflowSize / Constants.Storage.PageSize) + (overflowSize % Constants.Storage.PageSize == 0 ? 0 : 1));
        }
    }
}