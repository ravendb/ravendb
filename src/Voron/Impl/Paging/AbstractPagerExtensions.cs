using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Sparrow;
using Voron.Global;
using Voron.Data.BTrees;
using Voron.Platform.Win32;

namespace Voron.Impl.Paging
{
    public static unsafe class VirtualPagerLegacyExtensions
    {
        public static Page ReadPage(this AbstractPager pager, IPagerLevelTransactionState tx, long pageNumber, PagerState pagerState = null)
        {
            return new Page(pager.AcquirePagePointer(tx, pageNumber, pagerState));
        }

        public static TreePage Read(this AbstractPager pager, IPagerLevelTransactionState tx, long pageNumber, PagerState pagerState = null)
        {
            return new TreePage(pager.AcquirePagePointer(tx, pageNumber, pagerState), pager.PageSize);
        }

        public static bool WillRequireExtension(this AbstractPager pager, long requestedPageNumber, int numberOfPages)
        {
            return requestedPageNumber + numberOfPages > pager.NumberOfAllocatedPages;
        }

        public static long Write(this AbstractPager pager, List<Page> pages)
        {
            var pagerState = pager.GetPagerStateAndAddRefAtomically();
            try
            {
                long total = 0;
                var writer = pager.BatchWrites;
                foreach (var page in pages)
                {
                    var numberOfPages = pager.GetNumberOfPages(page);

                    writer.Write(page.PageNumber, numberOfPages, page.Pointer, pagerState);
                    total += numberOfPages*pager.PageSize;
                }
                writer.Flush();
                return total;
            }
            finally
            {
                pagerState.Release();
            }
        }



        public static int GetNumberOfPages(this AbstractPager pager, Page page)
        {
            return page.IsOverflow ? pager.GetNumberOfOverflowPages(page.OverflowSize) : 1;
        }

        public static int GetNumberOfOverflowPages(this AbstractPager pager, long overflowSize)
        {
            overflowSize += Constants.TreePageHeaderSize;
            return checked((int)(overflowSize / pager.PageSize) + (overflowSize % pager.PageSize == 0 ? 0 : 1));
        }
    }
}