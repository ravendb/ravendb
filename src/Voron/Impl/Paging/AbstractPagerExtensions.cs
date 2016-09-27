using System.IO;
using Sparrow;
using Voron.Global;
using Voron.Data.BTrees;

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

       public static long Write(this AbstractPager pager, TreePage page, long? pageNumber = null)
        {
            var startPage = pageNumber ?? page.PageNumber;

            var toWrite = page.IsOverflow ? pager.GetNumberOfOverflowPages(page.OverflowSize) : 1;

            return pager.WriteDirect(page.Base, startPage, toWrite);
        }



        public static long WritePage(this AbstractPager pager, Page page, long? pageNumber = null)
        {
            var startPage = pageNumber ?? page.PageNumber;

            var toWrite = pager.GetNumberOfPages(page);
            return pager.WriteDirect(page.Pointer, startPage, toWrite);
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