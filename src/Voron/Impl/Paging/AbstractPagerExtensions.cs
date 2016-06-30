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
}