using System.Runtime.CompilerServices;
using Voron.Data;
using Voron.Global;

namespace Voron.Impl.Paging
{
    public static unsafe class VirtualPagerLegacyExtensions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte* AcquirePagePointerWithOverflowHandling<T>(this AbstractPager pager, T tx, long pageNumber, PagerState pagerState) where T : IPagerLevelTransactionState
        {
            // Case 1: Page is not overflow ==> no problem, returning a pointer to existing mapping
            var pageHeader = (PageHeader*)pager.AcquirePagePointer(tx, pageNumber, pagerState);
            if ((pageHeader->Flags & PageFlags.Overflow) != PageFlags.Overflow)
                return (byte*)pageHeader;

            // Case 2: Page is overflow and already mapped large enough ==> no problem, returning a pointer to existing mapping
            if (pager.EnsureMapped(tx, pageNumber, GetNumberOfOverflowPages(pageHeader->OverflowSize)) == false)
                return (byte*)pageHeader;

            // Case 3: Page is overflow and was ensuredMapped above, view was re-mapped so we need to acquire a pointer to the new mapping.
            return pager.AcquirePagePointer(tx, pageNumber, pagerState);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte* AcquirePagePointerWithOverflowHandling<T>(this AbstractPager pager, T tx, long pageNumber) where T : IPagerLevelTransactionState
        {
            // Case 1: Page is not overflow ==> no problem, returning a pointer to existing mapping
            var pageHeader = (PageHeader*)pager.AcquirePagePointer(tx, pageNumber);
            if ((pageHeader->Flags & PageFlags.Overflow) != PageFlags.Overflow)
                return (byte*)pageHeader;

            // Case 2: Page is overflow and already mapped large enough ==> no problem, returning a pointer to existing mapping
            if (pager.EnsureMapped(tx, pageNumber, GetNumberOfOverflowPages(pageHeader->OverflowSize)) == false)
                return (byte*)pageHeader;

            // Case 3: Page is overflow and was ensuredMapped above, view was re-mapped so we need to acquire a pointer to the new mapping.
            return pager.AcquirePagePointer(tx, pageNumber);
        }

        public static bool WillRequireExtension(this AbstractPager pager, long requestedPageNumber, int numberOfPages)
        {
            return requestedPageNumber + numberOfPages > pager.NumberOfAllocatedPages;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetNumberOfOverflowPages(long overflowSize)
        {
            overflowSize += Constants.Tree.PageHeaderSize;
            return (int)(overflowSize / Constants.Storage.PageSize) + (overflowSize % Constants.Storage.PageSize == 0 ? 0 : 1);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetNumberOfPages(PageHeader* header)
        {
            if ((header->Flags & PageFlags.Overflow) != PageFlags.Overflow)
                return 1;

            var overflowSize = header->OverflowSize + Constants.Tree.PageHeaderSize;
            return checked((overflowSize / Constants.Storage.PageSize) + (overflowSize % Constants.Storage.PageSize == 0 ? 0 : 1));
        }
    }
}
