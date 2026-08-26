using System.Runtime.CompilerServices;
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

            pager.ThrowIfOverflowExtentExceedsAllocatedPages(pageNumber, pageHeader);

            var numberOfOverflowPages = GetNumberOfOverflowPages(pageHeader->OverflowSize);

            // Case 2: Page is overflow and already mapped large enough ==> no problem, returning a pointer to existing mapping
            if (pager.EnsureMapped(tx, pageNumber, numberOfOverflowPages) == false)
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

            pager.ThrowIfOverflowExtentExceedsAllocatedPages(pageNumber, pageHeader);

            var numberOfOverflowPages = GetNumberOfOverflowPages(pageHeader->OverflowSize);

            // Case 2: Page is overflow and already mapped large enough ==> no problem, returning a pointer to existing mapping
            if (pager.EnsureMapped(tx, pageNumber, numberOfOverflowPages) == false)
                return (byte*)pageHeader;

            // Case 3: Page is overflow and was ensuredMapped above, view was re-mapped so we need to acquire a pointer to the new mapping.
            return pager.AcquirePagePointer(tx, pageNumber);
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

            long overflowSize = (long)header->OverflowSize + Constants.Tree.PageHeaderSize;
            return (int)(overflowSize / Constants.Storage.PageSize) + (overflowSize % Constants.Storage.PageSize == 0 ? 0 : 1);
        }
    }
}
