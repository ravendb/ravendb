using System;
using System.Collections.Generic;
using Voron.Trees;

namespace Voron.Impl.Paging
{
    public unsafe interface IVirtualPager : IDisposable
    {
        PagerState PagerState { get; }

        byte* AcquirePagePointer(Transaction tx, long pageNumber, PagerState pagerState = null);
        Page Read(Transaction tx, long pageNumber, PagerState pagerState = null);
        void AllocateMorePages(Transaction tx, long newLength);
    
        bool Disposed { get; }

        long NumberOfAllocatedPages { get; }
        int PageMinSpace { get; }
        bool DeleteOnClose { get; set; }

        void Sync();

        bool ShouldGoToOverflowPage(int len);

        int GetNumberOfOverflowPages(int overflowSize);
        bool WillRequireExtension(long requestedPageNumber, int numberOfPages);
        void EnsureContinuous(Transaction tx, long requestedPageNumber, int numberOfPages);
        int Write(Page page, long? pageNumber = null);

        int WriteDirect(Page start, long pagePosition, int pagesToWrite);

        void TryPrefetchingWholeFile();
        void MaybePrefetchMemory(List<Page> sortedPages);
        void MaybePrefetchMemory(List<long> pagesToPrefetch);
    }
}
