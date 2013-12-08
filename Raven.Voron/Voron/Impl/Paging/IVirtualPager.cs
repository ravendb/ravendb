using System;
using Voron.Trees;

namespace Voron.Impl
{
    public unsafe interface IVirtualPager : IDisposable
    {
		PagerState PagerState { get; }

		byte* AcquirePagePointer(long pageNumber);
        Page Read(long pageNumber);
		void AllocateMorePages(Transaction tx, long newLength);
	
		bool Disposed { get; }

		long NumberOfAllocatedPages { get; }
		int MaxNodeSize { get; }
		int PageMinSpace { get; }
	    bool DeleteOnClose { get; set; }

	    void Sync();

		PagerState TransactionBegan();

		bool ShouldGoToOverflowPage(int len);

		int GetNumberOfOverflowPages(int overflowSize);
	    bool WillRequireExtension(long requestedPageNumber, int numberOfPages);
        void EnsureContinuous(Transaction tx, long requestedPageNumber, int numberOfPages);
        void Write(Page page, long? pageNumber = null);

        void WriteDirect(Page start, long pagePosition, int pagesToWrite);
	    Page GetWritable(long pageNumber);
	}
}