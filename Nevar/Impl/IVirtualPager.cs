using System;
using System.Collections.Generic;
using Nevar.Trees;

namespace Nevar.Impl
{
    public interface IVirtualPager : IDisposable
    {
        PagerState PagerState { get; }

        Page Get(Transaction tx, long n, bool errorOnChange = false);
		void AllocateMorePages(Transaction tx, long newLength);

        Page TempPage { get; }

        long NumberOfAllocatedPages { get; }
        int PageSize { get; }
        int MaxNodeSize { get; }
        int PageMaxSpace { get; }
        int PageMinSpace { get; }

		void Flush(List<long> sortedPagesToFlush);
		void Flush(long headerPageId);

	    void Sync();

        PagerState TransactionBegan();

        void EnsureContinious(Transaction tx, long requestedPageNumber, int pageCount);
    }
}