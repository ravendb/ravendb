using System;
using Nevar.Trees;

namespace Nevar.Impl
{
    public interface IVirtualPager : IDisposable
    {
        Page Get(Transaction tx, long n);

        long NumberOfAllocatedPages { get; }
        int PageSize { get; }
        int MaxNodeSize { get; }
        int PageMaxSpace { get; }
        int PageMinSpace { get; }

        void Flush();

        PagerState TransactionBegan();

        void EnsureContinious(Transaction tx, long requestedPageNumber, int pageCount);
    }
}