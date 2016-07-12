using System;
using System.Threading.Tasks;
using Voron.Impl.Paging;

namespace Voron.Impl.Journal
{
    public unsafe interface IJournalWriter : IDisposable
    {
        void WritePages(long position, byte* p, int numberOfPages);
        int NumberOfAllocatedPages { get;  }
        bool Disposed { get; }
        bool DeleteOnClose { get; set; }
        AbstractPager CreatePager();
        bool Read(long pageNumber, byte* buffer, int count);
    }
}
