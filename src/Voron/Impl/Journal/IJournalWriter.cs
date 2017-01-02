using System;
using Voron.Impl.Paging;

namespace Voron.Impl.Journal
{
    public unsafe interface IJournalWriter : IDisposable
    {
        void Write(long posBy4Kb, byte* p, int numberOf4Kb);
        int NumberOfAllocated4Kb { get;  }
        bool Disposed { get; }
        bool DeleteOnClose { get; set; }
        AbstractPager CreatePager();
        bool Read(byte* buffer, long numOfBytes, long offsetInFile);
        void Truncate(long size);
    }
}
