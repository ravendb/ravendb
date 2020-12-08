using System;
using Voron.Impl.Paging;
using Voron.Util.Settings;

namespace Voron.Impl.Journal
{
    public unsafe interface IJournalWriter : IDisposable
    {
        TimeSpan Write(long posBy4Kb, byte* p, long numberOf4Kb);
        int NumberOfAllocated4Kb { get; }
        bool Disposed { get; }
        bool DeleteOnClose { get; set; }
        AbstractPager CreatePager();
        void Read(byte* buffer, long numOfBytes, long offsetInFile);
        void Truncate(long size);

        void AddRef();
        bool Release();

        VoronPathSetting FileName { get; }
    }
}
