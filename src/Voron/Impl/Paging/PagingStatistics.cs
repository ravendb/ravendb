using System;
using System.Runtime.InteropServices;
using System.Threading;
using Sparrow.Binary;

namespace Voron.Impl.Paging;

public static class PagingStatistics
{
    [StructLayout(LayoutKind.Explicit, Size = 64)] // Cache line alignment (64 bytes)
    private struct Counter
    {
        [FieldOffset(0)]
        public long PageReads;
        [FieldOffset(8)]
        public long PageWrites;
    }

    private static readonly Counter[] Counters;
    private static readonly int CoreCountMask;

    static PagingStatistics()
    {
        int coreCount = Bits.PowerOf2(Environment.ProcessorCount);
        Counters = new Counter[coreCount];
        CoreCountMask = coreCount - 1;
    }

    public static void MarkRead(long l)
    {
        int coreId = Thread.GetCurrentProcessorId() & CoreCountMask;
        Interlocked.Add(ref Counters[coreId].PageReads, l);
    }

    public static void MarkWrite(long l)
    {
        int coreId = Thread.GetCurrentProcessorId() & CoreCountMask;
        Interlocked.Add(ref Counters[coreId].PageWrites, l);
    }

    public static (long TotalReads, long TotalWrites) GetTotals()
    {
        long totalReads = 0, totalWrites = 0;
        for (int i = 0; i < Counters.Length; i++)
        {
            totalReads += Interlocked.Read(ref Counters[i].PageReads);
            totalWrites += Interlocked.Read(ref Counters[i].PageWrites);
        }
        return (totalReads, totalWrites);
    }
}
