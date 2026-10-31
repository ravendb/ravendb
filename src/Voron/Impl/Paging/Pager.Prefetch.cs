using System;
using System.Threading;
using Sparrow.Platform;
using Sparrow.Server.Platform;
using Voron.Global;

namespace Voron.Impl.Paging;

public unsafe partial class Pager
{
    // Data files on Linux are opened with `MADV_RANDOM`, faulting at 4 KB at a time (vs. Voron's 8kb pages)
    // This help us avoid separate faults
    private static readonly bool WillNeedSupported = PlatformDetails.RunningOnLinux;

    // Avoid repeated prefetching (syscall overhead only), slot is page num+1 (0 means empty)
    // duplicates will override, so it automatically refresh. Optimization only, not correctness, so we can afford to miss
    private const int RecentHintsBits = 12;
    private int[] _recentHints;

    private static ref int RecentSlot(int[] recent, long pageNumber) =>
        ref recent[(int)(((ulong)pageNumber * 0x9E3779B97F4A7C15UL) >> (64 - RecentHintsBits))]; // Fibonacci hashing

    public static void WillNeedPages(State state, long pageNumber, int numberOfPages = 1)
    {
        if (WillNeedSupported == false || state.Pager.Options.RunningOn32Bits)
            return;

        var recent = state.Pager._recentHints ??= new int[1 << RecentHintsBits];
        ref var slot = ref RecentSlot(recent, pageNumber);
        var key = (int)(pageNumber + 1);
        if (Volatile.Read(ref slot) == key)
            return;

        if (StartRead(state, pageNumber, numberOfPages))
            Volatile.Write(ref slot, key);
    }

    private static bool StartRead(State state, long pageNumber, int numberOfPages)
    {
        var range = new PalDefinitions.PrefetchRanges(state.ReadAddress + pageNumber * Constants.Storage.PageSize,
            (long)numberOfPages * Constants.Storage.PageSize);
        return Pal.rvn_prefetch_ranges(&range, 1, out _) == PalFlags.FailCodes.Success;
    }
}
