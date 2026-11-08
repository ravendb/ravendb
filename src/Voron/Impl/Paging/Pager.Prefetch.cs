using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
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

    // above this, one wait on the ring costs less than a non-blocking read of each page on the caller's thread
    private const int CheckedReadAheadLimit = 16;

    /// <summary>False where <see cref="ReadPagesAhead"/> and <see cref="WillNeedPages"/> do nothing: callers can skip finding the pages.</summary>
    public static bool CanReadPagesAhead(StorageEnvironmentOptions options) =>
        WillNeedSupported && options.RunningOn32Bits == false && options.EnablePrefetching;

    /// <summary>Completes once the pages are in the page cache. Recently requested pages are skipped.</summary>
    [SkipLocalsInit]
    public static ValueTask ReadPagesAhead(State state, ReadOnlySpan<long> pageNumbers)
    {
        if (CanReadPagesAhead(state.Pager.Options) == false)
            return ValueTask.CompletedTask;

        var recent = state.Pager._recentHints ??= new int[1 << RecentHintsBits];
        long[] rented = null;
        Span<long> offsets = pageNumbers.Length <= 64 ? stackalloc long[pageNumbers.Length] : (rented = ArrayPool<long>.Shared.Rent(pageNumbers.Length));
        try
        {
            var count = 0;
            foreach (var pageNumber in pageNumbers)
            {
                ref var slot = ref RecentSlot(recent, pageNumber);
                var key = (int)(pageNumber + 1);
                if (Volatile.Read(ref slot) == key)
                    continue; // asked for a moment ago, or earlier in this group
                Volatile.Write(ref slot, key);
                offsets[count++] = pageNumber * Constants.Storage.PageSize;
            }
            if (count == 0)
                return ValueTask.CompletedTask;

            if (count > CheckedReadAheadLimit)
                return ReadAhead(state, offsets[..count]);

            int missing;
            fixed (long* p = offsets)
                missing = Pal.rvn_pager_start_reads(state.Handle, p, count);
            return missing == 0 ? ValueTask.CompletedTask : ReadAhead(state, offsets[..missing]);
        }
        finally
        {
            if (rented != null)
                ArrayPool<long>.Shared.Return(rented);
        }
    }

    // rvn_pager_start_reads reports the missing pages: kernel 6.12 and later also start their reads; earlier kernels do not
    private static ValueTask ReadAhead(State state, ReadOnlySpan<long> offsets)
    {
        var ring = state.Pager._prefetchRing;
        if (ring is { CanWaitForPages: true })
            return WaitForPages(state.Pager, ring, offsets);
        foreach (var offset in offsets)
            Pal.rvn_pager_will_need(state.Handle, offset, Constants.Storage.PageSize);
        return ValueTask.CompletedTask;
    }

    private static ValueTask WaitForPages(Pager pager, PrefetchRing ring, ReadOnlySpan<long> offsets)
    {
        var ticket = pager._prefetchTicket;
        if (ring.TryRentSlot(out var slot))
            return ring.WaitForPages(slot, ticket, offsets);
        var copy = offsets.ToArray(); // every wait slot is taken: rare
        return new ValueTask(ring.RentSlotAsync()
            .ContinueWith(rented => ring.WaitForPages(rented.Result, ticket, copy).AsTask(), TaskScheduler.Default)
            .Unwrap());
    }

    private static ref int RecentSlot(int[] recent, long pageNumber) =>
        ref recent[(int)(((ulong)pageNumber * 0x9E3779B97F4A7C15UL) >> (64 - RecentHintsBits))]; // Fibonacci hashing

    public static void WillNeedPages(State state, long pageNumber, int numberOfPages = 1)
    {
        if (CanReadPagesAhead(state.Pager.Options) == false)
            return;

        var recent = state.Pager._recentHints ??= new int[1 << RecentHintsBits];
        ref var slot = ref RecentSlot(recent, pageNumber);
        var key = (int)(pageNumber + 1);
        if (Volatile.Read(ref slot) == key)
            return;
        Volatile.Write(ref slot, key);

        Pal.rvn_pager_will_need(state.Handle, pageNumber * Constants.Storage.PageSize, (long)numberOfPages * Constants.Storage.PageSize);
    }
}
