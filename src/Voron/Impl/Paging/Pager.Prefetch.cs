using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Platform;
using Sparrow.Server.Platform;
using Sparrow.Server.Threading;
using Voron.Global;

namespace Voron.Impl.Paging;

public unsafe partial class Pager
{
    // a hint reads a whole 8 KB page in one request; a fault uses the kernel's heuristics
    private static readonly bool WillNeedSupported = PlatformDetails.RunningOnLinux;

    // Avoid repeated prefetching (syscall overhead only), slot is page num+1 (0 means empty)
    // duplicates will override, so it automatically refresh. Optimization only, not correctness, so we can afford to miss
    private const int RecentHintsBits = 12;
    private int[] _recentHints;

    // above this, one wait on the ring costs less than a non-blocking read of each page on the caller's thread
    private const int CheckedReadAheadLimit = 16;

    private PageHints _pageHints;

    /// <summary>An iterator reads ahead this many of the pages its next entries refer to.</summary>
    internal const int IteratorReadAheadPages = 8;

    private static readonly PerCoreValue<PerCoreSum> PagesHintedCount = new();
    private static readonly PerCoreValue<PerCoreSum> PagesReadAheadCount = new();
    private static readonly PerCoreValue<PerCoreSum> ReadAheadPagesSkippedCount = new();

    /// <summary>Data file pages hinted on a read since the process started.</summary>
    public static long PagesHinted => PagesHintedCount.Value;

    /// <summary>Data file pages read ahead of sequential reads since the process started.</summary>
    public static long PagesReadAhead => PagesReadAheadCount.Value;

    /// <summary>Data file pages not read ahead since the process started, because they were hinted within the stale time.</summary>
    public static long ReadAheadPagesSkipped => ReadAheadPagesSkippedCount.Value;

    internal bool HintsPages => _pageHints != null;

    /// <summary>Starts reading a data file page that was not hinted within the stale time.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void HintPage(State state, long pageNumber)
    {
        var hints = state.Pager._pageHints;
        if (hints == null || (ulong)pageNumber >= (ulong)state.NumberOfAllocatedPages || hints.TryMark(pageNumber) == false)
            return;

        Pal.rvn_pager_will_need(state.Handle, pageNumber * Constants.Storage.PageSize, Constants.Storage.PageSize);
        PagesHintedCount.Apply(1);
    }

    /// <summary>
    /// Starts reading pages ahead of a sequential read, from the first one not hinted within the stale time: on this thread
    /// with posix_fadvise when onCallingThread, else through the ring; neither waits for the reads. Returns the pages requested.
    /// </summary>
    internal static int ReadWindowAhead(State state, long firstPage, int numberOfPages, bool onCallingThread)
    {
        var hints = state.Pager._pageHints;
        if (hints == null)
            return 0;

        var first = hints.Mark(firstPage, numberOfPages);
        var skipped = first == -1 ? numberOfPages : (int)(first - firstPage);
        if (skipped != 0)
            ReadAheadPagesSkippedCount.Apply(skipped);
        if (first == -1)
            return 0;

        var pages = numberOfPages - skipped;
        var offset = first * Constants.Storage.PageSize;
        var bytes = (long)pages * Constants.Storage.PageSize;
        if (onCallingThread || Pal.rvn_pager_prefetch(state.Handle, offset, bytes) == 0) // 0: not queued, the ring is full
            Pal.rvn_pager_will_need(state.Handle, offset, bytes);
        PagesReadAheadCount.Apply(pages);
        return pages;
    }

    /// <summary>Starts reading the pages after the first page of an overflow value, from the first one not hinted within the stale time.</summary>
    public static void HintOverflowPages(State state, long pageNumber, int numberOfPages)
    {
        var hints = state.Pager._pageHints;
        if (hints == null || numberOfPages <= 1 || pageNumber + numberOfPages > state.NumberOfAllocatedPages)
            return;

        // a read-ahead window can hold the first pages of a value, not all of them
        var first = hints.Mark(pageNumber + 1, numberOfPages - 1);
        if (first == -1)
            return;
        Pal.rvn_pager_will_need(state.Handle, first * Constants.Storage.PageSize, (pageNumber + numberOfPages - first) * Constants.Storage.PageSize);
        PagesHintedCount.Apply(pageNumber + numberOfPages - first);
    }

    /// <summary>False where <see cref="ReadPagesAhead"/> and <see cref="HintPage"/> do nothing: callers can skip finding the pages.</summary>
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
}
