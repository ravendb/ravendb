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

    // above this, one wait on the ring costs less than a non-blocking read of each page on the caller's thread
    private const int CheckedReadAheadLimit = 16;

    private PageHints _pageHints;
    private bool _hintsReads;

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

    internal bool HintsPages => _hintsReads;

    /// <summary>Starts reading a data file page that was not hinted within the stale time.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void HintPage(State state, long pageNumber)
    {
        var pager = state.Pager;
        if (pager._hintsReads == false || (ulong)pageNumber >= (ulong)state.NumberOfAllocatedPages || pager._pageHints.TryMark(pageNumber) == false)
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
        if (state.Pager._hintsReads == false)
            return 0;
        var hints = state.Pager._pageHints;

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
        if (state.Pager._hintsReads == false || numberOfPages <= 1 || pageNumber + numberOfPages > state.NumberOfAllocatedPages)
            return;
        var hints = state.Pager._pageHints;

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

    /// <summary>Completes once the pages are in the page cache. Pages hinted or requested within the stale time are skipped.</summary>
    [SkipLocalsInit]
    public static ValueTask ReadPagesAhead(State state, ReadOnlySpan<long> pageNumbers)
    {
        if (state.Pager._hintsReads == false)
            return ValueTask.CompletedTask;

        var hints = state.Pager._pageHints;
        long[] rented = null;
        Span<long> offsets = pageNumbers.Length <= 64 ? stackalloc long[pageNumbers.Length] : (rented = ArrayPool<long>.Shared.Rent(pageNumbers.Length));
        try
        {
            var count = 0;
            foreach (var pageNumber in pageNumbers)
            {
                if ((ulong)pageNumber >= (ulong)state.NumberOfAllocatedPages || hints.TryMark(pageNumber) == false)
                    continue; // also a page earlier in this group
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
}
