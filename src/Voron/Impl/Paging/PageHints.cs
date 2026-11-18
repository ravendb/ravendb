using System;
using System.Runtime.CompilerServices;
using System.Threading;
using Voron.Global;

namespace Voron.Impl.Paging;

/// <summary>
/// One bit per page of a data file, set when the page is hinted. Each 4 KB of bits (256 MB of pages) has the policy's clock
/// of its last clearing: a chunk older than the policy's stale time is cleared on its next use, so a page that may have
/// left the page cache is hinted again.
/// </summary>
internal sealed class PageHints
{
    internal const int ChunkLongs = 4 * Constants.Size.Kilobyte / sizeof(long);
    private const int ChunkShift = 9; // log2(ChunkLongs)

    private sealed class Bitmap(long[] bits, int[] clearedAt)
    {
        public readonly long[] Bits = bits;
        public readonly int[] ClearedAt = clearedAt;
    }

    private readonly PrefetchPolicy _policy;
    private Bitmap _bitmap;

    public PageHints(PrefetchPolicy policy, long numberOfPages)
    {
        _policy = policy;
        var longs = (numberOfPages + 63) / 64;
        _bitmap = new Bitmap(new long[longs], NewClearedAt(Chunks(longs), 0, policy.Clock));
    }

    /// <summary>False when the page is already marked.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryMark(long pageNumber)
    {
        var index = pageNumber >> 6;
        var bitmap = Use(index, index);
        var bit = 1L << (int)(pageNumber & 63);
        var word = bitmap.Bits[index];
        if ((word & bit) != 0)
            return false;
        bitmap.Bits[index] = word | bit; // a write lost to a race or a clearing costs one more hint
        return true;
    }

    /// <summary>Marks the pages; returns the first page that was not marked, -1 when all were.</summary>
    public long Mark(long firstPage, int numberOfPages)
    {
        var bitmap = Use(firstPage >> 6, (firstPage + numberOfPages - 1) >> 6);
        var bits = bitmap.Bits;
        var firstUnmarked = -1L;
        for (var page = firstPage; page < firstPage + numberOfPages; page++)
        {
            ref var word = ref bits[page >> 6];
            var bit = 1L << (int)(page & 63);
            if ((word & bit) != 0)
                continue;
            if (firstUnmarked == -1)
                firstUnmarked = page;
            word |= bit;
        }
        return firstUnmarked;
    }

    /// <summary>True when the page is marked and its chunk is not stale.</summary>
    public bool IsMarked(long pageNumber)
    {
        var index = pageNumber >> 6;
        var bitmap = Use(index, index);
        return (bitmap.Bits[index] & (1L << (int)(pageNumber & 63))) != 0;
    }

    // the bitmap that holds the longs from first to last, with their stale chunks cleared
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private Bitmap Use(long firstIndex, long lastIndex)
    {
        var bitmap = _bitmap;
        if (lastIndex >= bitmap.Bits.Length)
            bitmap = Grow(lastIndex);
        var now = _policy.Clock;
        var staleAfter = _policy.StaleAfterSeconds;
        for (var chunk = firstIndex >> ChunkShift; chunk <= lastIndex >> ChunkShift; chunk++)
        {
            var clearedAt = Volatile.Read(ref bitmap.ClearedAt[chunk]);
            if (now - clearedAt >= staleAfter)
                Clear(bitmap, (int)chunk, clearedAt, now);
        }
        return bitmap;
    }

    private static void Clear(Bitmap bitmap, int chunk, int clearedAt, int now)
    {
        if (Interlocked.CompareExchange(ref bitmap.ClearedAt[chunk], now, clearedAt) != clearedAt)
            return; // another reader clears it
        var start = chunk << ChunkShift;
        Array.Clear(bitmap.Bits, start, Math.Min(ChunkLongs, bitmap.Bits.Length - start));
    }

    // a bit set in the old bitmap while it is copied is lost: one more hint
    private Bitmap Grow(long index)
    {
        while (true)
        {
            var bitmap = Volatile.Read(ref _bitmap);
            if (index < bitmap.Bits.Length)
                return bitmap;
            var bits = new long[Math.Max(index + 1, bitmap.Bits.Length + bitmap.Bits.Length / 4)];
            bitmap.Bits.CopyTo(bits, 0);
            var clearedAt = NewClearedAt(Chunks(bits.Length), bitmap.ClearedAt.Length, _policy.Clock);
            bitmap.ClearedAt.CopyTo(clearedAt, 0);
            var grown = new Bitmap(bits, clearedAt);
            if (Interlocked.CompareExchange(ref _bitmap, grown, bitmap) == bitmap)
                return grown;
        }
    }

    private static long Chunks(long longs) => (longs + ChunkLongs - 1) / ChunkLongs;

    private static int[] NewClearedAt(long chunks, int from, int now)
    {
        var clearedAt = new int[Math.Max(chunks, 1)];
        Array.Fill(clearedAt, now, from, clearedAt.Length - from);
        return clearedAt;
    }
}
