using System;
using System.Runtime.CompilerServices;

namespace Voron.Impl.Paging;

/// <summary>
/// Finds forward runs in the data file pages a transaction reads, and reads ahead of each run in a window that doubles up to
/// 256 KB. The first window is requested on the reading thread, the next one when the reads pass the middle of the window.
/// </summary>
internal struct SequentialReadAhead
{
    private const int Streams = 4;
    internal const int MaxGapPages = 8;
    internal const int FirstWindowPages = 4;
    internal const int MaxWindowPages = 32;

    private struct Stream
    {
        public long Next; // the page after the last read, 0 for an unused stream
        public long WindowEnd; // the page after the last page read ahead
        public int WindowPages;
    }

    [InlineArray(Streams)]
    private struct StreamArray
    {
        private Stream _element;
    }

    private StreamArray _streams;
    private int _replace;

    public void OnRead(Pager.State state, long pageNumber, int numberOfPages)
    {
        if (state.Pager.HintsPages == false)
            return; // Windows, macOS, 32 bits, or prefetching is off

        if (TryAdvance(pageNumber, numberOfPages, state.NumberOfAllocatedPages, out var start, out var pages, out var onCallingThread))
            Pager.ReadWindowAhead(state, start, pages, onCallingThread);
    }

    /// <summary>True when the read continues a run and a window should be read ahead.</summary>
    internal bool TryAdvance(long pageNumber, int numberOfPages, long numberOfAllocatedPages, out long start, out int pages, out bool onCallingThread)
    {
        start = 0;
        pages = 0;
        onCallingThread = false;
        for (var i = 0; i < Streams; i++)
        {
            ref var stream = ref _streams[i];
            if (stream.Next == 0 || pageNumber < stream.Next || pageNumber >= Math.Max(stream.WindowEnd, stream.Next + MaxGapPages))
                continue;

            stream.Next = pageNumber + numberOfPages;
            if (stream.WindowPages != 0 && stream.WindowEnd - stream.Next > stream.WindowPages / 2)
                return false;

            onCallingThread = stream.WindowPages == 0;
            start = Math.Max(stream.WindowEnd, stream.Next);
            stream.WindowPages = onCallingThread ? FirstWindowPages : Math.Min(stream.WindowPages * 2, MaxWindowPages);
            var end = Math.Min(start + stream.WindowPages, numberOfAllocatedPages);
            if (end <= start)
                return false;
            stream.WindowEnd = end;
            pages = (int)(end - start);
            return true;
        }

        ref var replaced = ref _streams[_replace];
        _replace = (_replace + 1) % Streams;
        replaced = new Stream { Next = pageNumber + numberOfPages, WindowEnd = pageNumber + numberOfPages };
        return false;
    }
}
