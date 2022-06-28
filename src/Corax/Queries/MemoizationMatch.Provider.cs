using System;
using System.Runtime.CompilerServices;
using Corax.Utils;
using static Voron.Global.Constants;

namespace Corax.Queries
{

    public unsafe struct MemoizationMatchProvider<TInner> : IMemoizationMatchSource
             where TInner : IQueryMatch
    {
        private int _replayCounter;
        public int ReplayCounter => _replayCounter;
        
        private TInner _inner;
        public bool IsBoosting => _inner.IsBoosting;
        public long Count => _inner.Count;
        public QueryCountConfidence Confidence => _inner.Confidence;

        public int BufferSize => _buffer == null ? 0 : _buffer.Length;

        internal long[] _buffer;
        internal int _bufferEndIdx;

        public MemoizationMatchProvider(in TInner inner)
        {
            _inner = inner;
            _replayCounter = 0;
            _buffer = null;
            _bufferEndIdx = -1;
        }

        public MemoizationMatch Replay()
        {
            _replayCounter++;
            return MemoizationMatch.Create(new MemoizationMatch<TInner>(this));
        }

        public Span<long> FillAndRetrieve()
        {
            if (_bufferEndIdx < 0)
                InitializeInner();

            if (_bufferEndIdx == 0)
                return Span<long>.Empty;

            return _buffer.AsSpan(0, _bufferEndIdx);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int Fill(Span<long> matches) => _inner.Fill(matches);
        
        private void InitializeInner()
        {
            // We rent a buffer size. 
            int bufferSize = 4 * Math.Min(Math.Max(Size.Kilobyte, (int)_inner.Count), 16 * Size.Kilobyte);
            _buffer = QueryContext.MatchesPool.Rent(bufferSize);

            var bufferState = _buffer.AsSpan();
            int iterations = 0;

            int count = 0;
            while (true)
            {
                var read = _inner.Fill(bufferState);
                if (read == 0)
                    goto End;

                // We will use multiple rounds to get the whole buffer.
                iterations++;

                // We havent finished and probably we will need to expand the temporary buffer.
                int bufferUsedItems = count + read;
                if (bufferUsedItems > _buffer.Length * 3 / 4)
                {
                    UnlikelyGrowBuffer(bufferUsedItems);
                    bufferState = _buffer.AsSpan().Slice(count);
                }

                // Every time this is called we will store in a growable temporary buffer all the matches to be used in the AndNot later.
                bufferState = bufferState.Slice(read);
                count += read;
            }

            End:
            // The problem is that multiple Fill calls do not ensure that we will get a sequence of ordered
            // values, therefore we must ensure that we get a 'sorted' sequence ensuring those happen.
            if (iterations > 1 && count > 1)
            {
                // We need to sort and remove duplicates.
                count = Sorting.SortAndRemoveDuplicates(_buffer.AsSpan(0, count));
            }

            _bufferEndIdx = count;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void UnlikelyGrowBuffer(int currentlyUsed)
        {
            // Calculate the new size. 
            int currentLength = _buffer.Length;
            int size;
            if (currentLength > 16 * Size.Megabyte)
            {
                size = (int)(currentLength * 1.5);
            }
            else
            {
                size = currentLength * 2;
            }

            // Allocate the new buffer
            var newBuffer = QueryContext.MatchesPool.Rent(size);

            // Ensure we copy the content and then switch the buffers. 
            _buffer.AsSpan(0, currentlyUsed).CopyTo(newBuffer.AsSpan(0, size));

            QueryContext.MatchesPool.Return(_buffer);
            _buffer = newBuffer;
        }

        public void Score(Span<long> matches, Span<float> scores)
        {
            _inner.Score(matches, scores);
        }

        public QueryInspectionNode Inspect()
        {
            return _inner.Inspect();
        }

        public void Dispose()
        {
            QueryContext.MatchesPool.Return(_buffer);
        }
    }
}
