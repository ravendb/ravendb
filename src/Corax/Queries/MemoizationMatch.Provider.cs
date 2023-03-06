using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow.Server;
using Sparrow.Server.Utils;
using static Voron.Global.Constants;

namespace Corax.Queries
{
    [DebuggerDisplay("{DebugView,nq}")]
    public class MemoizationMatchProvider<TInner> : IMemoizationMatchSource
             where TInner : IQueryMatch
    {
        private int _replayCounter;
        public int ReplayCounter => _replayCounter;

        private readonly ByteStringContext _ctx;
        private TInner _inner;

        public bool IsBoosting => _inner.IsBoosting;
        public long Count => _inner.Count;
        public QueryCountConfidence Confidence => _inner.Confidence;

        public Span<long> Buffer => MemoryMarshal.Cast<byte, long>(_bufferHolder.ToSpan());

        private int _bufferEndIdx;
        private ByteString _bufferHolder;
        private ByteStringContext<ByteStringMemoryCache>.InternalScope _bufferScope;
        
        public MemoizationMatchProvider(ByteStringContext ctx, in TInner inner)
        {
            _ctx = ctx;
            _inner = inner;
            _replayCounter = 0;
            _bufferHolder = default;
            _bufferScope = default;
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

            return Buffer[.._bufferEndIdx];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int Fill(Span<long> matches) => _inner.Fill(matches);
        
        private void InitializeInner()
        {
            // We rent a buffer size. 
            int bufferSize = 4 * Math.Min(Math.Max(Size.Kilobyte, (int)_inner.Count), 16 * Size.Kilobyte);
            _bufferScope.Dispose();
            _bufferScope = _ctx.Allocate(bufferSize * sizeof(long), out _bufferHolder);

            var bufferState = Buffer;
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
                if (bufferUsedItems > Buffer.Length * 3 / 4)
                {
                    UnlikelyGrowBuffer(bufferUsedItems);
                    bufferState = Buffer[count..];
                }

                // Every time this is called we will store in a growable temporary buffer all the matches to be used in the AndNot later.
                bufferState = bufferState[read..];
                count += read;
            }

            End:
            // The problem is that multiple Fill calls do not ensure that we will get a sequence of ordered
            // values, therefore we must ensure that we get a 'sorted' sequence ensuring those happen.
            if (iterations > 1 && count > 1)
            {
                // We need to sort and remove duplicates.
                count = Sorting.SortAndRemoveDuplicates(Buffer[..count]);
            }

            _bufferEndIdx = count;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void UnlikelyGrowBuffer(int currentlyUsed)
        {
            // Calculate the new size. 
            int currentLength = Buffer.Length;
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
            var newBufferScope = _ctx.Allocate(size * sizeof(long), out var newBufferHolder);

            // Ensure we copy the content and then switch the buffers. 
            Buffer[..currentlyUsed].CopyTo(MemoryMarshal.Cast<byte,long>(newBufferHolder.ToSpan()));
            _bufferScope.Dispose();
            _bufferHolder = newBufferHolder;
            _bufferHolder = newBufferHolder;
        }

        public void Score(Span<long> matches, Span<float> scores, float boostFactor)
        {
            _inner.Score(matches, scores, boostFactor);
        }

        public QueryInspectionNode Inspect()
        {
            return _inner.Inspect();
        }
        string DebugView => Inspect().ToString();

        public void Dispose()
        {
            _bufferScope.Dispose();
        }
    }
}
