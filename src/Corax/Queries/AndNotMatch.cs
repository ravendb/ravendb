using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow.Server;
using static Voron.Global.Constants;

namespace Corax.Queries
{
    [DebuggerDisplay("{DebugView,nq}")]
    public unsafe partial struct AndNotMatch<TInner, TOuter> : IQueryMatch
    where TInner : IQueryMatch
    where TOuter : IQueryMatch
    {
        private TInner _inner;
        private TOuter _outer;

        private long _totalResults;
        private long _current;
        private QueryCountConfidence _confidence;

        public bool IsBoosting => _inner.IsBoosting || _outer.IsBoosting;
        public long Count => _totalResults;
        public long Current => _current;

        private readonly ByteStringContext _context;
        internal int _bufferSize;
        internal int _bufferIdx;
        internal long* _documentBuffer;
        internal bool _requiresSort;
        internal IDisposable _bufferHandler;

        public QueryCountConfidence Confidence => _confidence;

        private AndNotMatch(ByteStringContext context, 
            in TInner inner, in TOuter outer,
            long totalResults, QueryCountConfidence confidence)
        {
            _totalResults = totalResults;
            _current = QueryMatch.Start;

            _inner = inner;
            _outer = outer;
            _confidence = confidence;

            _context = context;

            int bufferSize = 4 * Math.Min(Math.Max(Size.Kilobyte, (int)outer.Count), 16 * Size.Kilobyte);
            _bufferHandler = _context.Allocate(bufferSize * sizeof(long), out var buffer);
            _bufferSize = bufferSize;
            _documentBuffer = (long*)buffer.Ptr;
            _bufferIdx = -1;
            _requiresSort = false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Fill(Span<long> matches)
        {
            if (_current == QueryMatch.Invalid)
                return 0;

            // Check if this is the second time we enter or not. 
            if (_bufferIdx == -1)
            {                
                var bufferState = new Span<long>(_documentBuffer, _bufferSize);
                int iterations = 0;

                int count = 0;
                while (true)
                {
                    var read = _outer.Fill(bufferState);
                    if (read == 0)
                        goto End;

                    // We will use multiple rounds to get the whole buffer.
                    iterations++;

                    // We havent finished and probably we will need to expand the temporary buffer.
                    int bufferUsedItems = count + read;
                    if (bufferUsedItems > _bufferSize * 3 / 4)
                    {
                        UnlikelyGrowBuffer(bufferUsedItems);
                        bufferState = new Span<long>(_documentBuffer, _bufferSize).Slice(count);
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
                    var bufferBasePtr = _documentBuffer;
                    count = SortAndRemoveDuplicates(bufferBasePtr, count);
                }

                _bufferIdx = count;
            }

            // The outer is empty, so item in inner will be returned. 
            if (_bufferIdx == 0)
                return _inner.Fill(matches);

            // Now it is time to run the other part of the algorithm, which is getting the Inner data until we fill the buffer.
            while (true)
            {
                int totalResults = 0;
                int iterations = 0;

                var resultsSpan = matches;
                while (resultsSpan.Length > 0)
                {
                    // RavenDB-17750: We have to fill everything possible UNTIL there are no more matches availables.
                    var results = _inner.Fill(resultsSpan);
                    if (results == 0)
                        break;

                    totalResults += results;
                    iterations++;

                    resultsSpan = resultsSpan.Slice(results);
                }

                // Again multiple Fill calls do not ensure that we will get a sequence of ordered
                // values, therefore we must ensure that we get a 'sorted' sequence ensuring those happen.
                if (iterations > 1)
                {
                    // We need to sort and remove duplicates.
                    var bufferBasePtr = (long*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(matches));
                    totalResults = SortAndRemoveDuplicates(bufferBasePtr, totalResults);
                }

                if (totalResults == 0)
                    return 0;
                
                Span<long> outerBuffer = new Span<long>(_documentBuffer, _bufferIdx);
                Span<long> innerBuffer = matches.Slice(0, totalResults);

                totalResults = MergeHelper.AndNot(innerBuffer, innerBuffer, outerBuffer);
                if (totalResults != 0)
                    return totalResults;
            }
        }

        private static int SortAndRemoveDuplicates(long* bufferBasePtr, int count)
        {
            MemoryExtensions.Sort(new Span<long>(bufferBasePtr, count));

            // We need to fill in the gaps left by removing deduplication process.
            // If there are no duplicated the writes at the architecture level will execute
            // way faster than if there are.

            var outputBufferPtr = bufferBasePtr;

            var bufferPtr = bufferBasePtr;
            var bufferEndPtr = bufferBasePtr + count - 1;
            while (bufferPtr < bufferEndPtr)
            {
                outputBufferPtr += bufferPtr[1] != bufferPtr[0] ? 1 : 0;
                *outputBufferPtr = bufferPtr[1];

                bufferPtr++;
            }

            count = (int)(outputBufferPtr - bufferBasePtr + 1);
            return count;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void UnlikelyGrowBuffer(int currentlyUsed)
        {
            // Calculate the new size. 
            int currentLength = _bufferSize;
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
            var bufferHandler = _context.Allocate(size * sizeof(long), out var buffer);

            // Ensure we copy the content and then switch the buffers. 
            new Span<long>(_documentBuffer, currentlyUsed).CopyTo(new Span<long>(buffer.Ptr, size));
            _bufferHandler.Dispose();

            _bufferSize = size;
            _documentBuffer = (long*)buffer.Ptr;
            _bufferHandler = bufferHandler;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int AndWith(Span<long> buffer, int matches)
        {
            throw new NotSupportedException($"{nameof(AndNotMatch<TInner, TOuter>)} does not support the operation of {nameof(AndWith)}.");
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Score(Span<long> matches, Span<float> scores)
        {
            _inner.Score(matches, scores);
        }

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{nameof(BinaryMatch)} [AndNot]",
                children: new List<QueryInspectionNode> { _inner.Inspect(), _outer.Inspect() },
                parameters: new Dictionary<string, string>()
                {
                    { nameof(IsBoosting), IsBoosting.ToString() },
                    { nameof(Count), $"{Count} [{Confidence}]" }
                });
        }

        string DebugView => Inspect().ToString();


        public static AndNotMatch<TInner, TOuter> Create(IndexSearcher searcher, in TInner inner, in TOuter outer)
        {
            // Estimate Confidence values.
            QueryCountConfidence confidence;
            if (inner.Count < outer.Count / 2)
                confidence = inner.Confidence;
            else if (outer.Count < inner.Count / 2)
                confidence = outer.Confidence;
            else
                confidence = inner.Confidence.Min(outer.Confidence);

            return new AndNotMatch<TInner, TOuter>(searcher.Allocator, in inner, in outer, inner.Count, confidence);
        }
    }
}
