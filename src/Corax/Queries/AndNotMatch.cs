using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow.Server;
using Sparrow.Server.Utils;
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
        private QueryCountConfidence _confidence;        

        public bool IsBoosting => _inner.IsBoosting || _outer.IsBoosting;
        public long Count => _totalResults;

        private readonly ByteStringContext _context;
        internal bool _isAndWithBuffer;
        internal long* _buffer;
        internal int _bufferSize;
        internal int _bufferIdx;              
        internal IDisposable _bufferHandler;

        public QueryCountConfidence Confidence => _confidence;

        private AndNotMatch(ByteStringContext context, 
            in TInner inner, in TOuter outer,
            long totalResults, QueryCountConfidence confidence)
        {
            _totalResults = totalResults;

            _inner = inner;
            _outer = outer;
            _confidence = confidence;

            _context = context;

            int bufferSize = 4 * Math.Min(Math.Max(Size.Kilobyte, (int)outer.Count), 16 * Size.Kilobyte);
            _bufferHandler = _context.Allocate(bufferSize * sizeof(long), out var buffer);
            _bufferSize = bufferSize;
            _buffer = (long*)buffer.Ptr;
            _bufferIdx = -1;
            _isAndWithBuffer = false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Fill(Span<long> matches)
        {
            if (_isAndWithBuffer)
                throw new InvalidOperationException($"We cannot execute `{nameof(Fill)}` after initiating a `{nameof(AndWith)}` operation.");

            // Check if this is the second time we enter or not. 
            if (_bufferIdx == -1)
            {                
                var bufferState = new Span<long>(_buffer, _bufferSize);
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
                        bufferState = new Span<long>(_buffer, _bufferSize).Slice(count);
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
                    var bufferBasePtr = _buffer;
                    count = Sorting.SortAndRemoveDuplicates(bufferBasePtr, count);
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
                        break; // We are certainly done. As `Fill` must not return 0 results unless it is done. 

                    totalResults += results;
                    iterations++;

                    resultsSpan = resultsSpan.Slice(results);
                }

                // Again multiple Fill calls do not ensure that we will get a sequence of ordered
                // values, therefore we must ensure that we get a 'sorted' sequence ensuring those happen.
                if (iterations > 1)
                {
                    // We need to sort and remove duplicates.
                    totalResults = Sorting.SortAndRemoveDuplicates(matches.Slice(0, totalResults));
                }

                // This is an early bailout, the only way this can happen is when Fill returns 0 and we dont have
                // any match to return. 
                if (totalResults == 0)
                    return 0;
                
                // We have matches and therefore we need now to remove the ones found in the outer buffer.
                Span<long> outerBuffer = new Span<long>(_buffer, _bufferIdx);
                Span<long> innerBuffer = matches.Slice(0, totalResults);
                totalResults = MergeHelper.AndNot(innerBuffer, innerBuffer, outerBuffer);

                // Since we would require to sort again if we dont return, we return what we have instead.
                if (totalResults != 0)
                    return totalResults; 

                // If can happen that we filtered out everything, but we cannot return 0. Therefore, we will
                // continue executing until we run out of any potential inner match. 
            }
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
            new Span<long>(_buffer, currentlyUsed).CopyTo(new Span<long>(buffer.Ptr, size));
            _bufferHandler.Dispose();

            _bufferSize = size;
            _buffer = (long*)buffer.Ptr;
            _bufferHandler = bufferHandler;
        }


        [MethodImpl(MethodImplOptions.NoInlining)]
        private (IDisposable, ByteString) UnlikelyGrowBuffer(ByteString buffer, int currentlyUsed)
        {
            // Calculate the new size. 
            int currentSizeInBytes = buffer.Length;
            int sizeInBytes;
            if (currentSizeInBytes > 16 * Size.Megabyte)
            {
                sizeInBytes = (int)(currentSizeInBytes * 1.5);
            }
            else
            {
                sizeInBytes = currentSizeInBytes * 2;
            }

            // Allocate the new buffer based on the size the original buffer had in bytes.
            var newBufferHandler = _context.Allocate(sizeInBytes * sizeof(long), out var newBuffer);

            // Ensure we copy the content and then switch the buffers. 
            new Span<long>(buffer.Ptr, currentlyUsed).CopyTo(new Span<long>(newBuffer.Ptr, newBuffer.Length));

            return (newBufferHandler, newBuffer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int AndWith(Span<long> buffer, int matches)
        {
            // This is not an AndWith memoized buffer, therefore we need to acquire a buffer to store the results
            // before continuing.   

            if (!_isAndWithBuffer)
            {
                int bufferSizeInItems = 4 * Math.Min(Math.Max(Size.Kilobyte, (int)_inner.Count), 16 * Size.Kilobyte);
                
                IDisposable scope = _context.Allocate(bufferSizeInItems * sizeof(long), out ByteString currentMatchesBuffer);
                var currentMatches = MemoryMarshal.Cast<byte, long>(currentMatchesBuffer.ToSpan());

                int totalResults = 0;

                // Now it is time to run the other part of the algorithm, which is getting the Inner data until we fill the buffer.
                bool isDone = false;
                while (!isDone)
                {
                    int iterations = 0;

                    var resultsSpan = currentMatches;
                    while (resultsSpan.Length > 0)
                    {
                        // RavenDB-17750: We have to fill everything possible UNTIL there are no more matches availables.
                        var read = Fill(resultsSpan);
                        if (read == 0)
                        {
                            isDone = true;
                            break;
                        }

                        // We havent finished and probably we will need to expand the temporary buffer.
                        int bufferUsedItems = totalResults + read;
                        if (bufferUsedItems > bufferSizeInItems * 15 / 16)
                        {                            
                            (var newBufferHandler, currentMatchesBuffer) = UnlikelyGrowBuffer(currentMatchesBuffer, bufferUsedItems);
                            scope.Dispose();
                            scope = newBufferHandler;

                            resultsSpan = MemoryMarshal.Cast<byte, long>(currentMatchesBuffer.ToSpan());
                            bufferSizeInItems = resultsSpan.Length;
                            resultsSpan = resultsSpan.Slice(totalResults);                            
                        }

                        totalResults += read;
                        iterations++;

                        resultsSpan = resultsSpan.Slice(read);
                    }

                    // Again multiple Fill calls do not ensure that we will get a sequence of ordered
                    // values, therefore we must ensure that we get a 'sorted' sequence ensuring those happen.
                    if (iterations > 1 && totalResults > 0)
                    {
                        // We need to sort and remove duplicates.
                        var bufferBasePtr = (long*)currentMatchesBuffer.Ptr;
                        totalResults = Sorting.SortAndRemoveDuplicates(bufferBasePtr, totalResults);
                    }
                }

                // Now we signal that this is now indeed an AndWith memoized buffer, no Fill allowed from now on.                
                _isAndWithBuffer = true;
                _bufferHandler.Dispose();
                _bufferHandler = scope;
                _buffer = (long*)currentMatchesBuffer.Ptr;
                _bufferSize = currentMatchesBuffer.Size;
                _bufferIdx = totalResults;                
            }

            // If we dont have any result, no need to do anything. And with nothing will mean that there is nothing.
            if (_bufferSize == 0)
                return 0;

            return MergeHelper.And(buffer, buffer.Slice(0, matches), new Span<long>(_buffer, _bufferIdx));
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Score(Span<long> matches, Span<float> scores, float boostFactor)
        {
            _inner.Score(matches, scores, boostFactor);
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
