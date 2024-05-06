using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Corax.Querying.Matches.Meta;
using Sparrow.Server;
using Sparrow.Server.Utils;

namespace Corax.Querying.Matches
{
    [DebuggerDisplay("{DebugView,nq}")]
    public struct AndNotMatch<TInner, TOuter> : IQueryMatch
    where TInner : IQueryMatch
    where TOuter : IQueryMatch
    {
        private TInner _inner;
        private TOuter _outer;

        private long _totalResults;
        private QueryCountConfidence _confidence;
        private readonly CancellationToken _token;

        public bool IsBoosting => _inner.IsBoosting || _outer.IsBoosting;
        public long Count => _totalResults;

        private readonly ByteStringContext _context;
        
        /// <summary>
        /// Indicates that the buffer is used by the AndWith method.
        /// </summary>
        private bool _isAndWithBuffer;

        private GrowableBuffer<Progressive> _buffer;

        private bool _doNotSortResults;

        public SkipSortingResult AttemptToSkipSorting()
        {
            var r = _inner.AttemptToSkipSorting();
            // if the inner requires sorting, we also require it
            _doNotSortResults = r != SkipSortingResult.SortingIsRequired;
            return r;
        }

        public QueryCountConfidence Confidence => _confidence;

        private AndNotMatch(ByteStringContext context, 
            in TInner inner, in TOuter outer,
            long totalResults, QueryCountConfidence confidence, CancellationToken token)
        {
            _totalResults = totalResults;

            _inner = inner;
            _outer = outer;
            _confidence = confidence;
            _token = token;

            _context = context;
            _isAndWithBuffer = false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Fill(Span<long> matches)
        {
            if (_isAndWithBuffer)
                throw new InvalidOperationException($"We cannot execute `{nameof(Fill)}` after initiating a `{nameof(AndWith)}` operation.");
            // Check if this is the second time we enter or not. 
            if (_buffer.IsInitialized == false)
            {
                _buffer = new GrowableBuffer<Progressive>();
                int iterations = 0;
                
                _buffer.Init(_context, _outer.Count);
                while (_outer.Fill(_buffer.GetSpace()) is var read)
                {
                    if (read == 0)
                        break;
                    
                    _buffer.AddUsage(read);
                    iterations++;
                }
                
                // The problem is that multiple Fill calls do not ensure that we will get a sequence of ordered
                // values, therefore we must ensure that we get a 'sorted' sequence ensuring those happen.
                if (iterations > 1 && _buffer.Count > 1)
                {
                    var newCount = Sorting.SortAndRemoveDuplicates(_buffer.Results);
                    _buffer.Truncate(newCount);
                }
            }

            // The outer is empty, so item in inner will be returned. 
            if (_buffer.Count == 0)
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
                if (_doNotSortResults == false && iterations > 1)
                {
                    // We need to sort and remove duplicates.
                    
                    _token.ThrowIfCancellationRequested();
                    totalResults = Sorting.SortAndRemoveDuplicates(matches.Slice(0, totalResults));
                }

                // This is an early bailout, the only way this can happen is when Fill returns 0 and we dont have
                // any match to return. 
                if (totalResults == 0)
                    return 0;
                
                // We have matches and therefore we need now to remove the ones found in the outer buffer.
                Span<long> outerBuffer = _buffer.Results;
                Span<long> innerBuffer = matches.Slice(0, totalResults);
                _token.ThrowIfCancellationRequested();
                totalResults = MergeHelper.AndNot(innerBuffer, innerBuffer, outerBuffer);

                // Since we would require to sort again if we dont return, we return what we have instead.
                if (totalResults != 0)
                    return totalResults; 

                // If can happen that we filtered out everything, but we cannot return 0. Therefore, we will
                // continue executing until we run out of any potential inner match. 
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int AndWith(Span<long> buffer, int matches)
        {
            // This is not an AndWith memoized buffer, therefore we need to acquire a buffer to store the results
            // before continuing.   
            if (_isAndWithBuffer == false)
            {
                _token.ThrowIfCancellationRequested();
                var andWithBuffer = new GrowableBuffer<Progressive>();
                andWithBuffer.Init(_context, Count);

                // Now it is time to run the other part of the algorithm, which is getting the Inner data until we fill the buffer.
                int iterations = 0;
                while (Fill(andWithBuffer.GetSpace()) is var read)
                {
                    if (read == 0)
                        break;
                    
                    _token.ThrowIfCancellationRequested();
                    andWithBuffer.AddUsage(read);
                    iterations++;
                }

                // Again multiple Fill calls do not ensure that we will get a sequence of ordered
                // values, therefore we must ensure that we get a 'sorted' sequence ensuring those happen.
                if (iterations > 1 && andWithBuffer.Count > 0)
                {
                    // We need to sort and remove duplicates.
                    _token.ThrowIfCancellationRequested();
                    var newCount = Sorting.SortAndRemoveDuplicates(andWithBuffer.Results);
                    andWithBuffer.Truncate(newCount);
                }
                
                // Now we signal that this is now indeed an AndWith memoized buffer, no Fill allowed from now on.                
                _isAndWithBuffer = true;
                _buffer.Dispose();
                _buffer = andWithBuffer;
                
                //Since we evaluated whole query we exactly know how many items it returns.
                _totalResults = _buffer.Count;
                _confidence = QueryCountConfidence.High;
            }

            // If we don't have any result, no need to do anything. And with nothing will mean that there is nothing.
            if (_buffer.Count == 0)
                return 0;

            _token.ThrowIfCancellationRequested();
            return MergeHelper.And(buffer, buffer.Slice(0, matches), _buffer.Results);
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
                    { Constants.QueryInspectionNode.IsBoosting, IsBoosting.ToString() },
                    { Constants.QueryInspectionNode.Count, Count.ToString() },
                    { Constants.QueryInspectionNode.CountConfidence, Confidence.ToString() }
                });
        }

        string DebugView => Inspect().ToString();


        public static AndNotMatch<TInner, TOuter> Create(IndexSearcher searcher, in TInner inner, in TOuter outer, in CancellationToken token)
        {
            // Estimate Confidence values.
            QueryCountConfidence confidence;
            if (inner.Count < outer.Count / 2)
                confidence = inner.Confidence;
            else if (outer.Count < inner.Count / 2)
                confidence = outer.Confidence;
            else
                confidence = inner.Confidence.Min(outer.Confidence);

            return new AndNotMatch<TInner, TOuter>(searcher.Allocator, in inner, in outer, inner.Count, confidence, token);
        }
    }
}
