using System;
using System.Collections.Generic;
using System.Diagnostics;
using Corax.Querying.Matches.Meta;

namespace Corax.Querying.Matches
{    
    [DebuggerDisplay("{DebugView,nq}")]
    public struct MemoizationMatch<TInner> : IQueryMatch
        where TInner : IQueryMatch
    {
        private MemoizationMatchProvider<TInner> _inner;
        private int _bufferCurrentIdx;
        public bool IsAllEntries = typeof(TInner) == typeof(AllEntriesMatch);

        public bool IsBoosting => _inner.IsBoosting;
        public long Count => _inner.Count;

        public SkipSortingResult AttemptToSkipSorting()
        {
             return SkipSortingResult.WillSkipSorting;
        }

        public QueryCountConfidence Confidence => _inner.Confidence;


        internal MemoizationMatch(in MemoizationMatchProvider<TInner> inner)
        {
            _inner = inner;
            _bufferCurrentIdx = 0;
        }

        public Span<long> FillAndRetrieve() => _inner.FillAndRetrieve();
        
        public int Fill(Span<long> matches)
        {
            if (_inner.ReplayCounter == 1)
            {
                //We tried to pass around memoized version of TInner but there is no need to persist it 
                return _inner.Fill(matches);
            }
            
            // If it has never be initialized, acquire all the data from inner. 
            // PERF: In case we need to improve performance, we can initialize lazily on demand. 
            var memoizedMatches = _inner.FillAndRetrieve().Slice(_bufferCurrentIdx);

            int toRead = Math.Min(matches.Length, memoizedMatches.Length);
            if (toRead == 0)
                return 0;

            // Copy the current memoized matches to the output. 
            memoizedMatches.Slice(0, toRead).CopyTo(matches);

            // Move the pointer so we dont read them again. 
            _bufferCurrentIdx += toRead;
            return toRead;
        }

        public int AndWith(Span<long> buffer, int matches)
        {
            var memoizedMatches = _inner.FillAndRetrieve();
            if (memoizedMatches.Length == 0)
                return 0;

            return MergeHelper.And(buffer, buffer.Slice(0, matches), memoizedMatches);
        }

        public void Score(Span<long> matches, Span<float> scores, float boostFactor)
        {
            _inner.Score(matches, scores, boostFactor);
        }
        string DebugView => Inspect().ToString();

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{nameof(MemoizationMatch)} [Memoization]",
                children: new List<QueryInspectionNode> { _inner.Inspect() },
                parameters: new Dictionary<string, string>()
                {
                    { nameof(IsBoosting), IsBoosting.ToString() },
                    { nameof(Count), $"{Count} [{Confidence}]" }
                });
        }
    }
}
