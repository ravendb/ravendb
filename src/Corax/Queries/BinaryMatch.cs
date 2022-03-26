using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Corax.Queries
{
    [DebuggerDisplay("{DebugView,nq}")]
    public unsafe partial struct BinaryMatch<TInner, TOuter> : IQueryMatch
        where TInner : IQueryMatch
        where TOuter : IQueryMatch
    {
        private readonly delegate*<ref BinaryMatch<TInner, TOuter>, Span<long>, int>  _fillFunc;
        private readonly delegate*<ref BinaryMatch<TInner, TOuter>, Span<long>, int, int> _andWithFunc;
        private readonly delegate*<ref BinaryMatch<TInner, TOuter>, QueryInspectionNode> _inspectFunc;

        private TInner _inner;
        private TOuter _outer;

        private long _totalResults;
        private long _current;
        private QueryCountConfidence _confidence;

        public bool IsBoosting => _inner.IsBoosting || _outer.IsBoosting;
        public long Count => _totalResults;
        public long Current => _current;

        public QueryCountConfidence Confidence => _confidence;

        private BinaryMatch(in TInner inner, in TOuter outer,
            delegate*<ref BinaryMatch<TInner, TOuter>, Span<long>, int> fillFunc,
            delegate*<ref BinaryMatch<TInner, TOuter>, Span<long>, int, int> andWithFunc,
            delegate*<ref BinaryMatch<TInner, TOuter>, QueryInspectionNode> inspectionFunc,
            long totalResults,
            QueryCountConfidence confidence)
        {
            _totalResults = totalResults;
            _current = QueryMatch.Start;

            _fillFunc = fillFunc;
            _andWithFunc = andWithFunc;
            _inspectFunc = inspectionFunc;

            _inner = inner;
            _outer = outer;
            _confidence = confidence;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Fill(Span<long> buffer)
        {
            return _fillFunc(ref this, buffer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int AndWith(Span<long> buffer, int matches)
        {
            return _andWithFunc(ref this, buffer, matches);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Score(Span<long> matches, Span<float> scores)
        {
            // Nothing to do if there is no boosting happening at this level. 
            bool innerBoosting = _inner.IsBoosting;
            bool outerBoosting = _outer.IsBoosting;
            if (innerBoosting == false && outerBoosting == false)
                return;
            
            // From now on we have boosting happening somewhere in this chain. 

            // If there are two chains we need to combine them.
            if (innerBoosting == true && outerBoosting == true)
            {
                _inner.Score(matches, scores);

                var bufferHolder = QueryContext.MatchesRawPool.Rent(sizeof(float) * scores.Length);
                var outerScores = MemoryMarshal.Cast<byte, float>(bufferHolder).Slice(0, scores.Length);

                outerScores.Fill(1); // We will fill the scores with 1.0

                // We get the score for the outer chain.
                _outer.Score(matches, outerScores);

                // We multiply the scores from the outer chain with the current scores and return.
                for(int i = 0; i < scores.Length; i++)
                    scores[i] *= outerScores[i];

                QueryContext.MatchesRawPool.Return(bufferHolder);

                return;
            }

            // From now on, only a single requires score calculations. 

            if (innerBoosting == true)
            {
                // Inner can still be not boosting. In this case it is, so we delegate
                // the call into the boosting layer that provides us the information.
                _inner.Score(matches, scores);
                return;
            }

            if (outerBoosting == true)
            {
                // Outer can still be not boosting. In this case it is, so we delegate
                // the call into the boosting layer that provides us the information.
                _outer.Score(matches, scores);
            }
        }

        public QueryInspectionNode Inspect()
        {
            return _inspectFunc is null ? QueryInspectionNode.NotInitializedInspectionNode(nameof(BinaryMatch)) : _inspectFunc(ref this);
        }

        string DebugView => Inspect().ToString();
    }
}
