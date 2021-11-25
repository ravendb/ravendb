using System;
using System.Runtime.CompilerServices;

namespace Corax.Queries
{
    public unsafe partial struct BinaryMatch<TInner, TOuter> : IQueryMatch
        where TInner : IQueryMatch
        where TOuter : IQueryMatch
    {
        private readonly delegate*<ref BinaryMatch<TInner, TOuter>, Span<long>, int>  _fillFunc;
        private readonly delegate*<ref BinaryMatch<TInner, TOuter>, Span<long>, int> _andWith;
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
            delegate*<ref BinaryMatch<TInner, TOuter>, Span<long>, int> andWith,
            long totalResults,
            QueryCountConfidence confidence)
        {
            _totalResults = totalResults;
            _current = QueryMatch.Start;
            _fillFunc = fillFunc;
            _andWith = andWith;
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
        public int AndWith(Span<long> buffer)
        {
            return _andWith(ref this, buffer);
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
                
                // TODO: We are not going to use stackalloc here.
                Span<float> outerScores = stackalloc float[scores.Length];
                outerScores.Fill(1); // We will fill the scores with 1.0

                // We get the score for the outer chain.
                _outer.Score(matches, outerScores);

                // We multiply the scores from the outer chain with the current scores and return.
                for(int i = 0; i < scores.Length; i++)
                    scores[i] *= outerScores[i];
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
    }
}
