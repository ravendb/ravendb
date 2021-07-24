using System;
using System.Runtime.CompilerServices;


namespace Corax.Queries
{
    public unsafe struct BinaryMatch<TInner, TOuter> : IQueryMatch
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
        public static BinaryMatch<TInner, TOuter> YieldAnd(in TInner inner, in TOuter outer)
        {
            static int FillFunc(ref BinaryMatch<TInner, TOuter> match, Span<long> matches)
            {
                ref var inner = ref match._inner;
                ref var outer = ref match._outer;

                while (true)
                {
                    var results = inner.Fill(matches);
                    if (results == 0)
                        return 0;
                    
                    results = outer.AndWith(matches.Slice(0, results));
                    if (results != 0)
                        return results;
                }
            }

            static int AndWith(ref BinaryMatch<TInner, TOuter> match, Span<long> matches)
            {
                ref var inner = ref match._inner;
                ref var outer = ref match._outer;

                var results = inner.AndWith(matches);

                return outer.AndWith(matches.Slice(0, results));
            }

            // Estimate Confidence values.
            QueryCountConfidence confidence;
            if (inner.Count < outer.Count / 2)
                confidence = inner.Confidence;
            else if (outer.Count < inner.Count / 2)
                confidence = outer.Confidence;
            else
                confidence = inner.Confidence.Min(outer.Confidence);

            return new BinaryMatch<TInner, TOuter>(in inner, in outer, &FillFunc, &AndWith, Math.Min(inner.Count, outer.Count), confidence);
        }

        public static BinaryMatch<TInner, TOuter> YieldOr(in TInner inner, in TOuter outer)
        {
            static int AndWith(ref BinaryMatch<TInner, TOuter> match, Span<long> matches)
            {
                Span<long> orMatches = stackalloc long[matches.Length];
                var count = FillFunc(ref match, orMatches);

                return MergeHelper.And(matches, matches, orMatches.Slice(0, count));
            }

            [SkipLocalsInit]
            static int FillFunc(ref BinaryMatch<TInner, TOuter> match, Span<long> matches)
            {
                ref var inner = ref match._inner;
                ref var outer = ref match._outer;

                // need to be ready to put both outputs to the matches
                Span<long> innerMatches = stackalloc long[matches.Length / 2];
                Span<long> outerMatches = stackalloc long[matches.Length / 2];

                var innerCount = inner.Fill(innerMatches);
                var outerCount = outer.Fill(outerMatches);

                if (innerCount == 0)
                {
                    outerMatches.Slice(0, outerCount).CopyTo(matches);
                    return outerCount;
                }
                if (outerCount == 0)
                {
                    innerMatches.Slice(0, innerCount).CopyTo(matches);
                    return innerCount;
                }

                int innerIdx = 0, outerIdx = 0, matchesIdx = 0;
                while (innerIdx < innerCount && outerIdx < outerCount)
                {
                    if (innerMatches[innerIdx] == outerMatches[outerIdx])
                    {
                        matches[matchesIdx++] = innerMatches[innerIdx++];
                        outerIdx++;
                    }
                    else if (innerMatches[innerIdx] < outerMatches[outerIdx])
                    {
                        matches[matchesIdx++] = innerMatches[innerIdx++];
                    }
                    else
                    {
                        matches[matchesIdx++] = outerMatches[outerIdx++];
                    }
                }

                while (innerIdx < innerCount)
                {
                    matches[matchesIdx++] = innerMatches[innerIdx++];
                }
                while (outerIdx < outerCount)
                {
                    matches[matchesIdx++] = outerMatches[outerIdx++];
                }
                return matchesIdx;
            }

            // Estimate Confidence values.
            QueryCountConfidence confidence;
            if (inner.Count / 10 > outer.Count)
                confidence = inner.Confidence;
            else if (outer.Count / 10 > inner.Count)
                confidence = outer.Confidence;
            else
                confidence = inner.Confidence.Min(outer.Confidence);

            return new BinaryMatch<TInner, TOuter>(in inner, in outer, &FillFunc, &AndWith, inner.Count + outer.Count, confidence);
        }
    }
}
