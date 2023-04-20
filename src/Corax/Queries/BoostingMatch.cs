using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using Corax.Mappings;

namespace Corax.Queries
{
    public struct BoostingComparer : IMatchComparer
    {
        public MatchCompareFieldType FieldType => MatchCompareFieldType.Score;

        public FieldMetadata Field => throw new NotSupportedException($"{nameof(Field)} is not supported for {nameof(BoostingComparer)}");

        public int CompareById(long idx, long idy)
        {
            throw new NotSupportedException($"{nameof(CompareById)} is not supported for {nameof(BoostingComparer)}");
        }
        
        public int CompareNumerical<T>(T sx, T sy) where T : unmanaged, INumber<T>
        {
            return sy.CompareTo(sx);
        }

        public int CompareSequence(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy)
        {
            throw new NotSupportedException($"{nameof(CompareSequence)} is not supported for {nameof(BoostingComparer)}");
        }
    }


    //We should set inner type via generic but since we don't do that in QueryBuilder (we use interfaces all the time) let's skip that. 
    //This should be fixed when we introduce something similar to IL ( RavenDB-19568)
    [DebuggerDisplay("{DebugView,nq}")]
    public unsafe struct BoostingMatch : IQueryMatch
    {
        internal IQueryMatch _inner;
        public float BoostFactor;
        public BoostingMatch(IndexSearcher searcher, in IQueryMatch inner, float boostFactor)
        {
            _inner = inner;
            BoostFactor = boostFactor;
        }

        public long Count => _inner.Count;

        public QueryCountConfidence Confidence => _inner.Confidence;

        public bool IsBoosting => true;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int AndWith(Span<long> buffer, int matches) => _inner.AndWith(buffer, matches);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Fill(Span<long> matches) => _inner.Fill(matches);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Score(Span<long> matches, Span<float> scores, float boostFactor) => _inner.Score(matches, scores, boostFactor * BoostFactor);

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{nameof(BoostingMatch)}",
                children: new List<QueryInspectionNode> { _inner.Inspect() },
                parameters: new Dictionary<string, string>()
                {
                    { nameof(BoostFactor), $"[{BoostFactor}]" }
                });
        }
    }
}
