using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Corax.Queries
{
    [DebuggerDisplay("{DebugView,nq}")]
    public unsafe partial struct SortingMatch : IQueryMatch
    {
        private readonly FunctionTable _functionTable;
        private IQueryMatch _inner;

        internal SortingMatch(IQueryMatch match, FunctionTable functionTable)
        {
            _inner = match;
            _functionTable = functionTable;
        }

        public long TotalResults => _functionTable.TotalResultsFunc(ref this);

        public long Count => throw new NotSupportedException();

        public QueryCountConfidence Confidence => throw new NotSupportedException();

        public bool IsBoosting => _inner.IsBoosting;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Fill(Span<long> buffer)
        {
            return _functionTable.FillFunc(ref this, buffer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int AndWith(Span<long> buffer, int matches)
        {
            throw new NotSupportedException($"{nameof(SortingMatch)} does not support the operation of {nameof(AndWith)}.");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Score(Span<long> matches, Span<float> scores, float boostFactor)
        {
            _inner.Score(matches, scores, boostFactor);
        }

        internal class FunctionTable
        {
            public readonly delegate*<ref SortingMatch, Span<long>, int> FillFunc;
            public readonly delegate*<ref SortingMatch, long> TotalResultsFunc;

            public FunctionTable(
                delegate*<ref SortingMatch, Span<long>, int> fillFunc,
                delegate*<ref SortingMatch, long> totalResultsFunc)
            {
                FillFunc = fillFunc;
                TotalResultsFunc = totalResultsFunc;
            }
        }

        private static class StaticFunctionCache<TInner, TComparer>
            where TInner : IQueryMatch
            where TComparer : struct, IMatchComparer
        {
            public static readonly FunctionTable FunctionTable;

            static StaticFunctionCache()
            {
                static long CountFunc(ref SortingMatch match)
                {
                    return ((SortingMatch<TInner, TComparer>)match._inner).TotalResults;
                }
                static int FillFunc(ref SortingMatch match, Span<long> matches)
                {
                    if (match._inner is SortingMatch<TInner, TComparer> inner)
                    {
                        var result = inner.Fill(matches);
                        match._inner = inner;
                        return result;
                    }
                    return 0;
                }

                FunctionTable = new FunctionTable(&FillFunc, &CountFunc);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static SortingMatch Create<TInner, TComparer>(in SortingMatch<TInner, TComparer> query)
            where TInner : IQueryMatch
            where TComparer : struct, IMatchComparer
        {
            return new SortingMatch(query, StaticFunctionCache<TInner, TComparer>.FunctionTable);
        }

        public QueryInspectionNode Inspect()
        {
            return _inner.Inspect();
        }

        string DebugView => Inspect().ToString();
    }
}
