using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Corax.Queries.SortingMatches
{
    [DebuggerDisplay("{DebugView,nq}")]
    public unsafe partial struct NewMultiSortingMatch : IQueryMatch
    {
        private readonly FunctionTable _functionTable;
        private IQueryMatch _inner;

        internal NewMultiSortingMatch(IQueryMatch match, FunctionTable functionTable)
        {
            _inner = match;
            _functionTable = functionTable;
        }

        public long TotalResults => _functionTable.TotalResultsFunc(ref this);

        public long Count => throw new NotSupportedException();

        public bool DoNotSortResults() => _inner.DoNotSortResults();

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
            throw new NotSupportedException($"{nameof(NewMultiSortingMatch)} does not support the operation of {nameof(AndWith)}.");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Score(Span<long> matches, Span<float> scores, float boostFactor)
        {
            _inner.Score(matches, scores, boostFactor);
        }

        internal class FunctionTable
        {
            public readonly delegate*<ref NewMultiSortingMatch, Span<long>, int> FillFunc;
            public readonly delegate*<ref NewMultiSortingMatch, long> TotalResultsFunc;

            public FunctionTable(
                delegate*<ref NewMultiSortingMatch, Span<long>, int> fillFunc,
                delegate*<ref NewMultiSortingMatch, long> totalResultsFunc)
            {
                FillFunc = fillFunc;
                TotalResultsFunc = totalResultsFunc;
            }
        }

        private static class StaticFunctionCache<TInner>
            where TInner : IQueryMatch
        {
            public static readonly FunctionTable FunctionTable;

            static StaticFunctionCache()
            {
                static long CountFunc(ref NewMultiSortingMatch match)
                {
                    return ((NewMultiSortingMatch<TInner>)match._inner).TotalResults;
                }
                static int FillFunc(ref NewMultiSortingMatch match, Span<long> matches)
                {
                    if (match._inner is NewMultiSortingMatch<TInner> inner)
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
        public static NewMultiSortingMatch Create<TInner>(in NewMultiSortingMatch<TInner> query)
            where TInner : IQueryMatch
        {
            return new NewMultiSortingMatch(query, StaticFunctionCache<TInner>.FunctionTable);
        }

        public QueryInspectionNode Inspect()
        {
            return _inner.Inspect();
        }

        string DebugView => Inspect().ToString();
    }
}
