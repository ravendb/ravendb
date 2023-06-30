using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Corax.Queries.SortingMatches.Meta;
using Corax.Utils.Spatial;

namespace Corax.Queries.SortingMatches
{
    [DebuggerDisplay("{DebugView,nq}")]
    public unsafe partial struct SortingMatch : IQueryMatch
    {
        private readonly FunctionTable _functionTable;
        private IQueryMatch _inner;

        private SortingMatch(IQueryMatch match, FunctionTable functionTable)
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
            throw new NotSupportedException($"{nameof(SortingMatch)} does not support the operation of {nameof(AndWith)}.");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetScoreAndDistanceBuffer(in SortingDataTransfer sortingDataTransfer) => _functionTable.SetSortingDataTransfer(ref this, sortingDataTransfer);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Score(Span<long> matches, Span<float> scores, float boostFactor)
        {
            _inner.Score(matches, scores, boostFactor);
        }

        internal class FunctionTable(delegate*<ref SortingMatch, Span<long>, int> fillFunc,
            delegate*<ref SortingMatch, long> totalResultsFunc,
            delegate*<ref SortingMatch, in SortingDataTransfer, void> setSortingDataTransferFunc)
        {
            public readonly delegate*<ref SortingMatch, Span<long>, int> FillFunc = fillFunc;
            public readonly delegate*<ref SortingMatch, long> TotalResultsFunc = totalResultsFunc;
            public readonly delegate*<ref SortingMatch, in SortingDataTransfer, void> SetSortingDataTransfer = setSortingDataTransferFunc;
        }

        private static class StaticFunctionCache<TInner>
            where TInner : IQueryMatch
        {
            public static readonly FunctionTable FunctionTable;

            static StaticFunctionCache()
            {
                static long CountFunc(ref SortingMatch match)
                {
                    return ((SortingMatch<TInner>)match._inner).TotalResults;
                }
                static int FillFunc(ref SortingMatch match, Span<long> matches)
                {
                    if (match._inner is SortingMatch<TInner> inner)
                    {
                        var result = inner.Fill(matches);
                        match._inner = inner;
                        return result;
                    }
                    return 0;
                }

                static void SetScoreBufferFunc(ref SortingMatch match, in SortingDataTransfer sortingDataTransfer)
                {
                    if (match._inner is SortingMatch<TInner> inner)
                    {
                        inner.SetSortingDataTransfer(sortingDataTransfer);
                        match._inner = inner;
                    }
                }

                FunctionTable = new FunctionTable(&FillFunc, &CountFunc, &SetScoreBufferFunc);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static SortingMatch Create<TInner>(in SortingMatch<TInner> query)
            where TInner : IQueryMatch
        {
            return new SortingMatch(query, StaticFunctionCache<TInner>.FunctionTable);
        }

        public QueryInspectionNode Inspect()
        {
            return _inner.Inspect();
        }

        string DebugView => Inspect().ToString();
    }
}
