using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Corax.Queries.SortingMatches.Meta;
using Corax.Utils.Spatial;

namespace Corax.Queries.SortingMatches
{
    [DebuggerDisplay("{DebugView,nq}")]
    public unsafe partial struct SortingMultiMatch : IQueryMatch
    {
        private readonly FunctionTable _functionTable;
        private IQueryMatch _inner;

        internal SortingMultiMatch(IQueryMatch match, FunctionTable functionTable)
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
            throw new NotSupportedException($"{nameof(SortingMultiMatch)} does not support the operation of {nameof(AndWith)}.");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Score(Span<long> matches, Span<float> scores, float boostFactor)
        {
            _inner.Score(matches, scores, boostFactor);
        }

        internal class FunctionTable(delegate*<ref SortingMultiMatch, Span<long>, int> fillFunc,
            delegate*<ref SortingMultiMatch, long> totalResultsFunc,
            delegate*<ref SortingMultiMatch,  in SortingDataTransfer, void> setSortingDataTransfer)
        {
            public readonly delegate*<ref SortingMultiMatch, Span<long>, int> FillFunc = fillFunc;
            public readonly delegate*<ref SortingMultiMatch, long> TotalResultsFunc = totalResultsFunc;
            public readonly delegate*<ref SortingMultiMatch, in SortingDataTransfer, void> SetSortingDataTransferFunc = setSortingDataTransfer;
        }

        private static class StaticFunctionCache<TInner>
            where TInner : IQueryMatch
        {
            public static readonly FunctionTable FunctionTable;

            static StaticFunctionCache()
            {
                static long CountFunc(ref SortingMultiMatch match)
                {
                    return ((SortingMultiMatch<TInner>)match._inner).TotalResults;
                }
                static int FillFunc(ref SortingMultiMatch match, Span<long> matches)
                {
                    if (match._inner is SortingMultiMatch<TInner> inner)
                    {
                        var result = inner.Fill(matches);
                        match._inner = inner;
                        return result;
                    }
                    return 0;
                }

                static void SetScoreBufferFunc(ref SortingMultiMatch match, in SortingDataTransfer sortingDataTransfer)
                {
                    if (match._inner is SortingMultiMatch<TInner> inner)
                    {
                        inner.SetSortingDataTransfer(sortingDataTransfer);
                        match._inner = inner;
                    }
                }

                FunctionTable = new FunctionTable(&FillFunc, &CountFunc, &SetScoreBufferFunc);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetSortingDataTransfer(in SortingDataTransfer sortingDataTransfer) => _functionTable.SetSortingDataTransferFunc(ref this, sortingDataTransfer);
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static SortingMultiMatch Create<TInner>(in SortingMultiMatch<TInner> query)
            where TInner : IQueryMatch
        {
            return new SortingMultiMatch(query, StaticFunctionCache<TInner>.FunctionTable);
        }

        public QueryInspectionNode Inspect()
        {
            return _inner.Inspect();
        }

        string DebugView => Inspect().ToString();
    }
}
