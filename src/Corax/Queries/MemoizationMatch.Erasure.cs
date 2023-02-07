using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Corax.Queries
{
    [DebuggerDisplay("{DebugView,nq}")]
    public unsafe struct MemoizationMatch : IQueryMatch
    {
        private readonly FunctionTable _functionTable;
        private IQueryMatch _inner;

        internal MemoizationMatch(IQueryMatch match, FunctionTable functionTable)
        {
            _inner = match;
            _functionTable = functionTable;
        }

        public bool IsBoosting => _inner.IsBoosting;

        public long Count => _functionTable.CountFunc(ref this);

        public QueryCountConfidence Confidence => _inner.Confidence;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Fill(Span<long> buffer)
        {
            return _functionTable.FillFunc(ref this, buffer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int AndWith(Span<long> buffer, int matches)
        {
            return _functionTable.AndWithFunc(ref this, buffer, matches);
        }

        internal class FunctionTable
        {
            public readonly delegate*<ref MemoizationMatch, Span<long>, int> FillFunc;
            public readonly delegate*<ref MemoizationMatch, Span<long>, int, int> AndWithFunc;
            public readonly delegate*<ref MemoizationMatch, long> CountFunc;

            public FunctionTable(
                delegate*<ref MemoizationMatch, Span<long>, int> fillFunc,
                delegate*<ref MemoizationMatch, Span<long>, int, int> andWithFunc,
                delegate*<ref MemoizationMatch, long> countFunc)
            {
                FillFunc = fillFunc;
                AndWithFunc = andWithFunc;
                CountFunc = countFunc;
            }
        }

        private static class StaticFunctionCache<TInner>
            where TInner : IQueryMatch
        {
            public static readonly FunctionTable FunctionTable;

            static StaticFunctionCache()
            {
                static long CountFunc(ref MemoizationMatch match)
                {
                    return ((MemoizationMatch<TInner>)match._inner).Count;
                }

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                static int FillFunc(ref MemoizationMatch match, Span<long> matches)
                {
                    if (match._inner is MemoizationMatch<TInner> inner)
                    {
                        var result = inner.Fill(matches);
                        match._inner = inner;
                        return result;
                    }
                    return 0;
                }

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                static int AndWithFunc(ref MemoizationMatch match, Span<long> buffer, int matches)
                {
                    if (match._inner is MemoizationMatch<TInner> inner)
                    {
                        var result = inner.AndWith(buffer, matches);
                        match._inner = inner;
                        return result;
                    }
                    return 0;
                }

                FunctionTable = new FunctionTable(&FillFunc, &AndWithFunc, &CountFunc);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static MemoizationMatch Create<TInner>(in MemoizationMatch<TInner> query)
            where TInner : IQueryMatch
        {
            return new MemoizationMatch(query, StaticFunctionCache<TInner>.FunctionTable);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Score(Span<long> matches, Span<float> scores, float boostFactor)
        {
            _inner.Score(matches, scores, boostFactor);
        }

        public QueryInspectionNode Inspect()
        {
            return _inner.Inspect();
        }
    }
}
