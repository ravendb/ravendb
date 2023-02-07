using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Corax.Queries
{
    [DebuggerDisplay("{DebugView,nq}")]
    public unsafe struct AndNotMatch : IQueryMatch
    {
        private readonly FunctionTable _functionTable;
        private IQueryMatch _inner;

        internal AndNotMatch(IQueryMatch match, FunctionTable functionTable)
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Score(Span<long> matches, Span<float> scores, float boostFactor)
        {
            _inner.Score(matches, scores, boostFactor);
        }

        public QueryInspectionNode Inspect()
        {
            return _inner.Inspect();
        }

        internal class FunctionTable
        {
            public readonly delegate*<ref AndNotMatch, Span<long>, int> FillFunc;
            public readonly delegate*<ref AndNotMatch, Span<long>, int, int> AndWithFunc;
            public readonly delegate*<ref AndNotMatch, Span<long>, Span<float>, float, void> ScoreFunc;
            public readonly delegate*<ref AndNotMatch, long> CountFunc;

            public FunctionTable(
                delegate*<ref AndNotMatch, Span<long>, int> fillFunc,
                delegate*<ref AndNotMatch, Span<long>, int, int> andWithFunc,
                delegate*<ref AndNotMatch, Span<long>, Span<float>, float, void> scoreFunc,
                delegate*<ref AndNotMatch, long> countFunc)
            {
                FillFunc = fillFunc;
                AndWithFunc = andWithFunc;
                ScoreFunc = scoreFunc;
                CountFunc = countFunc;
            }
        }

        private static class StaticFunctionCache<TInner, TOuter>
            where TInner : IQueryMatch
            where TOuter : IQueryMatch
        {
            public static readonly FunctionTable FunctionTable;

            static StaticFunctionCache()
            {
                static long CountFunc(ref AndNotMatch match)
                {
                    return ((AndNotMatch<TInner, TOuter>)match._inner).Count;
                }

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                static int FillFunc(ref AndNotMatch match, Span<long> matches)
                {
                    if (match._inner is AndNotMatch<TInner, TOuter> inner)
                    {
                        var result = inner.Fill(matches);
                        match._inner = inner;
                        return result;
                    }
                    return 0;
                }

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                static int AndWithFunc(ref AndNotMatch match, Span<long> buffer, int matches)
                {
                    if (match._inner is AndNotMatch<TInner, TOuter> inner)
                    {
                        var result = inner.AndWith(buffer, matches);
                        match._inner = inner;
                        return result;
                    }
                    return 0;
                }

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                static void ScoreFunc(ref AndNotMatch match, Span<long> matches, Span<float> scores, float boostFactor)
                {
                    if (match._inner is AndNotMatch<TInner, TOuter> inner)
                    {
                        inner.Score(matches, scores, boostFactor);
                        match._inner = inner;
                    }
                }

                FunctionTable = new FunctionTable(&FillFunc, &AndWithFunc, &ScoreFunc, &CountFunc);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static AndNotMatch Create<TInner, TOuter>(in AndNotMatch<TInner, TOuter> query)
            where TInner : IQueryMatch
            where TOuter : IQueryMatch
        {
            return new AndNotMatch(query, StaticFunctionCache<TInner, TOuter>.FunctionTable);
        }

        string DebugView => Inspect().ToString();
    }
}
