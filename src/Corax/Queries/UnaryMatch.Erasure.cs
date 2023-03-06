using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;


namespace Corax.Queries
{
    [DebuggerDisplay("{DebugView,nq}")]
    public unsafe struct UnaryMatch : IQueryMatch
    {
        private readonly FunctionTable _functionTable;
        private IQueryMatch _inner;

        internal UnaryMatch(IQueryMatch match, FunctionTable functionTable)
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
            public readonly delegate*<ref UnaryMatch, Span<long>, int> FillFunc;
            public readonly delegate*<ref UnaryMatch, Span<long>, int, int> AndWithFunc;
            public readonly delegate*<ref UnaryMatch, long> CountFunc;

            public FunctionTable(
                delegate*<ref UnaryMatch, Span<long>, int> fillFunc,
                delegate*<ref UnaryMatch, Span<long>, int, int> andWithFunc,
                delegate*<ref UnaryMatch, long> countFunc)
            {
                FillFunc = fillFunc;
                AndWithFunc = andWithFunc;
                CountFunc = countFunc;
            }
        }

        private static class StaticFunctionCache<TInner, TValueType>
            where TInner : IQueryMatch
        {
            public static readonly FunctionTable FunctionTable;

            static StaticFunctionCache()
            {
                static long CountFunc(ref UnaryMatch match)
                {
                    return ((UnaryMatch<TInner, TValueType>)match._inner).Count;
                }

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                static int FillFunc(ref UnaryMatch match, Span<long> matches)
                {
                    if (match._inner is UnaryMatch<TInner, TValueType> inner)
                    {
                        var result = inner.Fill(matches);
                        match._inner = inner;
                        return result;
                    }
                    return 0;
                }

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                static int AndWithFunc(ref UnaryMatch match, Span<long> buffer, int matches)
                {
                    if (match._inner is UnaryMatch<TInner, TValueType> inner)
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
        public static UnaryMatch Create<TInner, TValueType>(in UnaryMatch<TInner, TValueType> query)
            where TInner : IQueryMatch
        {
            return new UnaryMatch(query, StaticFunctionCache<TInner, TValueType>.FunctionTable);
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

        string DebugView => Inspect().ToString();
    }
}
