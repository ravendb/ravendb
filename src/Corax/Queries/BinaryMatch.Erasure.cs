using System;
using System.Runtime.CompilerServices;


namespace Corax.Queries
{
    public unsafe struct BinaryMatch : IQueryMatch
    {
        private readonly FunctionTable _functionTable;
        private IQueryMatch _inner;

        internal BinaryMatch(IQueryMatch match, FunctionTable functionTable)
        {
            _inner = match;
            _functionTable = functionTable;
        }

        public long Count => _functionTable.CountFunc(ref this);

        public QueryCountConfidence Confidence => _inner.Confidence;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Fill(Span<long> buffer)
        {
            return _functionTable.FillFunc(ref this, buffer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int AndWith(Span<long> buffer)
        {
            return _functionTable.AndWithFunc(ref this, buffer);
        }

        internal class FunctionTable
        {
            public readonly delegate*<ref BinaryMatch, Span<long>, int> FillFunc;
            public readonly delegate*<ref BinaryMatch, Span<long>, int> AndWithFunc;
            public readonly delegate*<ref BinaryMatch, long> CountFunc;

            public FunctionTable(
                delegate*<ref BinaryMatch, Span<long>, int> fillFunc,
                delegate*<ref BinaryMatch, Span<long>, int> andWithFunc,
                delegate*<ref BinaryMatch, long> countFunc)
            {
                FillFunc = fillFunc;
                AndWithFunc = andWithFunc;
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
                static long CountFunc(ref BinaryMatch match)
                {
                    return ((BinaryMatch<TInner, TOuter>)match._inner).Count;
                }

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                static int FillFunc(ref BinaryMatch match, Span<long> matches)
                {
                    if (match._inner is BinaryMatch<TInner, TOuter> inner)
                    {
                        var result = inner.Fill(matches);
                        match._inner = inner;
                        return result;
                    }
                    return 0;
                }

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                static int AndWithFunc(ref BinaryMatch match, Span<long> matches)
                {
                    if (match._inner is BinaryMatch<TInner, TOuter> inner)
                    {
                        var result = inner.AndWith(matches);
                        match._inner = inner;
                        return result;
                    }
                    return 0;
                }

                FunctionTable = new FunctionTable(&FillFunc, &AndWithFunc, &CountFunc);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static BinaryMatch Create<TInner, TOuter>(in BinaryMatch<TInner, TOuter> query)
            where TInner : IQueryMatch
            where TOuter : IQueryMatch
        {
            return new BinaryMatch(query, StaticFunctionCache<TInner, TOuter>.FunctionTable);
        }
    }
}
