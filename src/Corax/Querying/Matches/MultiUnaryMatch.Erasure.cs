using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Corax.Querying.Matches.Meta;

namespace Corax.Querying.Matches
{
    [DebuggerDisplay("{DebugView,nq}")]
    public unsafe struct MultiUnaryMatch : IQueryMatch
    {
        private readonly FunctionTable _functionTable;
        private IQueryMatch _inner;
        private CancellationToken _token;
        internal MultiUnaryMatch(IQueryMatch match, FunctionTable functionTable, CancellationToken token)
        {
            _inner = match;
            _functionTable = functionTable;
            _token = token;
        }

        public bool IsBoosting => _inner.IsBoosting;

        public long Count => _functionTable.CountFunc(ref this);

        public SkipSortingResult AttemptToSkipSorting() => _inner.AttemptToSkipSorting();

        public QueryCountConfidence Confidence => _inner.Confidence;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Fill(Span<long> buffer)
        {
            _token.ThrowIfCancellationRequested();
            return _functionTable.FillFunc(ref this, buffer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int AndWith(Span<long> buffer, int matches)
        {
            _token.ThrowIfCancellationRequested();
            return _functionTable.AndWithFunc(ref this, buffer, matches);
        }

        internal sealed class FunctionTable
        {
            public readonly delegate*<ref MultiUnaryMatch, Span<long>, int> FillFunc;
            public readonly delegate*<ref MultiUnaryMatch, Span<long>, int, int> AndWithFunc;
            public readonly delegate*<ref MultiUnaryMatch, long> CountFunc;

            public FunctionTable(
                delegate*<ref MultiUnaryMatch, Span<long>, int> fillFunc,
                delegate*<ref MultiUnaryMatch, Span<long>, int, int> andWithFunc,
                delegate*<ref MultiUnaryMatch, long> countFunc)
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
                static long CountFunc(ref MultiUnaryMatch match)
                {
                    return ((MultiUnaryMatch<TInner>)match._inner).Count;
                }

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                static int FillFunc(ref MultiUnaryMatch match, Span<long> matches)
                {
                    if (match._inner is MultiUnaryMatch<TInner> inner)
                    {
                        var result = inner.Fill(matches);
                        match._inner = inner;
                        return result;
                    }
                    return 0;
                }

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                static int AndWithFunc(ref MultiUnaryMatch match, Span<long> buffer, int matches)
                {
                    if (match._inner is MultiUnaryMatch<TInner> inner)
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
        public static MultiUnaryMatch Create<TInner>(in MultiUnaryMatch<TInner> query, CancellationToken token)
            where TInner : IQueryMatch
        {
            return new MultiUnaryMatch(query, StaticFunctionCache<TInner>.FunctionTable, token);
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
