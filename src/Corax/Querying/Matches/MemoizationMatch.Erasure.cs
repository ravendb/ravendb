﻿using System;
using System.Runtime.CompilerServices;
using Corax.Querying.Matches.Meta;

namespace Corax.Querying.Matches
{
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

        public SkipSortingResult AttemptToSkipSorting() => _inner.AttemptToSkipSorting();

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
        public Span<long> FillAndRetrieve()
        {
#pragma warning disable CS9084 // Struct member returns 'this' or other instance members by reference
            return _functionTable.FillAndRetrieveFunc(ref this);
#pragma warning restore CS9084 // Struct member returns 'this' or other instance members by reference
        }

        internal sealed class FunctionTable
        {
            public readonly delegate*<ref MemoizationMatch, Span<long>, int> FillFunc;
            public readonly delegate*<ref MemoizationMatch, Span<long>, int, int> AndWithFunc;
            public readonly delegate*<ref MemoizationMatch, long> CountFunc;
            public readonly delegate*<ref MemoizationMatch, Span<long>> FillAndRetrieveFunc;


            public FunctionTable(
                delegate*<ref MemoizationMatch, Span<long>, int> fillFunc,
                delegate*<ref MemoizationMatch, Span<long>, int, int> andWithFunc,
                delegate*<ref MemoizationMatch, long> countFunc,
                delegate*<ref MemoizationMatch, Span<long>> fillAndRetrieveFunc)
            {
                FillFunc = fillFunc;
                AndWithFunc = andWithFunc;
                CountFunc = countFunc;
                FillAndRetrieveFunc = fillAndRetrieveFunc;
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
                static Span<long> FillAndRetrieveFunc(ref MemoizationMatch match)
                {
                    if (match._inner is MemoizationMatch<TInner> inner)
                    {
                        var result =  inner.FillAndRetrieve();
                        match._inner = inner;
                        return result;
                    }

                    return default;
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

                FunctionTable = new FunctionTable(&FillFunc, &AndWithFunc, &CountFunc, &FillAndRetrieveFunc);
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
