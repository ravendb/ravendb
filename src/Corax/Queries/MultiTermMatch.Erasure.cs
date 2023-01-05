using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow.Server;

namespace Corax.Queries
{
    [DebuggerDisplay("{DebugView,nq}")]
    public unsafe struct MultiTermMatch : IQueryMatch
    {
        private readonly FunctionTable _functionTable;
        private IQueryMatch _inner;

        internal MultiTermMatch(IQueryMatch match, FunctionTable functionTable)
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
        public void Score(Span<long> matches, Span<float> scores)
        {
            // We ignore. Nothing to do here. 
            _functionTable.ScoreFunc(ref this, matches, scores);
        }

        public QueryInspectionNode Inspect()
        {
            return _inner.Inspect();
        }

        string DebugView => Inspect().ToString();

        internal class FunctionTable
        {
            public readonly delegate*<ref MultiTermMatch, Span<long>, int> FillFunc;
            public readonly delegate*<ref MultiTermMatch, Span<long>, int, int> AndWithFunc;
            public readonly delegate*<ref MultiTermMatch, Span<long>, Span<float>, void> ScoreFunc;
            public readonly delegate*<ref MultiTermMatch, long> CountFunc;

            public FunctionTable(
                delegate*<ref MultiTermMatch, Span<long>, int> fillFunc,
                delegate*<ref MultiTermMatch, Span<long>, int, int> andWithFunc,
                delegate*<ref MultiTermMatch, Span<long>, Span<float>, void> scoreFunc,
                delegate*<ref MultiTermMatch, long> countFunc)
            {
                FillFunc = fillFunc;
                AndWithFunc = andWithFunc;
                ScoreFunc = scoreFunc;
                CountFunc = countFunc;
            }
        }

        private static class StaticFunctionCache<TTermMatch>
            where TTermMatch : IQueryMatch
        {
            public static readonly FunctionTable FunctionTable;

            static StaticFunctionCache()
            {
                static long CountFunc(ref MultiTermMatch match)
                {
                    if (match._inner is TTermMatch inner)
                        return inner.Count;

                    return match._inner.Count;
                }
                static int FillFunc(ref MultiTermMatch match, Span<long> matches)
                {
                    if (match._inner is TTermMatch inner)
                    {
                        var result = inner.Fill(matches);
                        match._inner = inner;
                        return result;
                    }
                    return 0;
                }

                static int AndWithFunc(ref MultiTermMatch match, Span<long> buffer, int matches)
                {
                    if (match._inner is TTermMatch inner)
                    {
                        var result = inner.AndWith(buffer, matches);
                        match._inner = inner;
                        return result;
                    }
                    return 0;
                }

                static void ScoreFunc(ref MultiTermMatch match, Span<long> matches, Span<float> scores)
                {
                    if (match._inner is TTermMatch inner)
                    {
                        inner.Score(matches, scores);
                        match._inner = inner;
                    }
                }

                FunctionTable = new FunctionTable(&FillFunc, &AndWithFunc, &ScoreFunc, &CountFunc);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static MultiTermMatch Create<TInner>(in MultiTermMatch<TInner> query)
             where TInner : ITermProvider
        {
            return new MultiTermMatch(query, StaticFunctionCache<MultiTermMatch<TInner>>.FunctionTable);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static MultiTermMatch Create(in BinaryMatch query)
        { 
            return new MultiTermMatch(query, StaticFunctionCache<BinaryMatch>.FunctionTable);
        }

        private struct EmptyTermProvider : ITermProvider
        {
            public int TermsCount => 0;

            public bool Next(out TermMatch term)
            {
                Unsafe.SkipInit(out term);
                return false;
            }

            public void Reset(){}

            public QueryInspectionNode Inspect()
            {
                return new QueryInspectionNode($"{nameof(EmptyTermProvider)}",
                    parameters: new Dictionary<string, string>()
                    {
                        { nameof(TermsCount), $"{TermsCount}" }
                    });
            }
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static MultiTermMatch CreateEmpty(ByteStringContext context)
        {
            return new MultiTermMatch(new MultiTermMatch<EmptyTermProvider>(default, context, new EmptyTermProvider()), StaticFunctionCache<MultiTermMatch<EmptyTermProvider>>.FunctionTable);
        }
    }
}
