using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Corax.Queries
{
    [DebuggerDisplay("{DebugView,nq}")]
    public unsafe struct BoostingMatch : IQueryMatch
    {
        private readonly FunctionTable _functionTable;
        internal IQueryMatch _inner;

        internal BoostingMatch(IQueryMatch match, FunctionTable functionTable)
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
            Debug.Assert(matches.Length == scores.Length);

            _functionTable.ScoreFunc(ref this, matches, scores);
        }

        public QueryInspectionNode Inspect()
        {
            return _inner.Inspect();
        }
        internal class FunctionTable
        {
            public readonly delegate*<ref BoostingMatch, Span<long>, int> FillFunc;
            public readonly delegate*<ref BoostingMatch, Span<long>, int, int> AndWithFunc;
            public readonly delegate*<ref BoostingMatch, Span<long>, Span<float>, void> ScoreFunc;
            public readonly delegate*<ref BoostingMatch, long> CountFunc;


            public FunctionTable(
                delegate*<ref BoostingMatch, Span<long>, int> fillFunc,
                delegate*<ref BoostingMatch, Span<long>, int, int> andWithFunc,
                delegate*<ref BoostingMatch, Span<long>, Span<float>, void> scoreFunc,
                delegate*<ref BoostingMatch, long> countFunc)
            {
                FillFunc = fillFunc;
                AndWithFunc = andWithFunc;
                ScoreFunc = scoreFunc;
                CountFunc = countFunc;
            }
        }

        private static class StaticFunctionCache<TInner, TQueryScoreFunction>
            where TInner : IQueryMatch
            where TQueryScoreFunction : IQueryScoreFunction
        {
            public static readonly FunctionTable FunctionTable;

            static StaticFunctionCache()
            {
                static long CountFunc(ref BoostingMatch match)
                {
                    return ((BoostingMatch<TInner, TQueryScoreFunction>)match._inner).Count;
                }

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                static int FillFunc(ref BoostingMatch match, Span<long> matches)
                {
                    if (match._inner is BoostingMatch<TInner, TQueryScoreFunction> inner)
                    {
                        var result = inner.Fill(matches);
                        match._inner = inner;
                        return result;
                    }
                    return 0;
                }

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                static int AndWithFunc(ref BoostingMatch match, Span<long> buffer, int matches)
                {
                    if (match._inner is BoostingMatch<TInner, TQueryScoreFunction> inner)
                    {
                        var result = inner.AndWith(buffer, matches);
                        match._inner = inner;
                        return result;
                    }
                    return 0;
                }

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                static void ScoreFunc(ref BoostingMatch match, Span<long> matches, Span<float> scores)
                {
                    if (match._inner is BoostingMatch<TInner, TQueryScoreFunction> inner)
                    {
                        inner.Score(matches, scores);
                        match._inner = inner;
                    }
                }

                FunctionTable = new FunctionTable(&FillFunc, &AndWithFunc, &ScoreFunc, &CountFunc);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static BoostingMatch Create<TInner, TQueryScoreFunction>(in BoostingMatch<TInner, TQueryScoreFunction> query)
            where TInner : IQueryMatch
            where TQueryScoreFunction : IQueryScoreFunction
        {
            return new BoostingMatch(query, StaticFunctionCache<TInner, TQueryScoreFunction>.FunctionTable);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static BoostingMatch WithConstant<TInner>(IndexSearcher searcher, in TInner inner, float value)
            where TInner : IQueryMatch
        {
            return WithConstant(searcher, inner, new ConstantScoreFunction(value));
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static BoostingMatch WithConstant<TInner>(IndexSearcher searcher, in TInner inner, ConstantScoreFunction func)
            where TInner : IQueryMatch
        {
            static QueryInspectionNode InspectFunc(ref BoostingMatch<TInner, ConstantScoreFunction> match)
            {
                return new QueryInspectionNode($"{nameof(BoostingMatch)} [Constant]",
                        children: new List<QueryInspectionNode> { match._inner.Inspect() },
                        parameters: new Dictionary<string, string>()
                        {
                            { nameof(match.IsBoosting), match.IsBoosting.ToString() },
                            { nameof(match.Count), $"{match.Count} [{match.Confidence}]" },
                            { "Value", $"{match._scorer.Value}" }
                        });
            }

            return Create(new BoostingMatch<TInner, ConstantScoreFunction>(searcher, inner, func, 
                &BoostingMatch<TInner, ConstantScoreFunction>.ConstantScoreFunc,
                &InspectFunc));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static BoostingMatch WithTermFrequency<TInner>(IndexSearcher searcher, in TInner inner, TermFrequencyScoreFunction func = default(TermFrequencyScoreFunction))
            where TInner : IQueryMatch
        {
            static QueryInspectionNode InspectFunc(ref BoostingMatch<TInner, TermFrequencyScoreFunction> match)
            {
                return new QueryInspectionNode($"{nameof(BoostingMatch)} [TermFrequency]",
                            children: new List<QueryInspectionNode> { match._inner.Inspect() },
                            parameters: new Dictionary<string, string>()
                            {
                                { nameof(match.IsBoosting), match.IsBoosting.ToString() },
                                { nameof(match.Count), $"{match.Count} [{match.Confidence}]" }
                            });
            }

            return Create(new BoostingMatch<TInner, TermFrequencyScoreFunction>(searcher, inner, func, 
                &BoostingMatch<TInner, TermFrequencyScoreFunction>.TermFrequencyScoreFunc,
                &InspectFunc));
        }

        string DebugView => Inspect().ToString();
    }
}
