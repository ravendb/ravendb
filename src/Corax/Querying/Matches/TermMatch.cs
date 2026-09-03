using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using Sparrow.Compression;
using Sparrow.Server;
using Voron.Data.Containers;
using Voron.Data.PostingLists;
using Voron.Util.PFor;

namespace Corax.Querying.Matches
{
    public unsafe struct TermMatch : IQueryMatch
    {
        private readonly delegate*<ref TermMatch, Span<long>, int> _fillFunc;
        private readonly delegate*<ref TermMatch, Span<long>, Span<float>, float, void> _scoreFunc;
        private readonly delegate*<ref TermMatch, QueryInspectionNode> _inspectFunc;

        private bool _returnedValue;
        private readonly long _totalResults;
        private long _current;
        private Bm25Relevance _bm25Relevance;
        private PostingList.Iterator _set;
        private FastPForBufferedReader _containerReader;
        public bool IsBoosting => _scoreFunc != null;
        // Scoring reads what Fill saved only in the stored mode; bigger posting lists are re-read at score time.
        internal bool ScoringNeedsFill => _scoreFunc != null && _bm25Relevance is { IsStored: true };
        public long Count => _totalResults;

#if DEBUG
        public string Term;
#endif



        private TermMatch(long totalResults,
            delegate*<ref TermMatch, Span<long>, int> fillFunc,
            delegate*<ref TermMatch, Span<long>, Span<float>, float, void> scoreFunc = null,
            delegate*<ref TermMatch, QueryInspectionNode> inspectFunc = null)
        {
            _totalResults = totalResults;
            _current = QueryMatch.Start;
            _fillFunc = fillFunc;
            _scoreFunc = scoreFunc;
            _inspectFunc = inspectFunc;
            _set = default;
            _containerReader = default;
#if DEBUG
            Term = null;
#endif
        }

        public static TermMatch CreateEmpty()
        {
            static int FillFunc(ref TermMatch term, Span<long> matches)
            {
                term._current = QueryMatch.Invalid;
                return 0;
            }

            static QueryInspectionNode InspectFunc(ref TermMatch term)
            {
                return new QueryInspectionNode($"{nameof(TermMatch)} [Empty]",
                    parameters: new Dictionary<string, string>()
                    {
                        { Constants.QueryInspectionNode.IsBoosting, term.IsBoosting.ToString() },
                        { Constants.QueryInspectionNode.Count, term.Count.ToString() },
                    });
            }

            return new TermMatch(0, &FillFunc, inspectFunc: &InspectFunc)
            {
#if DEBUG
                Term = "<empty>"
#endif
            };
        }

        public static TermMatch YieldOnce(IndexSearcher indexSearcher, ByteStringContext ctx, long value, double termRatioToWholeCollection, bool isBoosting)
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int FillFunc(ref TermMatch term, Span<long> matches)
            {
                if (term._returnedValue == false)
                {
                    term._returnedValue = true;
                    matches[0] = term._current;
                    return 1;
                }

                return 0;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static void ScoreFunc(ref TermMatch term, Span<long> matches, Span<float> scores, float boostFactor)
            {
                using (term._bm25Relevance)
                    term._bm25Relevance.Score(matches, scores, boostFactor);
            }

            static QueryInspectionNode InspectFunc(ref TermMatch term)
            {
                return new QueryInspectionNode($"{nameof(TermMatch)} [Once]",
                    parameters: new Dictionary<string, string>()
                    {
                        { Constants.QueryInspectionNode.IsBoosting, term.IsBoosting.ToString() },
                        { Constants.QueryInspectionNode.Count, term.Count.ToString() },
                    });
            }

            Bm25Relevance bm25Relevance = null;
            long current = -1;
            if (isBoosting)
            {
                bm25Relevance = Bm25Relevance.Once(indexSearcher, 1, ctx, 1, termRatioToWholeCollection);
                current = bm25Relevance.Add(value);
            }

            return new TermMatch(1, &FillFunc, scoreFunc: isBoosting ? &ScoreFunc : null, inspectFunc: &InspectFunc)
            {
                _current = bm25Relevance is not null
                    ? current
                    : (long)EntryIdEncodings.DecodeAndDiscardFrequency(value),
                _bm25Relevance = bm25Relevance,
                _returnedValue = false
            };
        }

        public static TermMatch YieldSmall(IndexSearcher indexSearcher, ByteStringContext ctx, Container.Item containerItem, double termRatioToWholeCollection,
            bool isBoosting)
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int FillFunc<TBoostingMode>(ref TermMatch term, Span<long> matches) where TBoostingMode : IBoostingMarker
            {
                int results;
                fixed (long* pMatches = matches)
                {
                    results = term._containerReader.Fill(pMatches, matches.Length);
                }

                if (results == 0)
                {
                    term._containerReader.Dispose();
                    return 0;
                }

                //Save the frequencies
                if (typeof(TBoostingMode) == typeof(HasBoosting))
                {
                    if (term._bm25Relevance.IsStored)
                        term._bm25Relevance.Process(matches, results);
                    else
                        EntryIdEncodings.DecodeAndDiscardFrequency(matches, results);
                }
                else
                {
                    EntryIdEncodings.DecodeAndDiscardFrequency(matches, results);
                }

                return results;
            }

            static void ScoreFunc(ref TermMatch term, Span<long> matches, Span<float> scores, float boostFactor)
            {
                using (term._bm25Relevance)
                    term._bm25Relevance.Score(matches, scores, boostFactor);
            }

            static QueryInspectionNode InspectFunc(ref TermMatch term)
            {
                return new QueryInspectionNode($"{nameof(TermMatch)} [SmallSet]",
                    parameters: new Dictionary<string, string>()
                    {
                        { Constants.QueryInspectionNode.IsBoosting, term.IsBoosting.ToString() },
                        { Constants.QueryInspectionNode.Count, term.Count.ToString() },
                    });
            }

            var itemsCount = VariableSizeEncoding.Read<int>(containerItem.Address, out var offset);
            var reader = new FastPForBufferedReader(ctx, containerItem.Address + offset, containerItem.Length - offset);
            return new TermMatch(itemsCount, isBoosting ? &FillFunc<HasBoosting> : &FillFunc<NoBoosting>,
                inspectFunc: &InspectFunc, scoreFunc: isBoosting ? &ScoreFunc : null)
            {
                _bm25Relevance = isBoosting
                    ? Bm25Relevance.Small(indexSearcher, itemsCount, ctx, itemsCount, termRatioToWholeCollection)
                    : null,
                _current = 0,
                _containerReader = reader
            };
        }

        public static TermMatch YieldSet(IndexSearcher indexSearcher, ByteStringContext ctx, PostingList postingList, double termRatioToWholeCollection, bool isBoosting)
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int FillFunc<TBoostingMode>(ref TermMatch term, Span<long> matches) where TBoostingMode : IBoostingMarker
            {
                var set = term._set;

                set.Fill(matches, out int i);

                if (typeof(TBoostingMode) == typeof(HasBoosting))
                {
                    if (term._bm25Relevance.IsStored == false)
                        EntryIdEncodings.DecodeAndDiscardFrequency(matches, i);
                    else
                        term._bm25Relevance.Process(matches, i);
                }
                else
                    EntryIdEncodings.DecodeAndDiscardFrequency(matches, i);

                term._set = set;
                return i;
            }

            static QueryInspectionNode InspectFunc(ref TermMatch term)
            {
                return new QueryInspectionNode($"{nameof(TermMatch)} [Set]",
                    parameters: new Dictionary<string, string>()
                    {
                        { Constants.QueryInspectionNode.IsBoosting, term.IsBoosting.ToString() },
                        { Constants.QueryInspectionNode.Count, term.Count.ToString() },
                    });
            }

            static void ScoreFunc(ref TermMatch term, Span<long> matches, Span<float> scores, float boostFactor)
            {
                using (term._bm25Relevance)
                    term._bm25Relevance.Score(matches, scores, boostFactor);
            }

            var bm25Relevance = isBoosting
                ? Bm25Relevance.Set(indexSearcher, postingList.State.NumberOfEntries, ctx, (int)postingList.State.NumberOfEntries, termRatioToWholeCollection,
                    postingList)
                : null;

            var isStored = isBoosting && bm25Relevance.IsStored;

            return new TermMatch(postingList.State.NumberOfEntries,
                (isBoosting, isStored) switch
                {
                    (isBoosting: true, isStored: true) => &FillFunc<HasBoosting>,
                    (isBoosting: true, isStored: false) => &FillFunc<HasBoostingNoStore>,
                    (_, _) => &FillFunc<NoBoosting>
                },
                inspectFunc: &InspectFunc,
                scoreFunc: isBoosting ? &ScoreFunc : null) { _set = postingList.Iterate(), _current = long.MinValue, _bm25Relevance = bm25Relevance };
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Fill(Span<long> matches)
        {
            return _fillFunc(ref this, matches);
        }

        public void ScoreSorted(Span<long> matches, Span<float> scores, float boostFactor)
        {
            if (_scoreFunc == null)
                return; // not boosting — nothing to score

            using (_bm25Relevance)
                _bm25Relevance.ScoreSorted(matches, scores, boostFactor);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Score(Span<long> matches, Span<float> scores, float boostFactor)
        {
            if (_scoreFunc == null)
            {
                return; // We ignore. Nothing to do here.
            }

            _scoreFunc(ref this, matches, scores, boostFactor);
        }

        public QueryInspectionNode Inspect()
        {
            return _inspectFunc is null ? QueryInspectionNode.NotInitializedInspectionNode(nameof(TermMatch)) : _inspectFunc(ref this);
        }

        /// <summary>
        /// Expose the underlying PostingList-kind iterator for galloping page-scan.
        /// Returns false for Empty / Single / SmallPostingList cases where the
        /// existing Fill-loop path is already optimal. The caller takes ownership
        /// of iteration; do not call <see cref="Fill"/> on the same TermMatch afterward.
        /// </summary>
        internal bool TryGetPostingListIterator(out PostingList.Iterator iterator)
        {
            // Empty / Single / Small all leave _set at default (zeroed Iterator).
            // Only the Set path initializes _set and stamps _totalResults > 0;
            // _containerReader stays default for the Set path, so zero-init makes
            // the check below sufficient to disambiguate.
            iterator = _set;
            return _totalResults > 1 && _containerReader.IsValid == false;
        }
    }
}
