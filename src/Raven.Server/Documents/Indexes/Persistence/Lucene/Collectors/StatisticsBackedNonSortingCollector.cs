using System;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Lucene.Net.Util;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Collectors
{
    public sealed class StatisticsBackedNonSortingCollector : Collector
    {
        private readonly int _numberOfDocsToCollect;
        private readonly ManagedScoreDocArray _scoreDocArray;

        private int _totalHits;
        private int _totalHitsBeforeCurrentSegment;
        private float _maxScore;
        private int _docBase;
        private bool _stopWhenPageIsFull;

        private Scorer _scorer;

        private StatisticsBackedNonSortingCollector(int numberOfDocsToCollect)
        {
            _scoreDocArray = new ManagedScoreDocArray();
            _numberOfDocsToCollect = numberOfDocsToCollect;
        }

        public static bool TryExecute(IndexSearcher searcher, Query query, int numberOfDocsToCollect, IState state, out TopDocs result)
        {
            result = null;

            Term term;
            switch (query)
            {
                case MatchAllDocsQuery:
                    term = null;
                    break;
                case TermQuery termQuery:
                    term = termQuery.Term;
                    break;
                default:
                    return false;
            }

            var reader = searcher.IndexReader;
            var segments = reader.GetSequentialSubReaders() ?? new[] { reader };
            var collector = new StatisticsBackedNonSortingCollector(numberOfDocsToCollect);
            Weight weight = null;
            var docBase = 0;

            foreach (var segment in segments)
            {
                // a term's document frequency counts the documents deleted since, the live document count does not
                var segmentIsCountable = term == null || segment.HasDeletions == false;

                if (collector.PageIsFull && segmentIsCountable)
                {
                    collector._totalHits += CountFromStatistics(segment, term, state);
                }
                else
                {
                    weight ??= query.Weight(searcher, state);
                    collector.SetNextReader(segment, docBase, state);
                    collector._stopWhenPageIsFull = segmentIsCountable;

                    var scorer = weight.Scorer(segment, scoreDocsInOrder: false, topScorer: true, state);
                    if (scorer != null)
                    {
                        try
                        {
                            scorer.Score(collector, state);
                        }
                        catch (PageFilledException)
                        {
                            var hitsWalkedInSegment = collector._totalHits - collector._totalHitsBeforeCurrentSegment;
                            collector._totalHits += CountFromStatistics(segment, term, state) - hitsWalkedInSegment;
                        }
                    }
                }

                docBase += segment.MaxDoc;
            }

            result = collector.ToTopDocs();
            return true;
        }

        private static int CountFromStatistics(IndexReader segment, Term term, IState state)
        {
            return term == null ? segment.NumDocs() : segment.DocFreq(term, state);
        }

        private bool PageIsFull => _scoreDocArray.Length >= _numberOfDocsToCollect;

        public override void SetScorer(Scorer scorer)
        {
            _scorer = scorer;
        }

        public override void Collect(int doc, IState state)
        {
            _totalHits++;

            if (_scoreDocArray.Length < _numberOfDocsToCollect)
            {
                var score = _scorer?.Score(state) ?? 0;
                if (score > _maxScore)
                    _maxScore = score;

                _scoreDocArray.Add(doc + _docBase, score);

                // a collector can only end a Lucene walk by throwing
                if (_stopWhenPageIsFull && PageIsFull)
                    throw new PageFilledException();
            }
        }

        public override void SetNextReader(IndexReader reader, int docBase, IState state)
        {
            _docBase = docBase;
            _totalHitsBeforeCurrentSegment = _totalHits;
            _stopWhenPageIsFull = false;
        }

        public override bool AcceptsDocsOutOfOrder => true;

        private TopDocs ToTopDocs()
        {
            return new TopDocs(_totalHits, _maxScore, _scoreDocArray);
        }

        private sealed class PageFilledException : Exception
        {
        }
    }
}
