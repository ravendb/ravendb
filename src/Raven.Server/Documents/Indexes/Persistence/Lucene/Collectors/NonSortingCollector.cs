using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Lucene.Net.Util;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Collectors
{
    public sealed class NonSortingCollector : Collector
    {
        private readonly int _numberOfDocsToCollect;
        private readonly ManagedScoreDocArray _scoreDocArray;

        private int _totalHits;
        private float _maxScore;
        private int _docBase;

        private Scorer _scorer;

        public NonSortingCollector(int numberOfDocsToCollect)
        {
            _scoreDocArray = new ManagedScoreDocArray();
            _numberOfDocsToCollect = numberOfDocsToCollect;
        }

        public override void SetScorer(Scorer scorer)
        {
            _scorer = scorer;
        }

        public override void Collect(int doc, IState state)
        {
            if (_scoreDocArray.Length < _numberOfDocsToCollect)
            {
                var score = _scorer?.Score(state) ?? 0;
                if (score > _maxScore)
                    _maxScore = score;

                _scoreDocArray.Add(doc + _docBase, score);
            }

            _totalHits++;
        }

        public override void SetNextReader(IndexReader reader, int docBase, IState state)
        {
            _docBase = docBase;
        }

        public override bool AcceptsDocsOutOfOrder => true;

        public TopDocs ToTopDocs()
        {
            return new TopDocs(_totalHits, _maxScore, _scoreDocArray);
        }
    }
}
