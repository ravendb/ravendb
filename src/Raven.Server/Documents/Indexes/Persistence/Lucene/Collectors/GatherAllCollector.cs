using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Lucene.Net.Util;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Collectors
{
    public sealed class GatherAllCollector : Collector
    {
        private readonly ManagedScoreDocArray _scoreDocArray;
        private int _docBase;

        private Scorer _scorer;
        private float _maxScore;

        public GatherAllCollector()
        {
            _scoreDocArray = new ManagedScoreDocArray();
        }

        public override void SetScorer(Scorer scorer)
        {
            _scorer = scorer;
        }

        public override void Collect(int doc, IState state)
        {
            var score = _scorer?.Score(state) ?? 0;
            if (score > _maxScore)
                _maxScore = score;

            _scoreDocArray.Add(doc + _docBase, score);
        }

        public override void SetNextReader(IndexReader reader, int docBase, IState state)
        {
            _docBase = docBase;
        }

        public override bool AcceptsDocsOutOfOrder => true;

        public TopDocs ToTopDocs()
        {
            return new TopDocs(_scoreDocArray.Length, _maxScore, _scoreDocArray);
        }
    }
}
