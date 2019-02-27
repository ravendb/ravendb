using System;
using System.Collections.Generic;

using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Collectors
{
    public sealed class NonSortingCollector : Collector, IDisposable
    {
        private readonly int _numberOfDocsToCollect;
        private readonly List<ScoreDoc> _docs;
        private int _totalHits;
        private int _docBase;

        private Scorer _scorer;
        private float _maxScore;

        public NonSortingCollector(int numberOfDocsToCollect)
        {
            _numberOfDocsToCollect = numberOfDocsToCollect;
            _docs = CollectorsPool.Instance.Allocate();            
        }

        public override void SetScorer(Scorer scorer)
        {
            _scorer = scorer;
        }

        public override void Collect(int doc, IState state)
        {
            if (_docs.Count < _numberOfDocsToCollect)
            {
                var score = _scorer?.Score(state) ?? 0;
                if (score > _maxScore)
                    _maxScore = score;

                _docs.Add(new ScoreDoc(doc + _docBase, score));
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
            return new TopDocs(_totalHits, _docs.ToArray(), _maxScore);
        }

        public void Dispose()
        {
            _docs.Clear();
            CollectorsPool.Instance.Free(_docs);
        }
    }
}
