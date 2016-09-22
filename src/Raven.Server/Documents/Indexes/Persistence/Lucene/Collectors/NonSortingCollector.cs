using System.Collections.Generic;

using Lucene.Net.Index;
using Lucene.Net.Search;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Collectors
{
    public class NonSortingCollector : Collector
    {
        private int _numberOfDocsToCollect;
        private List<ScoreDoc> _docs;
        private int _totalHits;


        public NonSortingCollector(int numberOfDocsToCollect)
        {
            _numberOfDocsToCollect = numberOfDocsToCollect;
            _docs = new List<ScoreDoc>(_numberOfDocsToCollect);
        }

        private int _docBase;

        public override void SetScorer(Scorer scorer)
        {
        }

        public override void Collect(int doc)
        {
            if (_docs.Count < _numberOfDocsToCollect)
            {
                _docs.Add(new ScoreDoc(doc + _docBase, 0));
            }

            _totalHits++;
        }

        public override void SetNextReader(IndexReader reader, int docBase)
        {
            _docBase = docBase;
        }

        public override bool AcceptsDocsOutOfOrder => true;

        public TopDocs ToTopDocs()
        {
            return new TopDocs(_totalHits, _docs.ToArray(), 0);
        }
    }
}
