using System.Collections.Generic;
using System.Linq;

using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Raven.Client;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Collectors
{
    public sealed class IntersectionCollector : Collector
    {
        private readonly Dictionary<string, SubQueryResult> _results = new Dictionary<string, SubQueryResult>();
        private IndexReader _currentReader;
        private Scorer _currentScorer;

        public IntersectionCollector(Searchable indexSearcher, IEnumerable<ScoreDoc> scoreDocs, IState state)
        {
            foreach (var scoreDoc in scoreDocs)
            {
                var document = indexSearcher.Doc(scoreDoc.Doc, state);
                var subQueryResult = new SubQueryResult
                {
                    LuceneId = scoreDoc.Doc,
                    RavenDocId = document.Get(Constants.Documents.Indexing.Fields.DocumentIdFieldName, state) ?? document.Get(Constants.Documents.Indexing.Fields.ReduceKeyHashFieldName, state),
                    Score = float.IsNaN(scoreDoc.Score) ? 0.0f : scoreDoc.Score,
                    Count = 1
                };
                _results[subQueryResult.RavenDocId] = subQueryResult;
            }
        }

        public override void SetScorer(Scorer scorer)
        {
            _currentScorer = scorer;
        }

        public override void Collect(int doc, IState state)
        {
            //Don't need to add the currentBase here, it's already accounted for
            var document = _currentReader.Document(doc, state);
            var key = document.Get(Constants.Documents.Indexing.Fields.DocumentIdFieldName, state) ?? document.Get(Constants.Documents.Indexing.Fields.ReduceKeyHashFieldName, state);
            var currentScore = _currentScorer.Score(state);

            if (_results.TryGetValue(key, out SubQueryResult value) == false)
                return;

            value.Count++;
            value.Score += currentScore;
        }

        public override void SetNextReader(IndexReader reader, int docBase, IState state)
        {
            _currentReader = reader;
        }

        public override bool AcceptsDocsOutOfOrder => true;

        public IEnumerable<SubQueryResult> DocumentsIdsForCount(int expectedCount)
        {
            return from value in _results.Values
                   where value.Count >= expectedCount
                   select value;
        }

        public class SubQueryResult
        {
            public int LuceneId { get; set; }
            public string RavenDocId { get; set; }
            public float Score { get; set; }
            public int Count { get; set; }
        }
    }
}
