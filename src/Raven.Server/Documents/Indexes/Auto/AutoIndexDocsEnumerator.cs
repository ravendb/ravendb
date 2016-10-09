using System.Collections;
using System.Collections.Generic;
using Raven.Client.Data.Indexes;

namespace Raven.Server.Documents.Indexes.Auto
{
    public class AutoIndexDocsEnumerator : IIndexedDocumentsEnumerator
    {
        private readonly IndexingStatsScope _documentReadStats;
        private readonly IEnumerator<Document> _docsEnumerator;
        private readonly Document[] _results = new Document[1];

        public AutoIndexDocsEnumerator(IEnumerable<Document> documents, IndexingStatsScope stats)
        {
            _documentReadStats = stats.For(IndexingOperation.Map.DocumentRead, start: false);
            _docsEnumerator = documents.GetEnumerator();
        }

        public bool MoveNext(out IEnumerable resultsOfCurrentDocument)
        {
            using (_documentReadStats.Start())
            {
                var moveNext = _docsEnumerator.MoveNext();

                _results[0] = _docsEnumerator.Current;
                resultsOfCurrentDocument = _results;

                return moveNext;
            }
        }

        public Document Current => _docsEnumerator.Current;

        public void Dispose()
        {
            _docsEnumerator.Dispose();
        }
    }
}