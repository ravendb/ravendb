using System.Collections;
using System.Collections.Generic;

namespace Raven.Server.Documents.Indexes.Auto
{
    public class AutoIndexDocsEnumerator : IIndexedDocumentsEnumerator
    {
        private readonly IEnumerator<Document> _docsEnumerator;
        private readonly Document[] _results = new Document[1];

        public AutoIndexDocsEnumerator(IEnumerable<Document> documents)
        {
            _docsEnumerator = documents.GetEnumerator();
        }

        public bool MoveNext(out IEnumerable resultsOfCurrentDocument)
        {
            var moveNext = _docsEnumerator.MoveNext();

            _results[0] = _docsEnumerator.Current;
            resultsOfCurrentDocument = _results;

            return moveNext;
        }

        public Document Current => _docsEnumerator.Current;

        public void Dispose()
        {
            _docsEnumerator.Dispose();
        }
    }
}