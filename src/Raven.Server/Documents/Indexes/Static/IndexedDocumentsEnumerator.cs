using System.Collections;
using System.Collections.Generic;

namespace Raven.Server.Documents.Indexes.Static
{
    public class IndexedDocumentsEnumerator : IEnumerable<DynamicDocumentObject>
    {
        private readonly IEnumerable<Document> _docs;
        private readonly DynamicDocumentObject _dynamicDocument = new DynamicDocumentObject();

        public IndexedDocumentsEnumerator(IEnumerable<Document> docs)
        {
            _docs = docs;
        }

        public IEnumerator<DynamicDocumentObject> GetEnumerator()
        {
            Document previous = null;

            foreach (var doc in _docs)
            {
                previous?.Data.Dispose();

                _dynamicDocument.Set(doc);
                yield return _dynamicDocument;

                previous = doc;
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}