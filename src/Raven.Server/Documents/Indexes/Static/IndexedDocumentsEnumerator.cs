using System.Collections;
using System.Collections.Generic;

namespace Raven.Server.Documents.Indexes.Static
{
    public class IndexedDocumentsEnumerator : IEnumerable<DynamicDocumentObject>
    {
        private readonly IEnumerable<Document> _docs;

        public IndexedDocumentsEnumerator(IEnumerable<Document> docs)
        {
            _docs = docs;
        }

        public IEnumerator<DynamicDocumentObject> GetEnumerator()
        {
            return new DynamicObjectEnumerator(_docs);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return new DynamicObjectEnumerator(_docs);
        }

        private class DynamicObjectEnumerator : IEnumerator<DynamicDocumentObject>
        {
            private readonly DynamicDocumentObject _dynamicDocument = new DynamicDocumentObject();
            private readonly IEnumerator<Document> _inner;
            private Document _previous;

            public DynamicObjectEnumerator(IEnumerable<Document> docs)
            {
                _inner = docs.GetEnumerator();
            }

            public bool MoveNext()
            {
                if (_inner.MoveNext() == false)
                    return false;

                _previous?.Data.Dispose();

                _dynamicDocument.Set(_inner.Current);
                _previous = _inner.Current;

                Current = _dynamicDocument;

                return true;
            }

            public void Reset()
            {
                throw new System.NotImplementedException();
            }

            public DynamicDocumentObject Current { get; private set; }

            object IEnumerator.Current => Current;

            public void Dispose()
            {
                _previous?.Data.Dispose();
            }
        }
    }
}