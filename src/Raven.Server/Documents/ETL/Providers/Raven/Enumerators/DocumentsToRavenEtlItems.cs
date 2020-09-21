using System.Collections;
using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.Raven.Enumerators
{
    public class DocumentsToRavenEtlItems : IExtractEnumerator<RavenEtlItem>
    {
        private readonly IEnumerator<Document> _docs;
        private readonly string _collection;

        public DocumentsToRavenEtlItems(IEnumerator<Document> docs, string collection)
        {
            _docs = docs;
            _collection = collection;
        }

        public bool Filter() => false;

        public bool MoveNext()
        {
            if (_docs.MoveNext() == false)
                return false;

            Current = new RavenEtlItem(_docs.Current, _collection);

            return true;
        }

        public void Reset()
        {
            throw new System.NotImplementedException();
        }

        object IEnumerator.Current => Current;

        public void Dispose()
        {
        }

        public RavenEtlItem Current { get; private set; }
    }
}
