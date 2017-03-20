using System.Collections;
using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.Raven.Enumerators
{
    public class DocumentsToRavenEtlItems : IEnumerator<RavenEtlItem>
    {
        private readonly IEnumerator<Document> _docs;

        public DocumentsToRavenEtlItems(IEnumerator<Document> docs)
        {
            _docs = docs;
        }

        public bool MoveNext()
        {
            if (_docs.MoveNext() == false)
                return false;

            Current = new RavenEtlItem(_docs.Current);

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