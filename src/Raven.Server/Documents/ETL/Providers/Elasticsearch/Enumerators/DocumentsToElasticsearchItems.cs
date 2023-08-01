using System;
using System.Collections;
using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.ElasticSearch.Enumerators
{
    public sealed class DocumentsToElasticSearchItems : IEnumerator<ElasticSearchItem>
    {
        private readonly string _collection;
        private readonly IEnumerator<Document> _docs;

        public DocumentsToElasticSearchItems(IEnumerator<Document> docs, string collection)
        {
            _docs = docs;
            _collection = collection;
        }

        public bool MoveNext()
        {
            if (_docs.MoveNext() == false)
                return false;

            Current = new ElasticSearchItem(_docs.Current, _collection);

            return true;
        }

        public void Reset()
        {
            throw new NotImplementedException();
        }

        object IEnumerator.Current => Current;

        public void Dispose()
        {
        }

        public ElasticSearchItem Current { get; private set; }
    }
}
