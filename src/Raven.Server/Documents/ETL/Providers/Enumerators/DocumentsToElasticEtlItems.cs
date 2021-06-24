using System;
using System.Collections;
using System.Collections.Generic;
using Raven.Server.Documents.ETL.Providers.Elasticsearch;

namespace Raven.Server.Documents.ETL.Providers.Enumerators
{
    public class DocumentsToElasticsearchEtlItems : IEnumerator<ElasticsearchItem>
    {
        private readonly string _collection;
        private readonly IEnumerator<Document> _docs;

        public DocumentsToElasticsearchEtlItems(IEnumerator<Document> docs, string collection)
        {
            _docs = docs;
            _collection = collection;
        }

        public bool MoveNext()
        {
            if (_docs.MoveNext() == false)
                return false;

            Current = new ElasticsearchItem(_docs.Current, _collection);

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

        public ElasticsearchItem Current { get; private set; }
    }
}
