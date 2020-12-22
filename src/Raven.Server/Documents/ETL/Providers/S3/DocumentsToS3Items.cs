using System.Collections;
using System.Collections.Generic;
using Raven.Server.Documents.ETL.Providers.SQL;

namespace Raven.Server.Documents.ETL.Providers.S3
{
    public class DocumentsToS3Items : IEnumerator<ToS3Item>
    {
        private readonly IEnumerator<Document> _docs;
        private readonly string _collection;

        public DocumentsToS3Items(IEnumerator<Document> docs, string collection)
        {
            _docs = docs;
            _collection = collection;
        }

        public bool MoveNext()
        {
            if (_docs.MoveNext() == false)
                return false;

            Current = new ToS3Item(_docs.Current, _collection);

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

        public ToS3Item Current { get; private set; }
    }
}
