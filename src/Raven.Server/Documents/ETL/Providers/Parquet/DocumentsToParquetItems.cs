using System.Collections;
using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.Parquet
{
    public class DocumentsToParquetItems : IEnumerator<ToParquetItem>
    {
        private readonly IEnumerator<Document> _docs;
        private readonly string _collection;

        public DocumentsToParquetItems(IEnumerator<Document> docs, string collection)
        {
            _docs = docs;
            _collection = collection;
        }

        public bool MoveNext()
        {
            if (_docs.MoveNext() == false)
                return false;

            Current = new ToParquetItem(_docs.Current, _collection);

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

        public ToParquetItem Current { get; private set; }
    }
}
