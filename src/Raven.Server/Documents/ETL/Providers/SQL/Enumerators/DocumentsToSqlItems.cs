using System.Collections;
using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.SQL.Enumerators
{
    public class DocumentsToSqlItems : IEnumerator<ToSqlItem>
    {
        private readonly IEnumerator<Document> _docs;
        private readonly string _collection;

        public DocumentsToSqlItems(IEnumerator<Document> docs, string collection)
        {
            _docs = docs;
            _collection = collection;
        }

        public bool MoveNext()
        {
            if (_docs.MoveNext() == false)
                return false;

            Current = new ToSqlItem(_docs.Current, _collection);

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

        public ToSqlItem Current { get; private set; }
    }
}
