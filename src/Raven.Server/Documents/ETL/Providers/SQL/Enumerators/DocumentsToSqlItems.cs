using System.Collections;
using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.SQL.Enumerators
{
    public class DocumentsToSqlItems : IEnumerator<ToSqlItem>
    {
        private readonly IEnumerator<Document> _docs;

        public DocumentsToSqlItems(IEnumerator<Document> docs)
        {
            _docs = docs;
        }

        public bool MoveNext()
        {
            if (_docs.MoveNext() == false)
                return false;

            Current = new ToSqlItem(_docs.Current);

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