using System;
using System.Collections;
using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.Queue.Enumerators
{
    public class DocumentsToQueueItems : IEnumerator<QueueItem>
    {
        private readonly string _collection;
        private readonly IEnumerator<Document> _docs;

        public DocumentsToQueueItems(IEnumerator<Document> docs, string collection)
        {
            _docs = docs;
            _collection = collection;
        }

        public bool MoveNext()
        {
            if (_docs.MoveNext() == false)
                return false;

            Current = new QueueItem(_docs.Current, _collection);

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

        public QueueItem Current { get; private set; }
    }
}
