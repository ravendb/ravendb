using System;
using System.Collections;
using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.Queue.Enumerators
{
    public class TombstonesToQueueItems : IEnumerator<QueueItem>
    {
        private readonly string _collection;
        private readonly IEnumerator<Tombstone> _tombstones;

        public TombstonesToQueueItems(IEnumerator<Tombstone> tombstones, string collection)
        {
            _tombstones = tombstones;
            _collection = collection;
        }

        public bool MoveNext()
        {
            if (_tombstones.MoveNext() == false)
                return false;

            Current = new QueueItem(_tombstones.Current, _collection) {Filtered = Filter()};

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

        private bool Filter()
        {
            return _tombstones.Current.Type != Tombstone.TombstoneType.Document;
        }
    }
}
