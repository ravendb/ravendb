using System;
using System.Collections;
using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.Elasticsearch.Enumerators
{
    public class TombstonesToElasticsearchItems : IEnumerator<ElasticsearchItem>
    {
        private readonly string _collection;
        private readonly IEnumerator<Tombstone> _tombstones;

        public TombstonesToElasticsearchItems(IEnumerator<Tombstone> tombstones, string collection)
        {
            _tombstones = tombstones;
            _collection = collection;
        }

        public bool MoveNext()
        {
            if (_tombstones.MoveNext() == false)
                return false;

            Current = new ElasticsearchItem(_tombstones.Current, _collection) {Filtered = Filter()};

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

        private bool Filter()
        {
            return _tombstones.Current.Type != Tombstone.TombstoneType.Document;
        }
    }
}
