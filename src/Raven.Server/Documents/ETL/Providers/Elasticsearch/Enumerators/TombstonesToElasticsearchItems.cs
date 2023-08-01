using System;
using System.Collections;
using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.ElasticSearch.Enumerators
{
    public sealed class TombstonesToElasticSearchItems : IEnumerator<ElasticSearchItem>
    {
        private readonly string _collection;
        private readonly IEnumerator<Tombstone> _tombstones;

        public TombstonesToElasticSearchItems(IEnumerator<Tombstone> tombstones, string collection)
        {
            _tombstones = tombstones;
            _collection = collection;
        }

        public bool MoveNext()
        {
            if (_tombstones.MoveNext() == false)
                return false;

            Current = new ElasticSearchItem(_tombstones.Current, _collection) {Filtered = Filter()};

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

        private bool Filter()
        {
            var tombstone = _tombstones.Current;
            return tombstone.Type != Tombstone.TombstoneType.Document || tombstone.Flags.Contain(DocumentFlags.Artificial);
        }
    }
}
