using System;
using System.Collections;
using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.Raven.Enumerators
{
    public class TombstonesToRavenEtlItems : IExtractEnumerator<RavenEtlItem>
    {
        private readonly IEnumerator<Tombstone> _tombstones;
        private readonly string _collection;
        private readonly bool _allDocs;

        public TombstonesToRavenEtlItems(IEnumerator<Tombstone> tombstones, string collection)
        {
            _tombstones = tombstones;
            _collection = collection;
            _allDocs = _collection == null;
        }

        public bool Filter()
        {
            var tombstone = _tombstones.Current;

            if (_allDocs == false)
            {
                if (tombstone.Type != Tombstone.TombstoneType.Document)
                    throw new InvalidOperationException();

                return false;
            }

            switch (tombstone.Type)
            {
                case Tombstone.TombstoneType.Attachment:
                case Tombstone.TombstoneType.Document:
                    return false;
                case Tombstone.TombstoneType.Revision:
                case Tombstone.TombstoneType.Counter:
                    return true;

                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public bool MoveNext()
        {
            if (_tombstones.MoveNext() == false)
                return false;
            
            Current = new RavenEtlItem(_tombstones.Current, _collection, EtlItemType.Document);

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

        public RavenEtlItem Current { get; private set; }
    }
}
