using System.Collections;
using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.Raven.Enumerators
{
    public class TombstonesToRavenEtlItems : IEnumerator<RavenEtlItem>
    {
        private readonly IEnumerator<Tombstone> _tombstones;
        private readonly string _collection;
        private readonly EtlItemType _tombstoneType;

        public TombstonesToRavenEtlItems(IEnumerator<Tombstone> tombstones, string collection, EtlItemType tombstoneType)
        {
            _tombstones = tombstones;
            _collection = collection;
            _tombstoneType = tombstoneType;
        }

        public bool MoveNext()
        {
            if (_tombstones.MoveNext() == false)
                return false;
            
            Current = new RavenEtlItem(_tombstones.Current, _collection, _tombstoneType);

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
