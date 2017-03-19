using System.Collections;
using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.Raven.Enumerators
{
    public class TombstonesToRavenEtlItems : IEnumerator<RavenEtlItem>
    {
        private readonly IEnumerator<DocumentTombstone> _tombstones;

        public TombstonesToRavenEtlItems(IEnumerator<DocumentTombstone> tombstones)
        {
            _tombstones = tombstones;
        }

        public bool MoveNext()
        {
            if (_tombstones.MoveNext() == false)
                return false;

            Current = new RavenEtlItem(_tombstones.Current);

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