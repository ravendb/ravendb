using System;
using System.Collections;
using System.Collections.Generic;
using Raven.Server.Documents.ETL.Stats;

namespace Raven.Server.Documents.ETL
{
    public class FilterTombstonesEnumerator : IEnumerator<Tombstone>
    {
        private readonly IEnumerator<Tombstone> _tombstones;
        private readonly EtlStatsScope _stats;
        private readonly Tombstone.TombstoneType _tombstoneType;

        public FilterTombstonesEnumerator(IEnumerator<Tombstone> tombstones, EtlStatsScope stats, Tombstone.TombstoneType tombstoneType)
        {
            _tombstones = tombstones;
            _stats = stats;
            _tombstoneType = tombstoneType;
        }

        public bool MoveNext()
        {
            Current = null;

            while (_tombstones.MoveNext())
            {
                var current = _tombstones.Current;

                // TODO arek - for counter we need to probably iterate up to last transformed / filtered doc etag

                if (current.Type == _tombstoneType)
                {
                    Current = current;
                    return true;
                }

                _stats.RecordChangeVector(current.ChangeVector);
                _stats.RecordLastFilteredOutEtag(current.Etag);
            }

            return false;
        }

        public void Reset()
        {
            throw new NotSupportedException();
        }

        public Tombstone Current { get; private set; }

        object IEnumerator.Current => Current;

        public void Dispose()
        {
        }
    }
}
