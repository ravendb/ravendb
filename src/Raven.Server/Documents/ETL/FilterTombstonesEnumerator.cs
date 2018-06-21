using System;
using System.Linq;
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
        private readonly List<string> _fromCollections;

        public FilterTombstonesEnumerator(IEnumerator<Tombstone> tombstones, EtlStatsScope stats, Tombstone.TombstoneType tombstoneType,
            List<string> fromCollections = null)
        {
            _tombstones = tombstones;
            _stats = stats;
            _tombstoneType = tombstoneType;
            _fromCollections = fromCollections;
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
                    if (_fromCollections == null || _fromCollections.Contains(current.Collection, StringComparer.OrdinalIgnoreCase))
                    {
                        Current = current;
                        return true;
                    }
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
