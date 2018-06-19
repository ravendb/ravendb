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

        public FilterTombstonesEnumerator(IEnumerator<Tombstone> tombstones, EtlStatsScope stats)
        {
            _tombstones = tombstones;
            _stats = stats;
        }

        public bool MoveNext()
        {
            Current = null;

            while (_tombstones.MoveNext())
            {
                var current = _tombstones.Current;
                if (current.Type == Tombstone.TombstoneType.Document)
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
