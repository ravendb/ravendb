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
        private readonly long? _maxEtag;

        public FilterTombstonesEnumerator(IEnumerator<Tombstone> tombstones, EtlStatsScope stats, Tombstone.TombstoneType tombstoneType,
            List<string> fromCollections = null, long? maxEtag = null)
        {
            _tombstones = tombstones;
            _stats = stats;
            _tombstoneType = tombstoneType;
            _fromCollections = fromCollections;
            _maxEtag = maxEtag;
        }

        public bool MoveNext()
        {
            Current = null;

            while (_tombstones.MoveNext())
            {
                var current = _tombstones.Current;

                var etlItemType = EtlItemType.None;

                switch (current.Type)
                {
                    case Tombstone.TombstoneType.Document:
                        etlItemType = EtlItemType.Document;
                        break;
                    case Tombstone.TombstoneType.Counter:
                        etlItemType = EtlItemType.Counter;
                        break;
                    default:
                        ThrowFilteringTombstonsOfTypeNotSupported(current.Type);
                        break;
                }

                if (_maxEtag != null)
                {
                    switch (_tombstoneType)
                    {
                        case Tombstone.TombstoneType.Counter:
                            if (PreventCountersIteratingTooFarEnumerator<ExtractedItem>.CanMoveNext(current.Etag, _maxEtag.Value) == false)
                                return false;
                            break;
                        default:
                            ThrowMaxEtagLimitNotSupported(_tombstoneType);
                            break;
                    }
                }

                if (current.Type == _tombstoneType)
                {
                    if (_fromCollections == null || _fromCollections.Contains(current.Collection, StringComparer.OrdinalIgnoreCase))
                    {
                        Current = current;
                        return true;
                    }
                }

                _stats.RecordChangeVector(current.ChangeVector);

                _stats.RecordLastFilteredOutEtag(current.Etag, etlItemType);
            }

            return false;
        }

        private static void ThrowMaxEtagLimitNotSupported(Tombstone.TombstoneType tombstoneType)
        {
            throw new NotSupportedException($"Limiting tombstones to iterate up to max etag isn't supported for tombstones of type: {tombstoneType}");
        }

        private static void ThrowFilteringTombstonsOfTypeNotSupported(Tombstone.TombstoneType tombstoneType)
        {
            throw new NotSupportedException($"Filtering tombstones of type: {tombstoneType} is not supported");
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
