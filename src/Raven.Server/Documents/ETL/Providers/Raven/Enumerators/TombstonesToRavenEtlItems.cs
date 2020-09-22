using System;
using System.Collections;
using System.Collections.Generic;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.Raven.Enumerators
{
    public class TombstonesToRavenEtlItems : IEnumerator<RavenEtlItem>
    {
        private readonly DocumentsOperationContext _context;
        private readonly IEnumerator<Tombstone> _tombstones;
        private readonly string _collection;
        private readonly bool _trackAttachments;
        private readonly bool _allDocs;

        public TombstonesToRavenEtlItems(DocumentsOperationContext context, IEnumerator<Tombstone> tombstones, string collection, bool trackAttachments)
        {
            _context = context;
            _tombstones = tombstones;
            _collection = collection;

            _trackAttachments = trackAttachments;
            _allDocs = _collection == null;
        }

        private bool Filter(RavenEtlItem item)
        {
            var tombstone = _tombstones.Current;

            if (_allDocs == false)
            {
                if (tombstone.Type != Tombstone.TombstoneType.Document)
                    ThrowInvalidTombstoneType(Tombstone.TombstoneType.Document, tombstone.Type);

                return false;
            }

            switch (tombstone.Type)
            {
                case Tombstone.TombstoneType.Attachment:
                    if (_trackAttachments == false)
                        return true;

                    return AttachmentTombstonesToRavenEtlItems.FilterAttachment(_context, item);
                case Tombstone.TombstoneType.Document:
                    return false;
                case Tombstone.TombstoneType.Revision:
                case Tombstone.TombstoneType.Counter:
                    return true;

                default:
                    throw new ArgumentOutOfRangeException(nameof(tombstone.Type),$"Unknown type '{tombstone.Type}'");
            }
        }

        public static void ThrowInvalidTombstoneType(Tombstone.TombstoneType expectedType, Tombstone.TombstoneType actualType)
        {
            throw new InvalidOperationException($"When collection is specified, tombstone must be of type '{expectedType}', but got '{actualType}'");
        }

        public bool MoveNext()
        {
            if (_tombstones.MoveNext() == false)
                return false;
            
            Current = new RavenEtlItem(_tombstones.Current, _collection, EtlItemType.Document);
            Current.Filtered = Filter(Current);

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
