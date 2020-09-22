using System;
using System.Collections;
using System.Collections.Generic;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.Raven.Enumerators
{
    public class AttachmentTombstonesToRavenEtlItems : IExtractEnumerator<RavenEtlItem>
    {
        private readonly DocumentsOperationContext _context;
        private readonly IEnumerator<Tombstone> _tombstones;
        private readonly List<string> _collections;

        public AttachmentTombstonesToRavenEtlItems(DocumentsOperationContext context, IEnumerator<Tombstone> tombstones, List<string> collections)
        {
            _context = context;
            _tombstones = tombstones;
            _collections = collections ?? throw new ArgumentNullException(nameof(collections));
        }

        public bool Filter()
        {
            var tombstone = _tombstones.Current;
            if (tombstone.Type != Tombstone.TombstoneType.Attachment)
                TombstonesToRavenEtlItems.ThrowInvalidTombstoneType(Tombstone.TombstoneType.Attachment, tombstone.Type);

            var documentId = AttachmentsStorage.ExtractDocIdAndAttachmentNameFromTombstone(_context, tombstone.LowerId).DocId;
            var document = _context.DocumentDatabase.DocumentsStorage.Get(_context, documentId);
            if (document == null)
                return true;

            // document could be deleted, no need to send DELETE of tombstone, we can filter it out
            var collection = _context.DocumentDatabase.DocumentsStorage.ExtractCollectionName(_context, document.Data).Name;
            Current.Collection = collection;

            return _collections.Contains(collection) == false;
        }

        public bool MoveNext()
        {
            if (_tombstones.MoveNext() == false)
                return false;

            Current = new RavenEtlItem(_tombstones.Current, "__undefined", EtlItemType.Document);

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
