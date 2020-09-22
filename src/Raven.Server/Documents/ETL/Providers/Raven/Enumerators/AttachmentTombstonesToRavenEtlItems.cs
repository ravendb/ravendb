using System;
using System.Collections;
using System.Collections.Generic;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.Raven.Enumerators
{
    public class AttachmentTombstonesToRavenEtlItems : IEnumerator<RavenEtlItem>
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

        private bool Filter(RavenEtlItem item)
        {
            var tombstone = _tombstones.Current;
            if (tombstone.Type != Tombstone.TombstoneType.Attachment)
                TombstonesToRavenEtlItems.ThrowInvalidTombstoneType(Tombstone.TombstoneType.Attachment, tombstone.Type);

            if (FilterAttachment(_context, item))
                return true;

            return _collections.Contains(item.Collection) == false;
        }

        public static bool FilterAttachment(DocumentsOperationContext context, RavenEtlItem item)
        {
            var documentId = AttachmentsStorage.ExtractDocIdAndAttachmentNameFromTombstone(context, item.AttachmentTombstoneId).DocId;
            var document = context.DocumentDatabase.DocumentsStorage.Get(context, documentId);
            if (document == null)
                return true; // document could be deleted, no need to send DELETE of tombstone, we can filter it out
            
            var collection = context.DocumentDatabase.DocumentsStorage.ExtractCollectionName(context, document.Data).Name;
            item.Collection = collection;
            return false;
        }

        public bool MoveNext()
        {
            if (_tombstones.MoveNext() == false)
                return false;

            Current = new RavenEtlItem(_tombstones.Current, "__undefined", EtlItemType.Document);
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
