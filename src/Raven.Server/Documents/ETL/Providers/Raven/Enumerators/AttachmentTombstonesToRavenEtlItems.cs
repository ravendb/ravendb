using System.Collections;
using System.Collections.Generic;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.Raven.Enumerators
{
    public sealed class AttachmentTombstonesToRavenEtlItems : IEnumerator<RavenEtlItem>
    {
        private readonly DocumentsOperationContext _context;
        private readonly IEnumerator<AttachmentTombstoneReplicationItem> _tombstones;
        private readonly List<string> _collections;

        public AttachmentTombstonesToRavenEtlItems(DocumentsOperationContext context, IEnumerator<AttachmentTombstoneReplicationItem> tombstones, List<string> collections)
        {
            _context = context;
            _tombstones = tombstones;
            _collections = collections;
        }

        private bool Filter(RavenEtlItem item)
        {
            if (item.AttachmentTombstone.Flags.Contain(DocumentFlags.Artificial))
                return true;

            return FilterAttachment(_context, item);
        }

        public bool FilterAttachment(DocumentsOperationContext context, RavenEtlItem item)
        {
            var documentId = AttachmentsStorage.ExtractDocIdAndAttachmentNameFromTombstone(item.AttachmentTombstone.Key).DocId;
            using var document = context.DocumentDatabase.DocumentsStorage.Get(context, documentId);
            if (document == null)
                return true; // document could be deleted, no need to send DELETE of tombstone, we can filter it out

            var collection = context.DocumentDatabase.DocumentsStorage.ExtractCollectionName(context, document.Data).Name;
            item.Collection = collection;

            if (_collections == null)
                return false;

            return _collections.Contains(item.Collection) == false;
        }

        public bool MoveNext()
        {
            if (_tombstones.MoveNext() == false)
                return false;

            Current = new RavenEtlItem(_context, _tombstones.Current);
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
