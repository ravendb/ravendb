using System;
using Raven.Server.ServerWide.Context;
using Sparrow.Server;
using Voron;

namespace Raven.Server.Documents.Replication.ReplicationItems
{
    public sealed class TombstoneReplicationItem
    {
        public static unsafe ReplicationBatchItem From(DocumentsOperationContext context, Tombstone doc)
        {
            switch (doc.Type)
            {
                case Tombstone.TombstoneType.Document:
                    return new DocumentReplicationItem
                    {
                        Type = ReplicationBatchItem.ReplicationItemType.DocumentTombstone,
                        Etag = doc.Etag,
                        Id = doc.LowerId,
                        TransactionMarker = doc.TransactionMarker,
                        ChangeVector = doc.ChangeVector,
                        Collection = doc.Collection,
                        Flags = doc.Flags,
                        LastModifiedTicks = doc.LastModified.Ticks
                    };

                case Tombstone.TombstoneType.Attachment:
                    var item = new AttachmentTombstoneReplicationItem
                    {
                        Type = ReplicationBatchItem.ReplicationItemType.AttachmentTombstone,
                        Etag = doc.Etag,
                        TransactionMarker = doc.TransactionMarker,
                        ChangeVector = doc.ChangeVector,
                        Flags = doc.Flags,
                        LastModifiedTicks = doc.LastModified.Ticks,
                    };

                    item.ToDispose(Slice.From(context.Allocator, doc.LowerId.Buffer, doc.LowerId.Size, ByteStringType.Immutable, out item.Key));
                    return item;

                case Tombstone.TombstoneType.Revision:
                    return new RevisionTombstoneReplicationItem
                    {
                        Type = ReplicationBatchItem.ReplicationItemType.RevisionTombstone,
                        Etag = doc.Etag,
                        Id = doc.LowerId,
                        TransactionMarker = doc.TransactionMarker,
                        ChangeVector = doc.ChangeVector,
                        Collection = doc.Collection,
                        Flags = doc.Flags
                    };
                default:
                    throw new ArgumentOutOfRangeException(nameof(doc.Type));
            }
        }
    }
}
