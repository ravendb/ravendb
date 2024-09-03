using System;
using Raven.Server.ServerWide.Context;
using Sparrow.Server;
using Voron;
using Voron.Data.Tables;
using static Raven.Server.Documents.Schemas.Tombstones;

namespace Raven.Server.Documents.Replication.ReplicationItems
{
    public sealed class TombstoneReplicationItem
    {
        public static ReplicationBatchItem From(DocumentsOperationContext context, ref TableValueReader tvr)
        {
            var doc = DocumentsStorage.TableValueToTombstone(context, ref tvr);
            switch (doc.Type)
            {
                case Tombstone.TombstoneType.Attachment:
                    AttachmentTombstoneReplicationItem item = AttachmentTombstoneReplicationItemInternal(context, doc);

                    var enumVal = DocumentsStorage.TableValueToInt((int)TombstoneTable.Flags, ref tvr);
                    if (enumVal == (int)AttachmentTombstoneFlags.FromStorageOnly)
                    {
                        item.TombstoneFlags = AttachmentTombstoneFlags.FromStorageOnly;
                    }
                    else
                    {
                        item.Flags = DocumentsStorage.TableValueToFlags((int)TombstoneTable.Flags, ref tvr);
                        item.TombstoneFlags = AttachmentTombstoneFlags.None;
                    }

                    return item;
                default:
                    return From(context, doc);
            }
        }

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
                    AttachmentTombstoneReplicationItem item = AttachmentTombstoneReplicationItemInternal(context, doc);
                    item.Flags = doc.Flags;

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

        private static unsafe AttachmentTombstoneReplicationItem AttachmentTombstoneReplicationItemInternal(DocumentsOperationContext context, Tombstone doc)
        {
            var item = new AttachmentTombstoneReplicationItem
            {
                Type = ReplicationBatchItem.ReplicationItemType.AttachmentTombstone,
                Etag = doc.Etag,
                TransactionMarker = doc.TransactionMarker,
                ChangeVector = doc.ChangeVector,
                LastModifiedTicks = doc.LastModified.Ticks,
            };

            item.ToDispose(Slice.From(context.Allocator, doc.LowerId.Buffer, doc.LowerId.Size, ByteStringType.Immutable, out item.Key));
            return item;
        }
    }
}
