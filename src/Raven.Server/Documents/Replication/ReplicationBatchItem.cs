using System;
using System.Collections.Generic;
using System.IO;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Documents.Replication
{
    public class ReplicationBatchItem
    {
        public LazyStringValue Id;
        public long Etag;
        public short TransactionMarker;

        #region Document

        public string ChangeVector;
        public BlittableJsonReaderObject Data;
        public LazyStringValue Collection;
        public DocumentFlags Flags;
        public long LastModifiedTicks;

        #endregion

        #region Attachment

        public ReplicationItemType Type;
        public LazyStringValue Name;
        public LazyStringValue ContentType;
        public Slice Base64Hash;
        public Stream Stream;

        #endregion

        #region Counter

        public long Value;

        public BlittableJsonReaderObject Values;

        #endregion

        public static ReplicationBatchItem From(Document doc)
        {
            return new ReplicationBatchItem
            {
                Type = ReplicationItemType.Document,
                Etag = doc.Etag,
                ChangeVector = doc.ChangeVector,
                Data = doc.Data,
                Id = doc.Id,
                Flags = doc.Flags,
                TransactionMarker = doc.TransactionMarker,
                LastModifiedTicks = doc.LastModified.Ticks
            };
        }

        public static ReplicationBatchItem From(Tombstone doc)
        {
            var item = new ReplicationBatchItem
            {
                Etag = doc.Etag,
                Id = doc.LowerId,
                TransactionMarker = doc.TransactionMarker,
                ChangeVector = doc.ChangeVector
            };

            switch (doc.Type)
            {
                case Tombstone.TombstoneType.Document:
                    item.Type = ReplicationItemType.DocumentTombstone;
                    item.Collection = doc.Collection;
                    item.Flags = doc.Flags;
                    item.LastModifiedTicks = doc.LastModified.Ticks;
                    break;
                case Tombstone.TombstoneType.Attachment:
                    item.Type = ReplicationItemType.AttachmentTombstone;
                    break;
                case Tombstone.TombstoneType.Revision:
                    item.Type = ReplicationItemType.RevisionTombstone;
                    item.Collection = doc.Collection;
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(doc.Type));
            }

            return item;
        }

        public static ReplicationBatchItem From(DocumentConflict doc)
        {
            return new ReplicationBatchItem
            {
                Type = doc.Doc == null ? ReplicationItemType.DocumentTombstone : ReplicationItemType.Document,
                Etag = doc.Etag,
                ChangeVector = doc.ChangeVector,
                Collection = doc.Collection,
                Data = doc.Doc,
                Id = doc.Id,
                Flags = doc.Flags,
                LastModifiedTicks = doc.LastModified.Ticks,
                TransactionMarker = -1// not relevant
            };
        }

        public static ReplicationBatchItem From(Attachment attachment)
        {
            return new ReplicationBatchItem
            {
                Type = ReplicationItemType.Attachment,
                Id = attachment.Key,
                Etag = attachment.Etag,
                ChangeVector = attachment.ChangeVector,
                Name = attachment.Name,
                ContentType = attachment.ContentType,
                Base64Hash = attachment.Base64Hash,
                Stream = attachment.Stream,
                TransactionMarker = attachment.TransactionMarker
            };
        }

        public enum ReplicationItemType : byte
        {
            Document = 1,
            DocumentTombstone = 2,
            Attachment = 3,
            AttachmentStream = 4,
            AttachmentTombstone = 5,
            RevisionTombstone = 6,
            LegacyCounter = 7,

            [Obsolete("ReplicationItemType.CounterTombstone is not supported anymore. Will be removed in next major version of the product.")]
            CounterTombstone = 8,

            CounterGroup = 9
        }
    }
}
