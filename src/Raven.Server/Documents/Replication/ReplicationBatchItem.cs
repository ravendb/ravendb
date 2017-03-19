using System.IO;
using Raven.Client.Documents.Replication.Messages;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Documents.Replication
{
    public class ReplicationBatchIndexItem
    {
        public string Name;
        public ChangeVectorEntry[] ChangeVector;
        public BlittableJsonReaderObject Definition;
        public long Etag;
        public int Type;
    }

    public class ReplicationBatchItem
    {
        public LazyStringValue Key;
        public ChangeVectorEntry[] ChangeVector;
        public BlittableJsonReaderObject Data;
        public long Etag;
        public LazyStringValue Collection;
        public DocumentFlags Flags;
        public short TransactionMarker;
        public long LastModifiedTicks;

        public static ReplicationBatchItem From(Document doc)
        {
            return new ReplicationBatchItem
            {
                Type = ReplicationItemType.Document,
                Etag = doc.Etag,
                ChangeVector = doc.ChangeVector,
                Data = doc.Data,
                Key = doc.Key,
                Flags = doc.Flags,
                TransactionMarker = doc.TransactionMarker,
                LastModifiedTicks = doc.LastModified.Ticks,
            };
        }

        public static ReplicationBatchItem From(DocumentTombstone doc)
        {
            return new ReplicationBatchItem
            {
                Type = ReplicationItemType.Document,
                Etag = doc.Etag,
                ChangeVector = doc.ChangeVector,
                Collection = doc.Collection,
                Key = doc.Key,
                Flags = doc.Flags,
                TransactionMarker = doc.TransactionMarker,
                LastModifiedTicks = doc.LastModified.Ticks,
            };
        }

        public static ReplicationBatchItem From(DocumentConflict doc)
        {
            return new ReplicationBatchItem
            {
                Type = ReplicationItemType.Document,
                Etag = doc.Etag,
                ChangeVector = doc.ChangeVector,
                Collection = doc.Collection,
                Data = doc.Doc,
                Key = doc.Key,
                LastModifiedTicks = doc.LastModified.Ticks,
                TransactionMarker = -1// not relevant
            };
        }

        public static ReplicationBatchItem From(Attachment attachment)
        {
            return new ReplicationBatchItem
            {
                Type = ReplicationItemType.Attachment,
                Key = attachment.LoweredKey,
                Etag = attachment.Etag,
                Name = attachment.Name,
                ContentType = attachment.ContentType,
                Base64Hash = attachment.Base64Hash,
                Stream = attachment.Stream,
                TransactionMarker = attachment.TransactionMarker,
            };
        }

        public ReplicationItemType Type { get; set; }
        public LazyStringValue Name { get; set; }
        public LazyStringValue ContentType { get; set; }
        public Slice Base64Hash { get; set; }
        public Stream Stream { get; set; }

        public enum ReplicationItemType : byte
        {
            Document = 1,
            Attachment = 2,
            AttachmentStream = 3,
        }
    }
}