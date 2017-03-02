using System.IO;
using Raven.Client.Documents.Replication.Messages;
using Sparrow.Json;

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

    public class ReplicationBatchDocumentItem
    {
        public LazyStringValue Key;
        public ChangeVectorEntry[] ChangeVector;
        public BlittableJsonReaderObject Data;
        public long Etag;
        public LazyStringValue Collection;
        public DocumentFlags Flags;
        public short TransactionMarker;
        public long LastModifiedTicks;

        public static ReplicationBatchDocumentItem From(Document doc)
        {
            return new ReplicationBatchDocumentItem
            {
                Etag = doc.Etag,
                ChangeVector = doc.ChangeVector,
                Data = doc.Data,
                Key = doc.Key,
                Flags = doc.Flags,
                TransactionMarker = doc.TransactionMarker,
                LastModifiedTicks = doc.LastModified.Ticks,
            };
        }

        public static ReplicationBatchDocumentItem From(DocumentTombstone doc)
        {
            return new ReplicationBatchDocumentItem
            {
                Etag = doc.Etag,
                ChangeVector = doc.ChangeVector,
                Collection = doc.Collection,
                Key = doc.Key,
                Flags = doc.Flags,
                TransactionMarker = doc.TransactionMarker,
                LastModifiedTicks = doc.LastModified.Ticks,
            };
        }

        public static ReplicationBatchDocumentItem From(DocumentConflict doc)
        {
            return new ReplicationBatchDocumentItem
            {
                Etag = doc.Etag,
                ChangeVector = doc.ChangeVector,
                Collection = doc.Collection,
                Data = doc.Doc,
                Key = doc.Key,
                LastModifiedTicks = doc.LastModified.Ticks,
                TransactionMarker = -1// not relevant
            };
        }

        public static ReplicationBatchDocumentItem From(Attachment attachment)
        {
            return new ReplicationBatchDocumentItem
            {
                IsAttachmnet = true,
                Key = attachment.LoweredKey,
                Etag = attachment.Etag,
                Name = attachment.Name,
                ContentType = attachment.ContentType,
                Stream = attachment.Stream,
                LastModifiedTicks = attachment.LastModified.Ticks,
            };
        }

        public bool IsAttachmnet { get; set; }
        public LazyStringValue Name { get; set; }
        public LazyStringValue ContentType { get; set; }
        public Stream Stream { get; set; }
    }
}