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


        public static implicit operator ReplicationBatchDocumentItem(Document doc)
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

        public static implicit operator ReplicationBatchDocumentItem(DocumentTombstone doc)
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

        public static implicit operator ReplicationBatchDocumentItem(DocumentConflict doc)
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
    }
}