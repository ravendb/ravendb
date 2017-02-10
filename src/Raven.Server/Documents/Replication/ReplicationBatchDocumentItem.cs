using Raven.Client.Replication.Messages;
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
    }
}