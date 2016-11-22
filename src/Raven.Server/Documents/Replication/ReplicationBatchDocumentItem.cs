using Raven.Client.Replication.Messages;
using Sparrow.Json;

namespace Raven.Server.Documents.Replication
{
    public struct ReplicationBatchIndexItem
    {
        public string Name;
        public ChangeVectorEntry[] ChangeVector;
        public BlittableJsonReaderObject Definition;
        public long Etag;
        public int Type;
    }

    public struct ReplicationBatchDocumentItem
    {
        public LazyStringValue Key;
        public ChangeVectorEntry[] ChangeVector;
        public BlittableJsonReaderObject Data;
        public long Etag;
        public LazyStringValue Collection;
    }
}