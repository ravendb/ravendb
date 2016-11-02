using Raven.Client.Replication.Messages;
using Sparrow.Json;

namespace Raven.Server.Documents.Replication
{
    public struct ReplicationBatchItem
    {
        public enum ItemType
        {
            Document,
            Index,
            Transformer,
            Tombstone
        }

        public LazyStringValue Key;
        public int Id;
        public ItemType Type;
        public ChangeVectorEntry[] ChangeVector;
        public BlittableJsonReaderObject Data;
        public long Etag;
        public LazyStringValue Collection;
    }
}