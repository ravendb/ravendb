using System;
using Raven.Client.Replication.Messages;
using Sparrow.Json;

namespace Raven.Server.Documents
{
    public class DocumentConflict
    {
        public LazyStringValue LoweredKey;
        public LazyStringValue Key;
        public BlittableJsonReaderObject Doc;
        public long StorageId;
        public ChangeVectorEntry[] ChangeVector;
        public LazyStringValue Collection;
        public DateTime LastModified;
        public long Etag; // the etag of the current db when this conflict was added

        public static DocumentConflict From(Document doc)
        {
            return new DocumentConflict
            {
                LoweredKey = doc.LoweredKey,
                Key = doc.Key,
                Doc = doc.Data,
                StorageId = doc.StorageId,
                ChangeVector = doc.ChangeVector,
                LastModified = doc.LastModified
            };
        }

        public static DocumentConflict From(DocumentTombstone tombstone)
        {
            return new DocumentConflict
            {
                LoweredKey = tombstone.LoweredKey,
                Key = tombstone.Key,
                Doc = null,
                StorageId = tombstone.StorageId,
                ChangeVector = tombstone.ChangeVector,
                LastModified = tombstone.LastModified
            };
        }

    }
}