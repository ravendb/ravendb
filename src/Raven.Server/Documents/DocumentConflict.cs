using System;
using System.Diagnostics;
using Raven.Client.Documents.Replication.Messages;
using Sparrow.Json;

namespace Raven.Server.Documents
{
    public class DocumentConflict
    {
        public LazyStringValue LowerId;
        public LazyStringValue Id;
        public BlittableJsonReaderObject Doc;
        public long StorageId;
        public ChangeVectorEntry[] ChangeVector;
        public LazyStringValue Collection;
        public DateTime LastModified;
        public long Etag; // the etag of the current db when this conflict was added

        public static DocumentConflict From(JsonOperationContext ctx,Document doc)
        {
            if (doc == null)
                return null;

            return new DocumentConflict
            {
                LowerId = doc.LowerId,
                Id = doc.Id,
                Doc = doc.Data,
                StorageId = doc.StorageId,
                ChangeVector = doc.ChangeVector,
                LastModified = doc.LastModified,
                Collection = ctx.GetLazyStringForFieldWithCaching(CollectionName.GetCollectionName(doc.Id, doc.Data))
            };
        }

        public static DocumentConflict From(DocumentTombstone tombstone)
        {
            if (tombstone == null)
                return null;

            Debug.Assert(tombstone.Type == DocumentTombstone.TombstoneType.Document);

            return new DocumentConflict
            {
                LowerId = tombstone.LowerId,
                Id = tombstone.LowerId,
                Doc = null,
                StorageId = tombstone.StorageId,
                ChangeVector = tombstone.ChangeVector,
                LastModified = tombstone.LastModified,
                Collection = tombstone.Collection
            };
        }

    }
}