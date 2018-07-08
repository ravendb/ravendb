using System;
using System.Diagnostics;
using Sparrow.Json;

namespace Raven.Server.Documents
{
    public class DocumentConflict
    {
        public LazyStringValue LowerId;
        public LazyStringValue Id;
        public BlittableJsonReaderObject Doc;
        public long StorageId;
        public string ChangeVector;
        public LazyStringValue Collection;
        public DateTime LastModified;
        public DocumentFlags Flags;
        public long Etag; // the etag of the current db when this conflict was added

        public DocumentConflict Clone()
        {
            return new DocumentConflict
            {
                LowerId = LowerId,
                Id = Id,
                Doc = Doc,
                StorageId = StorageId,
                ChangeVector = ChangeVector,
                Collection = Collection,
                LastModified = LastModified,
                Flags = Flags,
                Etag = Etag
            };
        }

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
                Collection = ctx.GetLazyStringForFieldWithCaching(CollectionName.GetCollectionName(doc.Data)),
                Flags = doc.Flags
            };
        }

        public static DocumentConflict From(Tombstone tombstone)
        {
            if (tombstone == null)
                return null;

            Debug.Assert(tombstone.Type == Tombstone.TombstoneType.Document);

            return new DocumentConflict
            {
                LowerId = tombstone.LowerId,
                Id = tombstone.LowerId,
                Doc = null,
                StorageId = tombstone.StorageId,
                ChangeVector = tombstone.ChangeVector,
                LastModified = tombstone.LastModified,
                Collection = tombstone.Collection,
                Flags = tombstone.Flags
            };
        }

    }
}
