using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.Raven
{
    public class RavenEtlItem : ExtractedItem
    {
        public RavenEtlItem(Document document, string collection) : base(document, collection)
        {
           
        }

        public RavenEtlItem(Tombstone tombstone, string collection) : base(tombstone, collection)
        {
            if (tombstone.Type == Tombstone.TombstoneType.Attachment)
            {
                AttachmentTombstoneId = tombstone.LowerId;
            }
        }

        public LazyStringValue AttachmentTombstoneId { get; protected set; }

        public bool IsAttachmentTombstone => AttachmentTombstoneId != null;
    }
}
