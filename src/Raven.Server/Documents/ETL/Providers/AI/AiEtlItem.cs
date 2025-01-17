using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.AI;

public sealed class AiEtlItem : ExtractedItem
{
    public AiEtlItem(Document document, string collection) : base(document, collection, EtlItemType.Document)
    {
           
    }

    public AiEtlItem(Tombstone tombstone, string collection, EtlItemType type) : base(tombstone, collection, type)
    {
        if (tombstone.Type == Tombstone.TombstoneType.Attachment)
        {
            AttachmentTombstoneId = tombstone.LowerId;
        }
    }
    
    public LazyStringValue AttachmentTombstoneId { get; }

    public bool IsAttachmentTombstone => AttachmentTombstoneId != null;
}
