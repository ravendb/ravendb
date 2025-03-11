using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.AI;

public sealed class EmbeddingsGenerationItem : ExtractedItem
{
    public EmbeddingsGenerationItem(Document document, string collection) : base(document, collection, EtlItemType.Document)
    {
           
    }

    public EmbeddingsGenerationItem(Tombstone tombstone, string collection, EtlItemType type) : base(tombstone, collection, type)
    {
        if (tombstone.Type == Tombstone.TombstoneType.Attachment)
        {
            AttachmentTombstoneId = tombstone.LowerId;
        }
    }
    
    public LazyStringValue AttachmentTombstoneId { get; }

    public bool IsAttachmentTombstone => AttachmentTombstoneId != null;
}
