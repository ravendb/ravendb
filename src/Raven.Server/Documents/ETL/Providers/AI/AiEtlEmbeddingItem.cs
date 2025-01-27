using System;

namespace Raven.Server.Documents.ETL.Providers.AI;

public class AiEtlEmbeddingItem
{
    public string DocumentId { get; set; }
    public string Value { get; set; }
    public string ValuePath { get; set; }

    public string ValueEmbeddingsDocumentId { get; set; }
    public string ValueEmbeddingsAttachmentName { get; set; }

    public ReadOnlyMemory<float> EmbeddingValue { get; set; }
}
