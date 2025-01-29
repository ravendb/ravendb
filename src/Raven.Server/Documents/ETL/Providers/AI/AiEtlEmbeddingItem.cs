using System;
using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.AI;

public class AiEtlEmbeddingItem
{
    public string DocumentId { get; set; }
    public string DocumentCollectionName { get; set; }
    
    // PropertyPath -> PropertyValues
    public Dictionary<string, List<AiEtlEmbeddingItemValue>> Values { get; set; }
}

public class AiEtlEmbeddingItemValue
{
    public string TextualValue { get; set; }
    public string ValueEmbeddingsDocumentId { get; set; }
    public string ValueEmbeddingsAttachmentName { get; set; }
    public ReadOnlyMemory<float> EmbeddingValue { get; set; }
}
