using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Raven.Server.Documents.ETL.Providers.AI;

public class AiEtlEmbeddingItem
{
    public string DocumentId { get; set; }
    public string DocumentCollectionName { get; set; }
    public bool IsDelete { get; set; }
    
    // PropertyPath -> PropertyValues
    public Dictionary<string, List<AiEtlEmbeddingItemValue>> Values { get; set; }
}

public class AiEtlEmbeddingItemValue
{
    public string TextualValue { get; set; }
    public string ValueEmbeddingsDocumentId { get; set; }

    public void SetPrefix(string prefix)
    {
        Debug.Assert(ValueEmbeddingsSourceAttachmentName is not null, "ValueEmbeddingsSourceAttachmentName is not null");
        ValueEmbeddingsDestinationAttachmentName = $"{prefix}{ValueEmbeddingsSourceAttachmentName}";
    }
    
    public string ValueEmbeddingsSourceAttachmentName { get; set; }

    public string ValueEmbeddingsDestinationAttachmentName { get; private set; }
    
    public ReadOnlyMemory<float> EmbeddingValue { get; set; }
}
