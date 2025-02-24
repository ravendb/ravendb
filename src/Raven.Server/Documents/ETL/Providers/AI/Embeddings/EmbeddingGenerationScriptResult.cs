using System.Collections.Generic;
using Raven.Server.Documents.AI.Embeddings;

namespace Raven.Server.Documents.ETL.Providers.AI.Embeddings;

public class EmbeddingGenerationScriptResult
{
    public string DocumentId { get; set; }
    public string DocumentCollectionName { get; set; }
    public bool IsDelete { get; set; }

    // PropertyPath -> PropertyValues
    public Dictionary<string, List<EmbeddingGenerationItem>> Values { get; set; }
}
