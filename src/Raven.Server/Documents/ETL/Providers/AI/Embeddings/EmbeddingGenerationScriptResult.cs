using System.Collections.Generic;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.AI.Embeddings;

namespace Raven.Server.Documents.ETL.Providers.AI.Embeddings;

public record EmbeddingGenerationScriptResult(string DocumentId, string DocumentCollectionName)
{
    // PropertyPath -> [PropertyValues]
    public Dictionary<string, List<(string,ChunkingOptions)>> Fields = new();

}

