using System.Collections.Generic;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.Documents.ETL.Test;

namespace Raven.Server.Documents.ETL.Providers.AI.Embeddings.Test;

public sealed class EmbeddingsGenerationTestScriptResult : TestEtlScriptResult
{
    public List<EmbeddingGenerationItem> EmbeddingItemValues { get; set; }
}
