using System.Collections.Generic;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.Documents.ETL.Test;

namespace Raven.Server.Documents.ETL.Providers.AI.Test;

public sealed class AiIntegrationTestScriptResult : TestEtlScriptResult
{
    public List<EmbeddingGenerationItem> EmbeddingItemValues { get; set; }
}
