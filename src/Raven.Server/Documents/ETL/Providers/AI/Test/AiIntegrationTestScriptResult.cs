using System.Collections.Generic;
using Raven.Server.Documents.ETL.Test;

namespace Raven.Server.Documents.ETL.Providers.AI.Test;

public sealed class AiIntegrationTestScriptResult : TestEtlScriptResult
{
    public List<AiIntegrationEmbeddingItemValue> EmbeddingItemValues { get; set; }
}
