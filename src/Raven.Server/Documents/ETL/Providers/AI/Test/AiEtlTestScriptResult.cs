using System.Collections.Generic;
using Raven.Server.Documents.ETL.Test;

namespace Raven.Server.Documents.ETL.Providers.AI.Test;

public sealed class AiEtlTestScriptResult : TestEtlScriptResult
{
    public List<AiEtlEmbeddingItemValue> EmbeddingItemValues { get; set; }
}
