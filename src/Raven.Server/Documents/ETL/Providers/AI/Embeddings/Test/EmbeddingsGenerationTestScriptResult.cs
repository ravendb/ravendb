using System;
using System.Collections.Generic;
using Raven.Server.Documents.ETL.Test;

namespace Raven.Server.Documents.ETL.Providers.AI.Embeddings.Test;

public sealed class EmbeddingsGenerationTestScriptResult : TestEtlScriptResult
{
    public record Item(string Value)
    {
        public ReadOnlyMemory<byte> Embeddings;
    }

    public Dictionary<string, List<Item>> Results { get; set; }
}

