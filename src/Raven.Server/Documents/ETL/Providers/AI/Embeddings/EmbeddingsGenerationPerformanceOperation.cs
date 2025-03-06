using System;
using Raven.Server.Documents.ETL.Stats;

namespace Raven.Server.Documents.ETL.Providers.AI.Embeddings;

public sealed class EmbeddingsGenerationPerformanceOperation : EtlPerformanceOperation
{
    public EmbeddingsGenerationPerformanceOperation(TimeSpan duration)
        : base(duration)
    {
    }

    public int NumberOfGeneratedEmbeddings { get; set; }
    public int NumberOfPutEmbeddingDocuments { get; set; }
    public int NumberOfDeletedEmbeddingDocuments { get; set; }
}
