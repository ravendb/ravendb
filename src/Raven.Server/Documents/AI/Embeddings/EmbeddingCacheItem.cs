using System;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Server.Documents.ETL.Providers.AI;

namespace Raven.Server.Documents.AI.Embeddings;

public class EmbeddingCacheItem(
    string textualValue,
    ReadOnlyMemory<byte> embeddingValue,
    VectorEmbeddingType quantization,
    AiConnectionStringIdentifier connectionStringIdentifier,
    IDisposable returnEmbedding) : IDisposable
{
    public ReadOnlyMemory<byte> EmbeddingValue = embeddingValue;
    public string TextualValue = textualValue;

    public AiConnectionStringIdentifier ConnectionStringIdentifier = connectionStringIdentifier;
    public VectorEmbeddingType Quantization = quantization;

    public void Dispose()
    {
        returnEmbedding.Dispose();
    }
}
