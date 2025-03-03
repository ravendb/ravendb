using System;

namespace Raven.Server.Documents.AI.Embeddings;

public interface IEmbeddingValue
{
    ReadOnlySpan<byte> GetEmbedding();
}
