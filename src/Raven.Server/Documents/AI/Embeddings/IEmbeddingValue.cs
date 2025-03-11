using System;
using System.IO;

namespace Raven.Server.Documents.AI.Embeddings;

public interface IEmbeddingValue
{
    ReadOnlySpan<byte> GetEmbedding();
    
    Stream GetEmbeddingStream();
}
