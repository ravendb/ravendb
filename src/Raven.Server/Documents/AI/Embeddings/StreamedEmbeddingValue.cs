using System;
using System.IO;
using Sparrow.Server;

namespace Raven.Server.Documents.AI.Embeddings;

public class StreamedEmbeddingValue : IEmbeddingValue
{
    private readonly Stream _stream;

    public StreamedEmbeddingValue(Stream stream)
    {
        _stream = stream;
    }

    public ReadOnlySpan<byte> GetEmbedding()
    {
        throw new NotSupportedException($"Getting embedding value from {nameof(StreamedEmbeddingValue)} isn't supported. Use {nameof(GetEmbeddingStream)}() instead");
    }

    public Stream GetEmbeddingStream()
    {
        return _stream;
    }
}
