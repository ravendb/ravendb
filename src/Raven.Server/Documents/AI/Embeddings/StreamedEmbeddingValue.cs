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
        throw new NotSupportedException($"Getting embedding value isn't supported. Use {nameof(ReadTo)}() instead");
    }

    public IDisposable ReadTo(ByteStringContext allocator, out Memory<byte> mem, out int usedBytes)
    {
        usedBytes = (int)_stream.Length;
        var memScope = allocator.Allocate((int)_stream.Length, out mem);
        _stream.ReadExactly(mem.Span);

        return memScope;
    }
}
