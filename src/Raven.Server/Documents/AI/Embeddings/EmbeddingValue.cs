using System;
using System.Runtime.InteropServices;
using Sparrow.Server.Utils;

namespace Raven.Server.Documents.AI.Embeddings;

public class EmbeddingValue : IEmbeddingValue
{
    private readonly ReadOnlyMemory<float> _value;
    private readonly int _usedBytes;

    public EmbeddingValue(ReadOnlyMemory<float> value, int usedBytes)
    {
        _value = value;
        _usedBytes = usedBytes;
    }

    public ReadOnlySpan<byte> GetEmbedding()
    {
        return MemoryMarshal.Cast<float, byte>(_value.Span)[.._usedBytes];
    }

    public ReadOnlyMemoryStream<float> GetEmbeddingStream()
    {
        return new ReadOnlyMemoryStream<float>(_value, _usedBytes);
    }
}
