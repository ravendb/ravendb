using System;
using System.Buffers;
using Sparrow.Server;
using Voron.Data.Graphs;

namespace Corax.Utils;

public struct VectorValue : IDisposable
{
    private readonly IDisposable _memoryScope;
    private readonly Memory<byte> _memory;
    private int _length;
    public int Length => _length;
    
    public readonly bool IsNull;
    public static readonly VectorValue Null = new(true);

    public readonly VectorEmbeddingType Type;
    
    public ReadOnlySpan<byte> GetEmbedding() =>_memory.Span.Slice(0, _length);

    public VectorValue()
    {
    }
    
    private VectorValue(bool isNull)
    {
        IsNull = isNull;
    }

    public VectorValue(IDisposable memoryScope, Memory<byte> embedding, VectorEmbeddingType type, int? length = null) : this(false)
    {
        Type = type;
        _memoryScope = memoryScope;
        _memory = embedding;
        _length = length ?? embedding.Length;
    }

    public void OverrideLength(int len) => _length = len;
    
    public void Dispose()
    {
        _memoryScope?.Dispose();
    }
}
