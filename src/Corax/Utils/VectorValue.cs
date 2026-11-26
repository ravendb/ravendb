using System;
using System.Buffers;
using Sparrow.Server;

namespace Corax.Utils;

public struct VectorValue : IDisposable
{
    private readonly IDisposable _memoryScope;
    private readonly Memory<byte> _memory;
    private int _length;
    public int Length => _length;

    // Bit-packed Binary embeddings lose the exact dimension count (ceil(dims/8) loses accuracy when not exactly 8 bits
    // 0 means uknown
    private int _sourceDimensions;
    public int SourceDimensions => _sourceDimensions;

    public readonly bool IsNull;
    public static readonly VectorValue Null = new(true);

    public ReadOnlySpan<byte> GetEmbedding()
    {
        return _memory.Span.Slice(0, _length);
    }

    public Memory<byte> GetEmbeddingMemory()
    {
        return _memory.Slice(0, _length);
    }
    
    private VectorValue(bool isNull)
    {
        IsNull = isNull;
    }

    public VectorValue(IDisposable memoryScope, Memory<byte> embedding, int? length = null)
    {
        _memoryScope = memoryScope;
        _memory = embedding;
        _length = length ?? embedding.Length;
    }

    public void OverrideLength(int len) => _length = len;

    public void SetSourceDimensions(int dimensions) => _sourceDimensions = dimensions;

    public void Dispose()
    {
        _memoryScope?.Dispose();
    }
}
