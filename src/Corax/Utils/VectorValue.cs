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

    public ReadOnlySpan<byte> GetEmbedding()
    {
        return _memory.Span.Slice(0, _length);
    }

    public VectorValue()
    {
    }

    public VectorValue(IDisposable memoryScope, Memory<byte> embedding, int? length = null)
    {
        _memoryScope = memoryScope;
        _memory = embedding;
        _length = length ?? embedding.Length;
    }

    public void OverrideLength(int len) => _length = len;


    public void Dispose()
    {
        _memoryScope.Dispose();
    }
}
