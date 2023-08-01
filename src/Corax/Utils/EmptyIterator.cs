using System;
using Sparrow;

namespace Corax.Utils;

public sealed class EmptyIterator : IReadOnlySpanIndexer
{
    public static readonly EmptyIterator Instance = new();

    private EmptyIterator()
    {
    }

    public int Length => 0;

    public bool IsNull(int i) => throw new IndexOutOfRangeException($"{nameof(EmptyIterator)} is empty by definition.");
    
    public ReadOnlySpan<byte> this[int i] => throw new IndexOutOfRangeException($"{nameof(EmptyIterator)} is empty by definition.");
}
