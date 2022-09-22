using System;
using System.Collections.Generic;
using Sparrow;
using Sparrow.Server;

namespace Corax.Utils;

public struct ByteStringIterator : IReadOnlySpanIndexer
{
    private readonly List<ByteString> _values;

    public ByteStringIterator(List<ByteString> values)
    {
        _values = values;
    }

    public int Length => _values.Count;

    public bool IsNull(int i)
    {
        if (i < 0 || i >= Length)
            throw new ArgumentOutOfRangeException();

        return !_values[i].HasValue;
    }

    public ReadOnlySpan<byte> this[int i] => IsNull(i) ? ReadOnlySpan<byte>.Empty : _values[i].ToReadOnlySpan();
}
