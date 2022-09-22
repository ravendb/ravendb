using System;
using System.Collections.Generic;
using Sparrow;
using Sparrow.Json;

namespace Corax.Utils;

public unsafe struct BlittableIterator : IReadOnlySpanIndexer, IDisposable
{
    private readonly List<BlittableJsonReaderObject> _values;
    private readonly List<IDisposable> _toDispose;

    public BlittableIterator(List<BlittableJsonReaderObject> values)
    {
        _values = values;
        _toDispose = new();
    }

    public int Length => _values.Count;

    public bool IsNull(int i)
    {
        if (i < 0 || i >= Length)
            throw new ArgumentOutOfRangeException();

        return false;
    }

    public ReadOnlySpan<byte> this[int i] => Memory(i);

    private ReadOnlySpan<byte> Memory(int id)
    {
        var reader = _values[id];
        if (reader.HasParent == false)
        {
            return new ReadOnlySpan<byte>(reader.BasePointer, reader.Size);
        }

        var clonedBlittable = reader.CloneOnTheSameContext();
        _toDispose.Add(clonedBlittable);
        return new ReadOnlySpan<byte>(clonedBlittable.BasePointer, clonedBlittable.Size);
    }

    public void Dispose()
    {
        foreach (var item in _toDispose)
        {
            item?.Dispose();
        }
    }
}
