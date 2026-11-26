using System;
using Voron;

namespace Raven.Server.Documents.Indexes;

public sealed class ReferenceContainer
{
    private static readonly Comparison<Slice> CompareSlices = SliceComparer.Compare;

    private ReferenceContainer _inverted;
    private Slice[] _keys;
    private Slice[] _values;
    private int _count;

    public ReferenceContainer() : this(16)
    {
    }

    private ReferenceContainer(int capacity)
    {
        _keys = capacity == 0 ? [] : new Slice[capacity];
        _values = capacity == 0 ? [] : new Slice[capacity];
    }

    public void Add(Slice key, Slice value)
    {
        if (_count == _keys.Length)
            EnsureCapacity(Math.Max(_count + 1, _keys.Length * 2));

        _keys[_count] = key;
        _values[_count] = value;
        _count++;
    }

    private void EnsureCapacity(int capacity)
    {
        if (_keys.Length >= capacity)
            return;

        Array.Resize(ref _keys, capacity);
        Array.Resize(ref _values, capacity);
    }

    public void Clear()
    {
        _count = 0;
        _inverted?.Clear();
    }

    public void PrepareForIndexing(out ReferenceContainer inverted)
    {
        SortKeysAndValuesWithDeduplication();

        _inverted ??= new ReferenceContainer(0);
        _inverted.Clear();
        _inverted.EnsureCapacity(_count);

        for (int i = 0; i < _count; i++)
            _inverted.Add(_values[i], _keys[i]);

        _inverted.SortKeysAndValuesWithDeduplication();
        inverted = _inverted;
    }

    private void SortKeysAndValuesWithDeduplication()
    {
        if (_count == 0)
            return;

        var keys = _keys.AsSpan(0, _count);
        var values = _values.AsSpan(0, _count);
        keys.Sort(values, SliceComparer.Instance);

        // every key's values are sorted and deduplicated in place, the output never overtakes the input
        int outputIdx = 0;
        int start = 0;
        while (start < keys.Length)
        {
            var key = keys[start];
            int end = start + 1;
            while (end < keys.Length && SliceComparer.Equals(keys[end], key))
                end++;

            var keyValues = values[start..end];
            if (keyValues.Length > 1)
                keyValues.Sort(CompareSlices);

            int firstOutputOfKey = outputIdx;
            for (int i = start; i < end; i++)
            {
                if (outputIdx > firstOutputOfKey && SliceComparer.Equals(values[i], values[outputIdx - 1]))
                    continue;

                keys[outputIdx] = key;
                values[outputIdx] = values[i];
                outputIdx++;
            }

            start = end;
        }

        _count = outputIdx;
    }

    // Valid after PrepareForIndexing, which groups the values of each key together
    public Enumerator GetEnumerator() => new Enumerator(this);

    public ref struct Enumerator(ReferenceContainer container)
    {
        private readonly ReferenceContainer _container = container;
        private int _start;
        private int _end;

        public bool MoveNext()
        {
            _start = _end;
            if (_start >= _container._count)
                return false;

            var key = _container._keys[_start];
            _end = _start + 1;
            while (_end < _container._count && SliceComparer.Equals(_container._keys[_end], key))
                _end++;

            return true;
        }

        public Slice CurrentKey => _container._keys[_start];
        public Span<Slice> CurrentValues => _container._values.AsSpan(_start, _end - _start);

        public void Reset()
        {
            _start = 0;
            _end = 0;
        }
    }
}
