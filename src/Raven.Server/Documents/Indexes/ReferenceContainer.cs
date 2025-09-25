using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Sparrow;
using Voron;

namespace Raven.Server.Documents.Indexes;

public sealed class ReferenceContainer
{
    private ReferenceContainer _inverted;
    private readonly Dictionary<Slice, int> _posById;
    private readonly List<List<Slice>> _values;
    private readonly List<Slice> _keys;

    public ReferenceContainer()
    {
        _posById = new Dictionary<Slice, int>(SliceComparer.Instance);
        _values = new List<List<Slice>>(1);
        _keys = new List<Slice>(1);
    }
    
    public List<Slice> GetOrCreateValuesContainer(Slice key)
    {
        ref var valuesIdx = ref CollectionsMarshal.GetValueRefOrAddDefault(_posById, key, out var exists);
        if (exists) 
            return _values[valuesIdx];
        
        valuesIdx = _values.Count;
        var container = new List<Slice>(1); 
        _values.Add(container);
        _keys.Add(key);
        return container;

    }

    public void Clear()
    {
        _posById.Clear();
        _values.Clear();
        _keys.Clear();
        _inverted?.Clear();
    }

    public void PrepareForIndexing(out ReferenceContainer inverted)
    {
        _inverted ??= new ReferenceContainer();
        _inverted.Clear();
        inverted = _inverted;
        foreach (var (key, valuesIdx) in _posById)
        {
            var values = _values[valuesIdx];
            foreach (var value in values)
            {
                _inverted.GetOrCreateValuesContainer(value).Add(key);
            }
        }
        
        SortKeysAndValuesWithDeduplication();
        _inverted.SortKeysAndValuesWithDeduplication();
    }

    private void SortKeysAndValuesWithDeduplication()
    {
        CollectionsMarshal.AsSpan(_keys).Sort(CollectionsMarshal.AsSpan(_values),SliceComparer.Instance);
        foreach (var values in _values)
        {
            if (values.Count <= 1)
                continue;
            
            var valuesAsSpan = CollectionsMarshal.AsSpan(values);
            valuesAsSpan.Sort(SliceComparer.Instance);
            
            int outputIdx = 0;
                    
            for (int i = 1; i < valuesAsSpan.Length; i++)
            {
                if (SliceComparer.Equals(valuesAsSpan[i], valuesAsSpan[outputIdx]) is false)
                {
                    valuesAsSpan[++outputIdx] = valuesAsSpan[i];
                }
            }
            
            CollectionsMarshal.SetCount(values, outputIdx + 1);
        }
    }

    public Enumerator GetEnumerator() => new Enumerator(this);
        
    public ref struct Enumerator(ReferenceContainer container)
    {
        private readonly ReferenceContainer _container = container;
        private int _current = -1;

        public bool MoveNext() => ++_current < _container._values.Count;
        public Slice CurrentKey => _container._keys[_current];
        public Span<Slice> CurrentValues => CollectionsMarshal.AsSpan(_container._values[_current]);
        public List<Slice> CurrentValuesAsList => _container._values[_current];
        public void Reset() => _current = -1;
    }
    
}
