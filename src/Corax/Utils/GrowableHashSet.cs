using System.Collections.Generic;

namespace Corax.Utils;

public sealed class GrowableHashSet<TItem>
{
    private List<HashSet<TItem>> _hashSetsBucket;
    private HashSet<TItem> _newestHashSet;
    private readonly int _maxSizePerCollection;
    private readonly IEqualityComparer<TItem> _comparer;

    public long Count
    {
        get
        {
            long result = _newestHashSet.Count;
            if (_hashSetsBucket != null)
            {
                foreach (var hashBucket in _hashSetsBucket)
                    result += hashBucket.Count;
            }

            return result;
        }
    }

    public bool HasMultipleHashSets => _hashSetsBucket != null;

    public GrowableHashSet(IEqualityComparer<TItem> comparer = null, int? maxSizePerCollection = null)
    {
        _comparer = comparer;
        _hashSetsBucket = null;
        _maxSizePerCollection = maxSizePerCollection ?? int.MaxValue;
        CreateNewHashSet();
    }

    public bool Add(TItem item)
    {
        if (_newestHashSet!.Count >= _maxSizePerCollection)
            UnlikelyGrowBuffer();

        if (_hashSetsBucket != null && Contains(item))
            return false;

        return _newestHashSet.Add(item);
    }

    private void UnlikelyGrowBuffer()
    {
        _hashSetsBucket ??= new();
        _hashSetsBucket.Add(_newestHashSet);
        CreateNewHashSet();
    }

    public bool Contains(TItem item)
    {
        if (_hashSetsBucket != null)
        {
            foreach (var hashSet in _hashSetsBucket)
                if (hashSet.Contains(item))
                    return true;
        }

        return _newestHashSet!.Contains(item);
    }

    private void CreateNewHashSet()
    {
        if (_comparer == null)
            _newestHashSet = new();
        else
            _newestHashSet = new(_comparer);
    }
}
