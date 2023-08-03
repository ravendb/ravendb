namespace Raven.Server.Utils;

using System.Collections.Generic;

public sealed class LruDictionary<TKey, TValue> where TKey : notnull
{
    private readonly int _maxCapacity;
    private readonly Dictionary<TKey, (LinkedListNode<TKey> Node, TValue Value)> _cache;
    private readonly LinkedList<TKey> _list;

    public LruDictionary(int maxCapacity)
    {
        _maxCapacity = maxCapacity;
        _cache = new Dictionary<TKey, (LinkedListNode<TKey> Node, TValue Value)>(maxCapacity);
        _list = new LinkedList<TKey>();
    }

    public bool TryGetValue(TKey key, out TValue value)
    {
        if (_cache.TryGetValue(key, out var node) == false)
        {
            value = default;
            return false;
        }

        _list.Remove(node.Node);
        _list.AddFirst(node.Node);

        value = node.Value;
        return true;
    }

    public TValue this[TKey key]
    {
        get
        {
            TryGetValue(key, out var value);
            return value;
        }

        set
        {
            if (_cache.TryGetValue(key, out var node))
            {
                _list.Remove(node.Node);
                _list.AddFirst(node.Node);

                _cache[key] = (node.Node, value);
            }
            else
            {
                if (_cache.Count >= _maxCapacity)
                {
                    var removeKey = _list.Last!.Value;
                    _cache.Remove(removeKey);
                    _list.RemoveLast();
                }

                // add cache
                _cache.Add(key, (_list.AddFirst(key), value));
            }
        }
    }
}
