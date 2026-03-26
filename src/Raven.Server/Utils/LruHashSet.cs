namespace Raven.Server.Utils;

public sealed class LruHashSet<TKey> where TKey : notnull
{
    private readonly LruDictionary<TKey, object> _cache;

    public LruHashSet(int maxCapacity)
    {
        _cache = new LruDictionary<TKey, object>(maxCapacity);
    }

    public bool Contains(TKey key) => _cache.TryGetValue(key, out _);

    public bool Add(TKey key)
    {
        if (_cache.TryGetValue(key, out _))
        {
            return false;
        }

        _cache[key] = null;
        return true;
    }

    public void Clear() => _cache.Clear();
}
