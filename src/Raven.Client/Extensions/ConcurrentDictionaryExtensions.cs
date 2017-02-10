using System.Collections.Concurrent;

namespace Raven.Client.Extensions
{
    public static class ConcurrentDictionaryExtensions
    {
        public static TVal GetOrDefault<TKey,TVal>(this ConcurrentDictionary<TKey, TVal> self, TKey key, TVal value = default(TVal))
        {
            TVal fromDic;
            if (self.TryGetValue(key, out fromDic))
                return fromDic;
            return value;
        }
    }
}
