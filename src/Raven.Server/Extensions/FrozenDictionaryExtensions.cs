using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Collections.Generic;

namespace Raven.Server.Extensions
{
    internal static class FrozenDictionaryExtensions
    {
        public static FrozenDictionary<TKey, TValue> ToFrozenDictionaryWithSameComparer<TKey, TValue>(this Dictionary<TKey, TValue> source)
            where TKey : notnull => source.ToFrozenDictionary(source.Comparer);

        public static FrozenDictionary<TKey, TValue> ToFrozenDictionaryWithSameComparer<TKey, TValue>(this ConcurrentDictionary<TKey, TValue> source)
            where TKey : notnull => source.ToFrozenDictionary(source.Comparer);
    }
}
