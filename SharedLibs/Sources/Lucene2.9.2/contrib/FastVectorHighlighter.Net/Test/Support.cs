using System;
using System.Collections.Generic;
using System.Text;

namespace Lucene.Net.Search.Vectorhighlight
{
    static class Extensions
    {
        public static V Get<K, V>(this Dictionary<K, V> list, K key)
        {
            if (key == null) return default(V);
            V v = default(V);
            list.TryGetValue(key, out v);
            return v;
        }
    }

    public class HashSet<T> : Dictionary<T, T>
    {
        public void Add(T key)
        {
            base.Add(key, key);
        }
    }
}

namespace System.Runtime.CompilerServices
{
    [AttributeUsage(AttributeTargets.Method)]
    public sealed class ExtensionAttribute : Attribute
    {
        public ExtensionAttribute() { }
    }
}
