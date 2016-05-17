using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Raven.Client.Indexing;
using Sparrow;

namespace Raven.Server.Documents.Indexes.Static
{
    /// <summary>
    /// This is a static class because creating indexes is expensive, we want to cache them 
    /// as much as possible, even across different databases and database instansiation. Per process,
    /// we are going to have a single cache for all indexes. This also plays nice with testing, which 
    /// will build up and tear down a server frequently, so we can still reduce the cost of compiling 
    /// the indexes.
    /// </summary>
    public static class IndexCompilationCache
    {
        private static readonly ConcurrentDictionary<CacheKey, Lazy<StaticIndexBase>> Cache
           = new ConcurrentDictionary<CacheKey, Lazy<StaticIndexBase>>();

        public static StaticIndexBase GetIndexInstance(IndexDefinition definition)
        {
            var list = new List<string>();
            list.AddRange(definition.Maps);
            if (definition.Reduce != null)
                list.Add(definition.Reduce);
            var key = new CacheKey(list);
            Func<StaticIndexBase> createIndex = () => new StaticIndexCompiler().Compile(definition);
            var result = Cache.GetOrAdd(key, _ => new Lazy<StaticIndexBase>(createIndex));
            return result.Value;
        }

        private class CacheKey : IEquatable<CacheKey>
        {
            private readonly int _hash;
            private readonly List<string> _items;

            public unsafe CacheKey(List<string> items )
            {
                _items = items;
                var ctx = Hashing.Streamed.XXHash32.BeginProcess();
                foreach (var str in items)
                {
                    fixed (char* p = str)
                    {
                        Hashing.Streamed.XXHash32.Process(ctx, (byte*)p, str.Length * sizeof(char));
                    }
                }
                _hash = (int)Hashing.Streamed.XXHash32.EndProcess(ctx);
            }

            public override bool Equals(object obj)
            {
                var cacheKey = obj as CacheKey;
                if (cacheKey != null)
                    return Equals(cacheKey);
                return false;
            }

            public bool Equals(CacheKey other)
            {
                if (_items.Count != other._items.Count)
                    return false;
                for (int i = 0; i < _items.Count; i++)
                {
                    if (_items[i] != other._items[i])
                        return false;
                }
                return true;
            }

            public override int GetHashCode()
            {
                return _hash;
            }
        }
    }
}