using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Sparrow;

namespace Raven.Server.Documents.Indexes.Static
{
    /// <summary>
    /// This is a static class because creating indexes is expensive, we want to cache them 
    /// as much as possible, even across different databases and database instantiation. Per process,
    /// we are going to have a single cache for all indexes. This also plays nice with testing, which 
    /// will build up and tear down a server frequently, so we can still reduce the cost of compiling 
    /// the indexes.
    /// </summary>
    public static class IndexCompilationCache
    {
        private static readonly ConcurrentDictionary<CacheKey, Lazy<StaticIndexBase>> _indexCache = new ConcurrentDictionary<CacheKey, Lazy<StaticIndexBase>>();

        public static StaticIndexBase GetIndexInstance(IndexDefinition definition, RavenConfiguration configuration)
        {
            var type = definition.DetectStaticIndexType();
            if (type.IsJavaScript())
                return GenerateIndex(definition, configuration, type);

            var key = GetCacheKey(definition);

            Lazy<StaticIndexBase> result = _indexCache.GetOrAdd(key, _ => new Lazy<StaticIndexBase>(() => GenerateIndex(definition, configuration, type)));

            try
            {
                return result.Value;
            }
            catch (Exception)
            {
                _indexCache.TryRemove(key, out _);
                throw;
            }
        }

        private static CacheKey GetCacheKey(IndexDefinition definition)
        {
            var list = new List<string>();

            list.AddRange(definition.Maps);
            if (definition.Reduce != null)
                list.Add(definition.Reduce);
            if (definition.AdditionalSources != null)
            {
                foreach (var kvp in definition.AdditionalSources.OrderBy(x => x.Key))
                {
                    list.Add(kvp.Key);
                    list.Add(kvp.Value);
                }
            }

            return new CacheKey(list);
        }

        internal static StaticIndexBase GenerateIndex(IndexDefinition definition, RavenConfiguration configuration, IndexType type)
        {
            switch (type)
            {
                case IndexType.None:
                case IndexType.AutoMap:
                case IndexType.AutoMapReduce:
                case IndexType.Map:
                case IndexType.MapReduce:
                case IndexType.Faulty:
                    return IndexCompiler.Compile(definition);
                case IndexType.JavaScriptMap:
                case IndexType.JavaScriptMapReduce:
                    return new JavaScriptIndex(definition, configuration);
                default:
                    throw new ArgumentOutOfRangeException($"Can't generate index of unknown type {definition.DetectStaticIndexType()}");
            }
        }

        private class CacheKey : IEquatable<CacheKey>
        {
            private readonly int _hash;
            private readonly List<string> _items;

            public CacheKey(List<string> items)
            {
                _items = items;

                var hasher = new HashCode();
                foreach (var item in items)
                {
                    hasher.Add(item);
                }

                _hash = hasher.ToHashCode();
            }

            public override bool Equals(object obj)
            {
                if (obj is CacheKey cacheKey)
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
