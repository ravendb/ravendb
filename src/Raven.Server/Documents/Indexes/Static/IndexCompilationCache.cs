using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Sparrow;

namespace Raven.Server.Documents.Indexes.Static
{
    public class IndexCompilationCache
    {
        /// <summary>
        /// This is a static because creating indexes is expensive, we want to cache them 
        /// as much as possible, even across different databases and database instantiation. Per process,
        /// we are going to have a single cache for all indexes. This also plays nice with testing, which 
        /// will build up and tear down a server frequently, so we can still reduce the cost of compiling 
        /// the indexes.
        /// </summary>
        private static readonly ConcurrentDictionary<IndexCacheKey, Lazy<StaticIndexBase>> _staticIndexCache = new ConcurrentDictionary<IndexCacheKey, Lazy<StaticIndexBase>>();

        private readonly ConcurrentDictionary<IndexCacheKey, Lazy<StaticIndexBase>> _localIndexCache = new ConcurrentDictionary<IndexCacheKey, Lazy<StaticIndexBase>>();

        public StaticIndexBase GetIndexInstance(IndexDefinition definition, RavenConfiguration configuration)
        {
            var type = definition.DetectStaticIndexType();
            var key = GetCacheKey(definition, type);
            var cache = GetCache(type);

            Lazy<StaticIndexBase> result = cache.GetOrAdd(key, k => new Lazy<StaticIndexBase>(() => GenerateIndex(k, definition, configuration, type)));

            try
            {
                return result.Value;
            }
            catch (Exception)
            {
                cache.TryRemove(key, out _);
                throw;
            }
        }

        public void Remove(IndexCacheKey key)
        {
            if (key == null)
                return;

            // we are only interested in removing instances from local cache e.g. JavaScript Indexes
            _localIndexCache.TryRemove(key, out _);
        }

        private ConcurrentDictionary<IndexCacheKey, Lazy<StaticIndexBase>> GetCache(IndexType type)
        {
            switch (type)
            {
                case IndexType.Map:
                case IndexType.MapReduce:
                    return _staticIndexCache;
                case IndexType.JavaScriptMap:
                case IndexType.JavaScriptMapReduce:
                    return _localIndexCache;
                default:
                    throw new NotSupportedException($"Unknown index type '{type}'.");
            }
        }

        private static IndexCacheKey GetCacheKey(IndexDefinition definition, IndexType type)
        {
            var list = new List<string>();

            if (type.IsJavaScript())
            {
                list.Add(definition.Name);
            }

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

            return new IndexCacheKey(list);
        }

        private static StaticIndexBase GenerateIndex(IndexCacheKey cacheKey, IndexDefinition definition, RavenConfiguration configuration, IndexType type)
        {
            switch (type)
            {
                case IndexType.None:
                case IndexType.AutoMap:
                case IndexType.AutoMapReduce:
                case IndexType.Map:
                case IndexType.MapReduce:
                case IndexType.Faulty:
                    return IndexCompiler.Compile(cacheKey, definition);
                case IndexType.JavaScriptMap:
                case IndexType.JavaScriptMapReduce:
                    return new JavaScriptIndex(cacheKey, definition, configuration);
                default:
                    throw new ArgumentOutOfRangeException($"Can't generate index of unknown type {definition.DetectStaticIndexType()}");
            }
        }
    }

    public class IndexCacheKey : IEquatable<IndexCacheKey>
    {
        private readonly int _hash;
        private readonly List<string> _items;

        public unsafe IndexCacheKey(List<string> items)
        {
            _items = items;

            byte[] temp = null;
            var ctx = Hashing.Streamed.XXHash32.BeginProcess();
            foreach (var str in items)
            {
                fixed (char* buffer = str)
                {
                    var toProcess = str.Length;
                    var current = buffer;
                    do
                    {
                        if (toProcess < Hashing.Streamed.XXHash32.Alignment)
                        {
                            if (temp == null)
                                temp = new byte[Hashing.Streamed.XXHash32.Alignment];

                            fixed (byte* tempBuffer = temp)
                            {
                                Memory.Set(tempBuffer, 0, temp.Length);
                                Memory.Copy(tempBuffer, (byte*)current, toProcess);

                                ctx = Hashing.Streamed.XXHash32.Process(ctx, tempBuffer, temp.Length);
                                break;
                            }
                        }

                        ctx = Hashing.Streamed.XXHash32.Process(ctx, (byte*)current, Hashing.Streamed.XXHash32.Alignment);
                        toProcess -= Hashing.Streamed.XXHash32.Alignment;
                        current += Hashing.Streamed.XXHash32.Alignment;
                    }
                    while (toProcess > 0);
                }
            }
            _hash = (int)Hashing.Streamed.XXHash32.EndProcess(ctx);
        }

        public override bool Equals(object obj)
        {
            if (obj is IndexCacheKey cacheKey)
                return Equals(cacheKey);
            return false;
        }

        public bool Equals(IndexCacheKey other)
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
