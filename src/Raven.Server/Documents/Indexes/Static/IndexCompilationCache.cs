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
    /// as much as possible, even across different databases and database instansiation. Per process,
    /// we are going to have a single cache for all indexes. This also plays nice with testing, which 
    /// will build up and tear down a server frequently, so we can still reduce the cost of compiling 
    /// the indexes.
    /// </summary>
    public static class IndexCompilationCache
    {
        private static readonly ConcurrentDictionary<CacheKey, Lazy<StaticIndexBase>> IndexCache = new ConcurrentDictionary<CacheKey, Lazy<StaticIndexBase>>();

        public static StaticIndexBase GetIndexInstance(IndexDefinition definition, RavenConfiguration configuration)
        {
            var list = new List<string>();
            var type = definition.DetectStaticIndexType();

            // we do not want to reuse javascript indexes definitions, because they have the Engine, which can't be reused.
            // we also need to make sure that the same index is not used in two different databases under the same definitions
            // todo: when porting javascript indexes to use ScriptRunner (RavenDB-10918), check if we can generate and/or pool the single runs, allowing to remove this code
            if (type.IsJavaScript())
            {
                list.Add(definition.Name);
                list.Add(configuration.ResourceName);
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

            var key = new CacheKey(list);
            Lazy<StaticIndexBase> result = IndexCache.GetOrAdd(key, _ => new Lazy<StaticIndexBase>(() => GenerateIndex(definition, configuration, type)));

            try
            {
                return result.Value;
            }
            catch (Exception)
            {
                IndexCache.TryRemove(key, out _);
                throw;
            }
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

        internal class CacheKey : IEquatable<CacheKey>
        {
            private readonly int _hash;
            private readonly List<string> _items;

            public unsafe CacheKey(List<string> items)
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
