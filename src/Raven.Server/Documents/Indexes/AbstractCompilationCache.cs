using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Indexes
{
    public abstract class AbstractCompilationCache<TType>
        where TType : class
    {
        private readonly ConcurrentDictionary<CacheKey, Lazy<TType>> _compilationCache = new();

        internal readonly ConcurrentDictionary<CacheKey, Lazy<TType>> ServerWideCache = new();

        internal readonly ConcurrentDictionary<CacheKey, Lazy<TType>> PerDatabaseCache = new();

        public TType GetItemType(string name, string databaseName)
        {
            var key = CacheKey.ForDatabase(databaseName, name, code: null);

            if (TryGetItemType(PerDatabaseCache, key, out var type))
                return type;

            key = CacheKey.ForServerWide(name, code: null);

            if (TryGetItemType(ServerWideCache, key, out type))
                return type;

            return null;
        }

        private static bool TryGetItemType(ConcurrentDictionary<CacheKey, Lazy<TType>> cache, CacheKey key, out TType type)
        {
            type = null;

            if (cache.TryGetValue(key, out var result) == false)
                return false;

            try
            {
                type = result.Value;
                return true;
            }
            catch (Exception)
            {
                cache.TryRemove(key, out _);
                throw;
            }
        }

        public void AddServerWideItem(string name, string code)
        {
            var key = CacheKey.ForServerWide(name, code);
            AddItemInternal(ServerWideCache, key, name, code);
        }

        public void RemoveServerWideItem(string name)
        {
            foreach (var kvp in ServerWideCache.ForceEnumerateInThreadSafeManner())
            {
                if (string.Equals(kvp.Key.Name, name, StringComparison.OrdinalIgnoreCase) == false)
                    continue;

                ServerWideCache.TryRemove(kvp.Key, out _);
            }
        }

        protected abstract bool DatabaseRecordContainsItem(RawDatabaseRecord databaseRecord, string name);

        protected abstract IEnumerable<(string Name, string Code)> GetItemsFromDatabaseRecord(RawDatabaseRecord databaseRecord);

        protected abstract IEnumerable<(string Name, string Code)> GetItemsFromCluster(ServerStore serverStore, TransactionOperationContext context);

        public void AddItems(RawDatabaseRecord databaseRecord)
        {
            foreach (var kvp in PerDatabaseCache.ForceEnumerateInThreadSafeManner())
            {
                var key = kvp.Key;
                if (string.Equals(key.ResourceName, databaseRecord.DatabaseName, StringComparison.OrdinalIgnoreCase) == false)
                    continue;

                if (DatabaseRecordContainsItem(databaseRecord, key.Name))
                    continue;

                PerDatabaseCache.TryRemove(key, out _);
            }

            var aggregator = new ExceptionAggregator("Could not update cache");

            foreach (var kvp in GetItemsFromDatabaseRecord(databaseRecord))
            {
                aggregator.Execute(() =>
                {
                    var name = kvp.Name;
                    var code = kvp.Code;
                    var key = CacheKey.ForDatabase(databaseRecord.DatabaseName, name, code);

                    AddItemInternal(PerDatabaseCache, key, name, code);
                });
            }

            aggregator.ThrowIfNeeded();
        }

        public void AddServerWideItems(ServerStore serverStore)
        {
            using (serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                foreach (var kvp in GetItemsFromCluster(serverStore, context))
                {
                    var name = kvp.Name;
                    var code = kvp.Code;
                    var key = CacheKey.ForServerWide(name, code);

                    AddItemInternal(ServerWideCache, key, name, code);
                }
            }
        }

        public void Clear(string resourceName)
        {
            foreach (var kvp in PerDatabaseCache.ForceEnumerateInThreadSafeManner())
            {
                var key = kvp.Key;
                if (string.Equals(key.ResourceName, resourceName, StringComparison.OrdinalIgnoreCase) == false)
                    continue;

                PerDatabaseCache.TryRemove(key, out _);
            }
        }

        private void AddItemInternal(ConcurrentDictionary<CacheKey, Lazy<TType>> cache, CacheKey key, string name, string code)
        {
            var result = cache.GetOrAdd(key, _ => new Lazy<TType>(() => CompileAnalyzer(name, code)));
            if (result.IsValueCreated)
                return;

            try
            {
                // compile item
                _ = result.Value;
            }
            catch (Exception)
            {
                cache.TryRemove(key, out _);
                throw;
            }
        }

        protected abstract TType CompileItem(string name, string code);

        private TType CompileAnalyzer(string name, string code)
        {
            var key = CacheKey.ForCompilation(name, code);
            var result = _compilationCache.GetOrAdd(key, _ => new Lazy<TType>(() => CompileItem(name, code)));

            try
            {
                return result.Value;
            }
            catch (Exception)
            {
                _compilationCache.TryRemove(key, out _);
                throw;
            }
        }

        internal class CacheKey : IEquatable<CacheKey>
        {
            public readonly string ResourceName;
            public readonly string Name;
            private readonly string _code;

            private CacheKey(string resourceName, string name, string code)
            {
                ResourceName = resourceName;
                Name = name;
                _code = code;
            }

            public static CacheKey ForDatabase(string databaseName, string name, string code)
            {
                return new(databaseName, name, code);
            }

            public static CacheKey ForServerWide(string name, string code)
            {
                return new(resourceName: null, name, code);
            }

            public static CacheKey ForCompilation(string name, string code)
            {
                return new(resourceName: null, name, code);
            }

            public bool Equals(CacheKey other)
            {
                if (ReferenceEquals(null, other))
                    return false;
                if (ReferenceEquals(this, other))
                    return true;

                var equals = string.Equals(ResourceName, other.ResourceName, StringComparison.OrdinalIgnoreCase)
                             && string.Equals(Name, other.Name, StringComparison.OrdinalIgnoreCase);

                if (equals == false)
                    return false;

                if (_code == null || other._code == null)
                    return true;

                return string.Equals(_code, other._code);
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj))
                    return false;
                if (ReferenceEquals(this, obj))
                    return true;
                if (obj.GetType() != GetType())
                    return false;
                return Equals((CacheKey)obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    var hashCode = (ResourceName != null ? StringComparer.OrdinalIgnoreCase.GetHashCode(ResourceName) : 0);
                    hashCode = (hashCode * 397) ^ (Name != null ? StringComparer.OrdinalIgnoreCase.GetHashCode(Name) : 0);
                    return hashCode;
                }
            }
        }
    }
}
