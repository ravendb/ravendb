using System;
using System.Collections.Concurrent;
using Raven.Client.Documents.Indexes.Analysis;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Analyzers;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Indexes.Analysis
{
    public static class AnalyzerCompilationCache
    {
        private static readonly ConcurrentDictionary<CacheKey, Lazy<Type>> AnalyzersCompilationCache = new ConcurrentDictionary<CacheKey, Lazy<Type>>();

        internal static readonly ConcurrentDictionary<CacheKey, Lazy<Type>> AnalyzersServerWideCache = new ConcurrentDictionary<CacheKey, Lazy<Type>>();

        internal static readonly ConcurrentDictionary<CacheKey, Lazy<Type>> AnalyzersPerDatabaseCache = new ConcurrentDictionary<CacheKey, Lazy<Type>>();

        public static Type GetAnalyzerType(string name, string databaseName)
        {
            var key = CacheKey.ForDatabase(databaseName, name);

            if (TryGetAnalyzerType(AnalyzersPerDatabaseCache, key, out var type))
                return type;

            key = CacheKey.ForServerWide(name);

            if (TryGetAnalyzerType(AnalyzersServerWideCache, key, out type))
                return type;

            return null;
        }

        private static bool TryGetAnalyzerType(ConcurrentDictionary<CacheKey, Lazy<Type>> cache, CacheKey key, out Type type)
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

        public static void AddServerWideAnalyzers(ServerStore serverStore)
        {
            using (serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                foreach (var kvp in serverStore.Cluster.ItemsStartingWith(context, PutServerWideAnalyzerCommand.Prefix, 0, long.MaxValue))
                {
                    var analyzerDefinition = JsonDeserializationServer.AnalyzerDefinition(kvp.Value);
                    analyzerDefinition.Validate();

                    var key = CacheKey.ForServerWide(analyzerDefinition.Name);

                    AddAnalyzerInternal(AnalyzersServerWideCache, key, analyzerDefinition.Name, analyzerDefinition.Code);
                }
            }
        }

        public static void AddServerWideAnalyzer(AnalyzerDefinition definition)
        {
            var key = CacheKey.ForServerWide(definition.Name);
            AddAnalyzerInternal(AnalyzersServerWideCache, key, definition.Name, definition.Code);
        }

        public static void RemoveServerWideAnalyzer(string name)
        {
            var key = CacheKey.ForServerWide(name);
            AnalyzersServerWideCache.TryRemove(key, out _);
        }

        public static void AddAnalyzers(DatabaseRecord databaseRecord)
        {
            foreach (var kvp in AnalyzersPerDatabaseCache.ForceEnumerateInThreadSafeManner())
            {
                var key = kvp.Key;
                if (string.Equals(key.ResourceName, databaseRecord.DatabaseName, StringComparison.OrdinalIgnoreCase) == false)
                    continue;

                if (databaseRecord.Analyzers != null && databaseRecord.Analyzers.ContainsKey(key.AnalyzerName))
                    continue;

                AnalyzersPerDatabaseCache.TryRemove(key, out _);
            }

            if (databaseRecord.Analyzers == null || databaseRecord.Analyzers.Count == 0)
                return;

            var aggregator = new ExceptionAggregator("Could not update analyzers cache");

            foreach (var kvp in databaseRecord.Analyzers)
            {
                aggregator.Execute(() =>
                {
                    var analyzerName = kvp.Value.Name;
                    var analyzerCode = kvp.Value.Code;
                    var key = CacheKey.ForDatabase(databaseRecord.DatabaseName, analyzerName);

                    AddAnalyzerInternal(AnalyzersPerDatabaseCache, key, analyzerName, analyzerCode);
                });
            }

            aggregator.ThrowIfNeeded();
        }

        public static void Clear(string databaseName)
        {
            foreach (var kvp in AnalyzersPerDatabaseCache.ForceEnumerateInThreadSafeManner())
            {
                var key = kvp.Key;
                if (string.Equals(key.ResourceName, databaseName, StringComparison.OrdinalIgnoreCase) == false)
                    continue;

                AnalyzersPerDatabaseCache.TryRemove(key, out _);
            }
        }

        private static void AddAnalyzerInternal(ConcurrentDictionary<CacheKey, Lazy<Type>> cache, CacheKey key, string name, string analyzerCode)
        {
            var result = cache.GetOrAdd(key, _ => new Lazy<Type>(() => CompileAnalyzer(name, analyzerCode)));
            if (result.IsValueCreated)
                return;

            try
            {
                // compile analyzer
                var value = result.Value;
            }
            catch (Exception)
            {
                cache.TryRemove(key, out _);
                throw;
            }
        }

        private static Type CompileAnalyzer(string name, string analyzerCode)
        {
            var key = CacheKey.ForCompilation(name, analyzerCode);
            var result = AnalyzersCompilationCache.GetOrAdd(key, _ => new Lazy<Type>(() => AnalyzerCompiler.Compile(name, analyzerCode)));

            try
            {
                return result.Value;
            }
            catch (Exception)
            {
                AnalyzersCompilationCache.TryRemove(key, out _);
                throw;
            }
        }

        internal class CacheKey : IEquatable<CacheKey>
        {
            public readonly string ResourceName;
            public readonly string AnalyzerName;
            private readonly string _analyzerCode;

            private CacheKey(string resourceName, string analyzerName, string analyzerCode)
            {
                ResourceName = resourceName;
                AnalyzerName = analyzerName;
                _analyzerCode = analyzerCode;
            }

            public static CacheKey ForDatabase(string databaseName, string analyzerName)
            {
                return new(databaseName, analyzerName, analyzerCode: null);
            }

            public static CacheKey ForServerWide(string analyzerName)
            {
                return new(resourceName: null, analyzerName, analyzerCode: null);
            }

            public static CacheKey ForCompilation(string analyzerName, string analyzerCode)
            {
                return new(resourceName: null, analyzerName, analyzerCode);
            }

            public bool Equals(CacheKey other)
            {
                if (ReferenceEquals(null, other))
                    return false;
                if (ReferenceEquals(this, other))
                    return true;

                var equals = string.Equals(ResourceName, other.ResourceName, StringComparison.OrdinalIgnoreCase)
                             && string.Equals(AnalyzerName, other.AnalyzerName, StringComparison.OrdinalIgnoreCase);

                if (equals == false)
                    return false;

                if (_analyzerCode == null || other._analyzerCode == null)
                    return true;

                return string.Equals(_analyzerCode, other._analyzerCode);
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
                    hashCode = (hashCode * 397) ^ (AnalyzerName != null ? StringComparer.OrdinalIgnoreCase.GetHashCode(AnalyzerName) : 0);
                    return hashCode;
                }
            }
        }
    }
}
