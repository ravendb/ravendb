using System;
using System.Collections.Concurrent;
using Raven.Client.Documents.Indexes.Analysis;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Indexes.Analysis
{
    public static class AnalyzerCompilationCache
    {
        private static readonly ConcurrentDictionary<CacheKey, Lazy<Type>> AnalyzersCache = new ConcurrentDictionary<CacheKey, Lazy<Type>>();

        internal static readonly ConcurrentDictionary<CacheKey, Lazy<Type>> AnalyzersPerDatabaseCache = new ConcurrentDictionary<CacheKey, Lazy<Type>>();

        public static Type GetAnalyzerType(string name, string databaseName)
        {
            var key = new CacheKey(databaseName, name, null);

            if (AnalyzersPerDatabaseCache.TryGetValue(key, out var result) == false)
                return null;

            try
            {
                return result.Value;
            }
            catch (Exception)
            {
                AnalyzersPerDatabaseCache.TryRemove(key, out _);
                throw;
            }
        }

        public static void AddAnalyzer(AnalyzerDefinition definition, string databaseName)
        {
            AddAnalyzerInternal(definition.Name, definition.Code, databaseName);
        }

        public static void AddAnalyzers(DatabaseRecord databaseRecord)
        {
            foreach (var kvp in AnalyzersPerDatabaseCache.ForceEnumerateInThreadSafeManner())
            {
                var key = kvp.Key;
                if (string.Equals(key.DatabaseName, databaseRecord.DatabaseName, StringComparison.OrdinalIgnoreCase) == false)
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
                    AddAnalyzerInternal(kvp.Value.Name, kvp.Value.Code, databaseRecord.DatabaseName);
                });
            }

            aggregator.ThrowIfNeeded();
        }

        public static void Clear(string databaseName)
        {
            foreach (var kvp in AnalyzersPerDatabaseCache.ForceEnumerateInThreadSafeManner())
            {
                var key = kvp.Key;
                if (string.Equals(key.DatabaseName, databaseName, StringComparison.OrdinalIgnoreCase) == false)
                    continue;

                AnalyzersPerDatabaseCache.TryRemove(key, out _);
            }
        }

        private static void AddAnalyzerInternal(string name, string analyzerCode, string databaseName)
        {
            var key = new CacheKey(databaseName, name, analyzerCode);

            var result = AnalyzersPerDatabaseCache.GetOrAdd(key, _ => new Lazy<Type>(() => CompileAnalyzer(name, analyzerCode)));
            if (result.IsValueCreated)
                return;

            try
            {
                // compile analyzer
                var value = result.Value;
            }
            catch (Exception)
            {
                AnalyzersPerDatabaseCache.TryRemove(key, out _);
                throw;
            }
        }

        private static Type CompileAnalyzer(string name, string analyzerCode)
        {
            var key = new CacheKey(null, name, analyzerCode);
            var result = AnalyzersCache.GetOrAdd(key, _ => new Lazy<Type>(() => AnalyzerCompiler.Compile(name, analyzerCode)));

            try
            {
                return result.Value;
            }
            catch (Exception)
            {
                AnalyzersCache.TryRemove(key, out _);
                throw;
            }
        }

        internal class CacheKey : IEquatable<CacheKey>
        {
            public readonly string DatabaseName;
            public readonly string AnalyzerName;
            private readonly string _analyzerCode;

            public CacheKey(string databaseName, string analyzerName, string analyzerCode)
            {
                DatabaseName = databaseName;
                AnalyzerName = analyzerName;
                _analyzerCode = analyzerCode;
            }

            public bool Equals(CacheKey other)
            {
                if (ReferenceEquals(null, other))
                    return false;
                if (ReferenceEquals(this, other))
                    return true;

                var equals = string.Equals(DatabaseName, other.DatabaseName, StringComparison.OrdinalIgnoreCase)
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
                    var hashCode = (DatabaseName != null ? StringComparer.OrdinalIgnoreCase.GetHashCode(DatabaseName) : 0);
                    hashCode = (hashCode * 397) ^ (AnalyzerName != null ? StringComparer.OrdinalIgnoreCase.GetHashCode(AnalyzerName) : 0);
                    return hashCode;
                }
            }
        }
    }
}
