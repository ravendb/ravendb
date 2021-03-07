using System;
using System.Collections.Concurrent;
using Raven.Client.Documents.Queries.Sorting;
using Raven.Client.Exceptions.Documents.Sorters;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Indexes.Sorting
{
    public static class SorterCompilationCache
    {
        private static readonly ConcurrentDictionary<CacheKey, Lazy<CreateSorter>> SortersCache = new ConcurrentDictionary<CacheKey, Lazy<CreateSorter>>();

        internal static readonly ConcurrentDictionary<CacheKey, Lazy<CreateSorter>> SortersPerDatabaseCache = new ConcurrentDictionary<CacheKey, Lazy<CreateSorter>>();

        public static CreateSorter GetSorter(string name, string databaseName)
        {
            var key = new CacheKey(databaseName, name, null);

            if (SortersPerDatabaseCache.TryGetValue(key, out var result) == false)
                SorterDoesNotExistException.ThrowFor(name);

            try
            {
                return result.Value;
            }
            catch (Exception)
            {
                SortersPerDatabaseCache.TryRemove(key, out _);
                throw;
            }
        }

        public static void AddSorter(SorterDefinition definition, string databaseName)
        {
            AddSorterInternal(definition.Name, definition.Code, databaseName);
        }

        public static void AddSorters(DatabaseRecord databaseRecord)
        {
            foreach (var kvp in SortersPerDatabaseCache.ForceEnumerateInThreadSafeManner())
            {
                var key = kvp.Key;
                if (string.Equals(key.DatabaseName, databaseRecord.DatabaseName, StringComparison.OrdinalIgnoreCase) == false)
                    continue;

                if (databaseRecord.Sorters != null && databaseRecord.Sorters.ContainsKey(key.SorterName))
                    continue;

                SortersPerDatabaseCache.TryRemove(key, out _);
            }

            if (databaseRecord.Sorters == null || databaseRecord.Sorters.Count == 0)
                return;

            var aggregator = new ExceptionAggregator("Could not update sorters cache");

            foreach (var kvp in databaseRecord.Sorters)
            {
                aggregator.Execute(() =>
                {
                    AddSorterInternal(kvp.Value.Name, kvp.Value.Code, databaseRecord.DatabaseName);
                });
            }

            aggregator.ThrowIfNeeded();
        }

        public static void Clear(string databaseName)
        {
            foreach (var kvp in SortersPerDatabaseCache.ForceEnumerateInThreadSafeManner())
            {
                var key = kvp.Key;
                if (string.Equals(key.DatabaseName, databaseName, StringComparison.OrdinalIgnoreCase) == false)
                    continue;

                SortersPerDatabaseCache.TryRemove(key, out _);
            }
        }

        private static void AddSorterInternal(string name, string sorterCode, string databaseName)
        {
            var key = new CacheKey(databaseName, name, sorterCode);

            var result = SortersPerDatabaseCache.GetOrAdd(key, _ => new Lazy<CreateSorter>(() => CompileSorter(name, sorterCode)));
            if (result.IsValueCreated)
                return;

            try
            {
                // compile sorter
                var value = result.Value;
            }
            catch (Exception)
            {
                SortersPerDatabaseCache.TryRemove(key, out _);
                throw;
            }
        }

        private static CreateSorter CompileSorter(string name, string sorterCode)
        {
            var key = new CacheKey(null, name, sorterCode);
            var result = SortersCache.GetOrAdd(key, _ => new Lazy<CreateSorter>(() => SorterCompiler.Compile(name, sorterCode)));

            try
            {
                return result.Value;
            }
            catch (Exception)
            {
                SortersCache.TryRemove(key, out _);
                throw;
            }
        }

        internal class CacheKey : IEquatable<CacheKey>
        {
            public readonly string DatabaseName;
            public readonly string SorterName;
            private readonly string _sorterCode;

            public CacheKey(string databaseName, string sorterName, string sorterCode)
            {
                DatabaseName = databaseName;
                SorterName = sorterName;
                _sorterCode = sorterCode;
            }

            public bool Equals(CacheKey other)
            {
                if (ReferenceEquals(null, other))
                    return false;
                if (ReferenceEquals(this, other))
                    return true;

                var equals = string.Equals(DatabaseName, other.DatabaseName, StringComparison.OrdinalIgnoreCase)
                             && string.Equals(SorterName, other.SorterName, StringComparison.OrdinalIgnoreCase);

                if (equals == false)
                    return false;

                if (_sorterCode == null || other._sorterCode == null)
                    return true;

                return string.Equals(_sorterCode, other._sorterCode);
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
                    hashCode = (hashCode * 397) ^ (SorterName != null ? StringComparer.OrdinalIgnoreCase.GetHashCode(SorterName) : 0);
                    return hashCode;
                }
            }
        }
    }
}
