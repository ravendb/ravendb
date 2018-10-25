using System;
using System.Collections.Concurrent;
using Lucene.Net.Search;
using Raven.Client.Documents.Queries.Sorting;
using Raven.Client.Exceptions.Documents.Sorters;
using Raven.Client.ServerWide;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Indexes.Sorting
{
    public class SorterCompilationCache
    {
        private static readonly ConcurrentDictionary<CacheKey, Lazy<Func<string, int, int, bool, FieldComparator>>> SortersCache = new ConcurrentDictionary<CacheKey, Lazy<Func<string, int, int, bool, FieldComparator>>>();

        private static readonly ConcurrentDictionary<CacheKey, Lazy<Func<string, int, int, bool, FieldComparator>>> SortersPerDatabaseCache = new ConcurrentDictionary<CacheKey, Lazy<Func<string, int, int, bool, FieldComparator>>>();

        public static Func<string, int, int, bool, FieldComparator> GetSorter(string name, string databaseName)
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

        public static void RemoveSorter(string name, string databaseName)
        {
            var key = new CacheKey(databaseName, name, null);
            SortersPerDatabaseCache.TryRemove(key, out _);
        }

        public static void AddSorter(SorterDefinition definition, string databaseName)
        {
            AddSorterInternal(definition.Name, definition.Code, databaseName);
        }

        public static void AddSorters(DatabaseRecord databaseRecord)
        {
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

        private static void AddSorterInternal(string name, string sorterCode, string databaseName)
        {
            var key = new CacheKey(databaseName, name, sorterCode);

            var result = SortersPerDatabaseCache.GetOrAdd(key, _ => new Lazy<Func<string, int, int, bool, FieldComparator>>(() => CompileSorter(name, sorterCode)));
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

        private static Func<string, int, int, bool, FieldComparator> CompileSorter(string name, string sorterCode)
        {
            var result = SortersCache.GetOrAdd(new CacheKey(null, null, sorterCode), _ => new Lazy<Func<string, int, int, bool, FieldComparator>>(() => SorterCompiler.Compile(name, sorterCode)));
            return result.Value;
        }

        private class CacheKey : IEquatable<CacheKey>
        {
            private readonly string _databaseName;
            private readonly string _sorterName;
            private readonly string _sorterCode;

            public CacheKey(string databaseName, string sorterName, string sorterCode)
            {
                _databaseName = databaseName;
                _sorterName = sorterName;
                _sorterCode = sorterCode;
            }

            public bool Equals(CacheKey other)
            {
                if (ReferenceEquals(null, other))
                    return false;
                if (ReferenceEquals(this, other))
                    return true;

                var equals = string.Equals(_databaseName, other._databaseName, StringComparison.OrdinalIgnoreCase)
                             && string.Equals(_sorterName, other._sorterName, StringComparison.OrdinalIgnoreCase);

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
                    var hashCode = (_databaseName != null ? StringComparer.OrdinalIgnoreCase.GetHashCode(_databaseName) : 0);
                    hashCode = (hashCode * 397) ^ (_sorterName != null ? StringComparer.OrdinalIgnoreCase.GetHashCode(_sorterName) : 0);
                    return hashCode;
                }
            }
        }
    }
}
