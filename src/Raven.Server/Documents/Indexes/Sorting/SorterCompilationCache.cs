using System;
using System.Collections.Concurrent;
using Lucene.Net.Search;
using Raven.Client.Exceptions.Documents.Sorters;
using Raven.Client.ServerWide;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Indexes.Sorting
{
    public class SorterCompilationCache
    {
        private static readonly ConcurrentDictionary<CacheKey, Lazy<Func<string, int, int, bool, FieldComparator>>> SorterCache = new ConcurrentDictionary<CacheKey, Lazy<Func<string, int, int, bool, FieldComparator>>>();

        public static Func<string, int, int, bool, FieldComparator> GetSorter(string name, string databaseName)
        {
            var key = new CacheKey(databaseName, name, null);

            if (SorterCache.TryGetValue(key, out var result) == false)
                SorterDoesNotExistException.ThrowFor(name);

            try
            {
                return result.Value;
            }
            catch (Exception)
            {
                SorterCache.TryRemove(key, out _);
                throw;
            }
        }

        public static void UpdateCache(DatabaseRecord databaseRecord)
        {
            if (databaseRecord.Sorters == null || databaseRecord.Sorters.Count == 0)
                return;

            ExceptionAggregator aggregator = null;

            foreach (var kvp in databaseRecord.Sorters)
            {
                var key = new CacheKey(databaseRecord.DatabaseName, kvp.Value.Name, kvp.Value.Code);

                var result = SorterCache.GetOrAdd(key, _ => new Lazy<Func<string, int, int, bool, FieldComparator>>(() => GenerateSorter(kvp.Value.Name, kvp.Value.Code)));

                if (result.IsValueCreated)
                    continue;

                if (aggregator == null)
                    aggregator = new ExceptionAggregator("Could not update sorters cache");

                aggregator.Execute(() =>
                {
                    try
                    {
                        // compile sorter
                        var value = result.Value;
                    }
                    catch (Exception)
                    {
                        SorterCache.TryRemove(key, out _);
                        throw;
                    }
                });
            }

            aggregator?.ThrowIfNeeded();
        }

        private static Func<string, int, int, bool, FieldComparator> GenerateSorter(string name, string sorterCode)
        {
            return SorterCompiler.Compile(name, sorterCode);
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
                    return (_databaseName.GetHashCode() * 397) ^ _sorterName.GetHashCode();
                }
            }
        }
    }
}
