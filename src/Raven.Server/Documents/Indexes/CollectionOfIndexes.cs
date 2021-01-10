using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Sparrow.Collections;

namespace Raven.Server.Documents.Indexes
{
    public class CollectionOfIndexes : IEnumerable<Index>
    {
        private readonly ConcurrentDictionary<string, Index> _indexesByName = new ConcurrentDictionary<string, Index>(IndexNameComparer.Instance);
        private readonly ConcurrentDictionary<string, ConcurrentSet<Index>> _indexesByCollection = 
            new ConcurrentDictionary<string, ConcurrentSet<Index>>(StringComparer.OrdinalIgnoreCase);

        public class IndexNameComparer : IEqualityComparer<string>
        {
            public static readonly IndexNameComparer Instance = new IndexNameComparer();

            public bool Equals(string x, string y)
            {
                if (x.StartsWith("Auto/"))
                {
                    return StringComparer.Ordinal.Equals(x, y);
                }
                return StringComparer.OrdinalIgnoreCase.Equals(x, y);
            }

            public int GetHashCode(string obj)
            {
                if (obj.StartsWith("Auto/"))
                {
                    return StringComparer.Ordinal.GetHashCode(obj);
                }
                return StringComparer.OrdinalIgnoreCase.GetHashCode(obj);
            }
        }

        public void Add(Index index)
        {
            _indexesByName[index.Name] = index;

            foreach (var collection in index.Definition.Collections)
            {
                var indexes = _indexesByCollection.GetOrAdd(collection, s => new ConcurrentSet<Index>());
                indexes.Add(index);
            }
        }

        public void ReplaceIndex(string name, Index oldIndex, Index newIndex)
        {
            Debug.Assert(oldIndex == null || IndexNameComparer.Instance.Equals(name, oldIndex.Name));

            _indexesByName.AddOrUpdate(name, newIndex, (key, oldValue) => newIndex);
            if (newIndex.Name != name)
                _indexesByName.TryRemove(newIndex.Name, out Index _);

            if (oldIndex == null)
                return;

            foreach (var collection in oldIndex.Definition.Collections)
            {
                if (_indexesByCollection.TryGetValue(collection, out ConcurrentSet<Index> indexes) == false)
                    continue;

                indexes.TryRemove(oldIndex);
            }
        }

        public bool TryRemoveByName(string name, Index existingInstance)
        {
            var result = _indexesByName.TryGetValue(name, out var index);
            if (result == false)
                return false;

            if (index != existingInstance)
                return false;

            if (_indexesByName.TryRemove(name, out index) == false)
                return false; // already removed?

            if (index != existingInstance)
            {
                // here for correctness only, don't expect this to ever happen
                _indexesByName.TryAdd(name, index);// re-add the removed one
                return false;
            }

            foreach (var collection in index.Definition.Collections)
            {
                if (_indexesByCollection.TryGetValue(collection, out ConcurrentSet<Index> indexes) == false)
                    continue;

                indexes.TryRemove(index);
            }

            return true;
        }


        public bool TryGetByName(string name, out Index index)
        {
            return _indexesByName.TryGetValue(name, out index);
        }

        public IEnumerable<Index> GetForCollection(string collection)
        {

            if (_indexesByCollection.TryGetValue(collection, out ConcurrentSet<Index> indexes) == false)
                return Enumerable.Empty<Index>();

            return indexes;
        }

        public IEnumerator<Index> GetEnumerator()
        {
            // This doesn't happen often enough for this lock to hurt
            return _indexesByName.Values.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>
        /// This value should be used sparingly: fetching it locks the cache.
        /// </summary>
        public int Count => _indexesByName.Count;
    }
}
