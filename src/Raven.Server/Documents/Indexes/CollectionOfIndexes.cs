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
        private readonly ConcurrentDictionary<long, Index> _indexesByEtag = new ConcurrentDictionary<long, Index>();
        private readonly ConcurrentDictionary<string, Index> _indexesByName = new ConcurrentDictionary<string, Index>(StringComparer.OrdinalIgnoreCase);
        private readonly ConcurrentDictionary<string, ConcurrentSet<Index>> _indexesByCollection = new ConcurrentDictionary<string, ConcurrentSet<Index>>(StringComparer.OrdinalIgnoreCase);

        public void Add(Index index)
        {
            _indexesByEtag[index.Etag] = index;
            _indexesByName[index.Name] = index;

            foreach (var collection in index.Definition.Collections)
            {
                var indexes = _indexesByCollection.GetOrAdd(collection, s => new ConcurrentSet<Index>());
                indexes.Add(index);
            }
        }

        public void ReplaceIndex(string name, Index oldIndex, Index newIndex)
        {
            Debug.Assert(oldIndex == null || string.Equals(name, oldIndex.Name, StringComparison.OrdinalIgnoreCase));

            _indexesByName.AddOrUpdate(name, oldIndex, (key, oldValue) => newIndex);

            _indexesByName.TryRemove(newIndex.Name, out Index _);

            if (oldIndex == null)
                return;

            foreach (var collection in oldIndex.Definition.Collections)
            {
                if (_indexesByCollection.TryGetValue(collection, out ConcurrentSet<Index> indexes) == false)
                    continue;

                indexes.TryRemove(oldIndex);
            }
            _indexesByEtag.TryRemove(oldIndex.Etag, out oldIndex);
            _indexesByEtag.TryAdd(newIndex.Etag, newIndex);
        }

        public void UpdateIndexEtag(long oldEtag, long newEtag)
        {
            _indexesByEtag.TryRemove(oldEtag, out Index value);
            _indexesByEtag.TryAdd(newEtag, value);
            value.Etag = newEtag;
        }

        public bool TryGetByEtag(long etag, out Index index)
        {
            return _indexesByEtag.TryGetValue(etag, out index);
        }

        public bool TryGetByName(string name, out Index index)
        {
            return _indexesByName.TryGetValue(name, out index);
        }

        public bool TryRemoveByEtag(long etag, out Index index)
        {
            var result = _indexesByEtag.TryRemove(etag, out index);
            if (result == false)
                return false;

            _indexesByName.TryRemove(index.Name, out index);

            foreach (var collection in index.Definition.Collections)
            {
                if (_indexesByCollection.TryGetValue(collection, out ConcurrentSet<Index> indexes) == false)
                    continue;

                indexes.TryRemove(index);
            }

            return true;
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
        public int Count => _indexesByEtag.Count;
    }
}
