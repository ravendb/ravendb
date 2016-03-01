using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace Raven.Server.Documents.Indexes
{
    public class CollectionOfIndexes : IEnumerable<Index>
    {
        private readonly ConcurrentDictionary<int, Index> _indexesById = new ConcurrentDictionary<int, Index>();
        private readonly ConcurrentDictionary<string, Index> _indexesByName = new ConcurrentDictionary<string, Index>();
        private readonly ConcurrentDictionary<string, List<Index>> _indexesByCollection = new ConcurrentDictionary<string, List<Index>>();
        private int _nextIndexId = 1;

        public void Add(Index index)
        {
            _nextIndexId = Math.Max(index.IndexId, _nextIndexId) + 1;
            _indexesById[index.IndexId] = index;
            _indexesByName[index.Name] = index;

            foreach (var collection in index.Definition.Collections)
            {
                List<Index> indexes;
                if (_indexesByCollection.TryGetValue(collection, out indexes) == false)
                {
                    indexes = new List<Index>();
                    _indexesByCollection[collection] = indexes;
                }

                indexes.Add(index);
            }
        }

        public bool TryGetById(int id, out Index index)
        {
            return _indexesById.TryGetValue(id, out index);
        }

        public bool TryGetByName(string name, out Index index)
        {
            return _indexesByName.TryGetValue(name, out index);
        }

        public List<Index> GetForCollection(string collection)
        {
            List<Index> indexes;

            if (_indexesByCollection.TryGetValue(collection, out indexes) == false)
                return Enumerable.Empty<Index>().ToList();

            return indexes;
        }

        public List<T> GetDefinitionsOfTypeForCollection<T>(string collection) where T : IndexDefinitionBase
        {
            return GetForCollection(collection).Where(x => x.Definition is T).Select(x => (T)x.Definition).ToList();
        }

        public int GetNextIndexId()
        {
            return _nextIndexId;
        }

        public IEnumerator<Index> GetEnumerator()
        {
            return _indexesById.Values.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}