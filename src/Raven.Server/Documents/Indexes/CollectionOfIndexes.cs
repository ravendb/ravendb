using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Raven.Server.Documents.Indexes.Auto;

namespace Raven.Server.Documents.Indexes
{
    public class CollectionOfIndexes : IEnumerable<Index>
    {
        private static readonly List<AutoIndexDefinition> EmptyAutoIndexDefinitions = new List<AutoIndexDefinition>();

        private readonly Dictionary<int, Index> _indexesById = new Dictionary<int, Index>();
        private readonly Dictionary<string, Index> _indexesByName = new Dictionary<string, Index>();
        private readonly Dictionary<string, List<Index>> _indexesByCollection = new Dictionary<string, List<Index>>();

        public void Add(Index index)
        {
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

        public List<AutoIndexDefinition> GetAutoIndexDefinitionsForCollection(string collection)
        {
            List<Index> indexes;

            if (_indexesByCollection.TryGetValue(collection, out indexes) == false)
                return EmptyAutoIndexDefinitions;

            return indexes.OfType<AutoIndex>().Select(x => x.Definition).ToList();
        }

        public int GetNextIndexId()
        {
            return 1; //TODO arek
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