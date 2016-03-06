using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

using Raven.Server.Documents.Indexes.Auto;

namespace Raven.Server.Documents.Indexes
{
    public class IndexStore : IDisposable
    {
        private readonly DocumentDatabase _documentDatabase;

        private readonly CollectionOfIndexes _indexes = new CollectionOfIndexes();

        private readonly object _locker = new object();

        private bool _initialized;

        private string _path;

        public IndexStore(DocumentDatabase documentDatabase)
        {
            _documentDatabase = documentDatabase;
        }

        public void Initialize()
        {
            if (_initialized)
                throw new InvalidOperationException();

            lock (_locker)
            {
                if (_initialized)
                    throw new InvalidOperationException();

                if (_documentDatabase.Configuration.Indexing.RunInMemory == false)
                {
                    _path = _documentDatabase.Configuration.Indexing.IndexStoragePath;
                    if (Directory.Exists(_path) == false && _documentDatabase.Configuration.Indexing.RunInMemory == false)
                        Directory.CreateDirectory(_path);
                }

                Task.Factory.StartNew(OpenIndexes, TaskCreationOptions.LongRunning);

                _initialized = true;
            }
        }

        public Index GetIndex(int id)
        {
            Index index;
            if (_indexes.TryGetById(id, out index) == false)
                return null;

            return index;
        }

        public Index GetIndex(string name)
        {
            Index index;
            if (_indexes.TryGetByName(name, out index) == false)
                return null;

            return index;
        }

        public int CreateIndex(AutoIndexDefinition definition)
        {
            var indexId = _indexes.GetNextIndexId();

            _indexes.Add(AutoMapIndex.CreateNew(indexId, definition, _documentDatabase));

            return indexId;
        }

        public void Dispose()
        {
            //FlushMapIndexes();
            //FlushReduceIndexes();

            foreach (var index in _indexes)
            {
                index.Dispose();
            }
        }

        private void OpenIndexes()
        {
            if (_documentDatabase.Configuration.Indexing.RunInMemory)
                return;

            foreach (var indexDirectory in new DirectoryInfo(_path).GetDirectories())
            {
                int indexId;
                if (int.TryParse(indexDirectory.Name, out indexId) == false)
                    continue;

                var index = Index.Open(indexId, _documentDatabase);
                _indexes.Add(index);
            }
        }

        public List<AutoIndexDefinition> GetAutoIndexDefinitionsForCollection(string collection)
        {
            return _indexes.GetDefinitionsOfTypeForCollection<AutoIndexDefinition>(collection);
        }

        public IEnumerable<Index> GetIndexesForCollection(string collection)
        {
            return _indexes.GetForCollection(collection);
        }
    }
}