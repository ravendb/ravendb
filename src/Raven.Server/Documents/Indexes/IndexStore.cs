using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.Auto;

namespace Raven.Server.Documents.Indexes
{
    public class IndexStore : IDisposable
    {
        private readonly DocumentsStorage _documentsStorage;
        private readonly IndexingConfiguration _configuration;

        private readonly DatabaseNotifications _databaseNotifications;

        private readonly object _locker = new object();

        private bool _initialized;

        private string _path;

        public CollectionOfIndexes _indexes = new CollectionOfIndexes();

        public IndexStore(DocumentsStorage documentsStorage, IndexingConfiguration configuration, DatabaseNotifications databaseNotifications)
        {
            _documentsStorage = documentsStorage;
            _configuration = configuration;
            _databaseNotifications = databaseNotifications;
        }

        public void Initialize()
        {
            if (_initialized)
                throw new InvalidOperationException();

            lock (_locker)
            {
                if (_initialized)
                    throw new InvalidOperationException();

                if (_configuration.RunInMemory == false)
                {
                    _path = _configuration.IndexStoragePath;
                    if (System.IO.Directory.Exists(_path) == false && _configuration.RunInMemory == false)
                        System.IO.Directory.CreateDirectory(_path);
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

            _indexes.Add(AutoIndex.CreateNew(indexId, definition, _documentsStorage, _configuration, _databaseNotifications));

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
            if (_configuration.RunInMemory)
                return;

            foreach (var indexDirectory in new DirectoryInfo(_path).GetDirectories())
            {
                int indexId;
                if (int.TryParse(indexDirectory.Name, out indexId) == false)
                    continue;

                var index = Index.Open(indexId, indexDirectory.FullName, _documentsStorage, _configuration, _databaseNotifications);
                _indexes.Add(index);
            }
        }

        public List<AutoIndexDefinition> GetAutoIndexDefinitionsForCollection(string collection)
        {
            return _indexes.GetAutoIndexDefinitionsForCollection(collection); // TODO arek
        }
    }
}