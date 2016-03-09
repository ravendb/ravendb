using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Utils;

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

            var index = AutoMapIndex.CreateNew(indexId, definition, _documentDatabase);
            index.Execute();

            _indexes.Add(index);

            return indexId;
        }

        public int ResetIndex(int id)
        {
            var index = GetIndex(id);
            if (index == null)
                throw new InvalidOperationException("There is no index with id: " + id);

            RemoveIndex(id);

            switch (index.Type)
            {
                case IndexType.AutoMap:
                    var autoMapIndex = (AutoMapIndex)index;
                    var autoMapIndexDefinition = autoMapIndex.Definition;
                    return CreateIndex(autoMapIndexDefinition);
                default:
                    throw new NotSupportedException(index.Type.ToString());
            }
        }

        public void RemoveIndex(int id)
        {
            Index index;
            if (_indexes.TryRemoveById(id, out index) == false)
                throw new InvalidOperationException("There is no index with id: " + id);

            try
            {
                index.Dispose();
            }
            catch (Exception)
            {
                //TODO [ppekrol] log
            }

            var path = Path.Combine(_documentDatabase.Configuration.Indexing.IndexStoragePath, id.ToString());

            Task.Factory.StartNew(() => IOExtensions.DeleteDirectory(path));
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