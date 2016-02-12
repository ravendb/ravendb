using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

using Raven.Abstractions.Extensions;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.Auto;

namespace Raven.Server.Documents.Indexes
{
    public class IndexStore : IDisposable
    {
        private readonly DocumentsStorage _documentsStorage;
        private readonly IndexingConfiguration _configuration;

        private readonly object _locker = new object();

        private readonly Dictionary<int, Index> _indexes = new Dictionary<int, Index>();

        private bool _initialized;

        private string _path;

        public IndexStore(DocumentsStorage documentsStorage, IndexingConfiguration configuration)
        {
            _documentsStorage = documentsStorage;
            _configuration = configuration;
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

        public Index GetIndex(int indexId)
        {
            Index index;
            if (_indexes.TryGetValue(indexId, out index) == false)
                return null;

            return index;
        }

        public Index GetIndex(string indexName)
        {
            throw new NotImplementedException();
        }

        public int CreateIndex(AutoIndexDefinition definition)
        {
            var indexId = 1; // TODO
            AddIndex(indexId, AutoIndex.CreateNew(indexId, definition, _documentsStorage, _configuration));
            return indexId;
        }

        public List<AutoIndexDefinition> GetAutoIndexDefinitionsForCollection(string collection)
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            //FlushMapIndexes();
            //FlushReduceIndexes();

            _indexes.ForEach(x => x.Value.Dispose());
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

                var index = Index.Open(indexId, indexDirectory.FullName, _documentsStorage);
                AddIndex(indexId, index);
            }
        }

        private void AddIndex(int indexId, Index index)
        {
            _indexes[indexId] = index;
        }
    }
}