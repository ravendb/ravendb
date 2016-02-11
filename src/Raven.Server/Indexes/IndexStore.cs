using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

using Raven.Abstractions.Extensions;
using Raven.Server.Documents;
using Raven.Server.Indexes.Auto;

namespace Raven.Server.Indexes
{
    public class IndexStore : IDisposable
    {
        private readonly DocumentsStorage _documentsStorage;

        private readonly object _locker = new object();

        private readonly Dictionary<int, Index> _indexes = new Dictionary<int, Index>();

        private bool _initialized;

        private string _path;

        public IndexStore(DocumentsStorage documentsStorage)
        {
            _documentsStorage = documentsStorage;
        }

        public void Initialize()
        {
            if (_initialized)
                throw new InvalidOperationException();

            lock (_locker)
            {
                if (_initialized)
                    throw new InvalidOperationException();

                _path = _documentsStorage.Configuration.Core.IndexStoragePath;

                if (System.IO.Directory.Exists(_path) == false && _documentsStorage.Configuration.Core.RunInMemory == false)
                    System.IO.Directory.CreateDirectory(_path);

                Task.Factory.StartNew(OpenIndexes, TaskCreationOptions.LongRunning);

                _initialized = true;
            }
        }

        private void OpenIndexes()
        {
            if (_documentsStorage.Configuration.Core.RunInMemory)
                return;

            foreach (var indexDirectory in new DirectoryInfo(_path).GetDirectories())
            {
                int indexId;
                if (int.TryParse(indexDirectory.Name, out indexId) == false)
                    continue;

                var index = Index.Create(indexId, indexDirectory.FullName, _documentsStorage);
                AddIndex(indexId, index);
            }
        }

        private void AddIndex(int indexId, Index index)
        {
            _indexes[indexId] = index;
        }

        public Index GetIndex(string indexName)
        {
            throw new NotImplementedException();
        }

        public AutoIndex CreateIndex(AutoIndexDefinition definition)
        {
            throw new NotImplementedException();
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
    }
}