using System;
using System.IO;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Transformers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Voron;

namespace Raven.Server.Documents
{
    public class ConfigurationStorage : IDisposable
    {
        private TransactionContextPool _contextPool;
        public IndexesEtagsStorage IndexesEtagsStorage { get; }

        public AlertsStorage AlertsStorage { get; }

        public StorageEnvironment Environment { get; }

        public ConfigurationStorage(DocumentDatabase db, ServerStore serverStore)
        {
            var options = db.Configuration.Core.RunInMemory
                ? StorageEnvironmentOptions.CreateMemoryOnly(Path.Combine(db.Configuration.Core.DataDirectory, "Configuration"))
                : StorageEnvironmentOptions.ForPath(Path.Combine(db.Configuration.Core.DataDirectory, "Configuration"));

            options.SchemaVersion = 1;
            Environment = new StorageEnvironment(options);

            AlertsStorage = new AlertsStorage(db.Name, serverStore);

            IndexesEtagsStorage = new IndexesEtagsStorage(db.Name);
        }

        public void Initialize(IndexStore indexStore, 
                               TransformerStore transformerStore,
                               StorageEnvironment documentStorageEnvironment,
                               DocumentsContextPool documentsContextPool)
        {
            _contextPool = new TransactionContextPool(Environment);
            AlertsStorage.Initialize(Environment, _contextPool);
            IndexesEtagsStorage.Initialize(documentStorageEnvironment, documentsContextPool, indexStore, transformerStore);
        }

        public void Dispose()
        {
            _contextPool?.Dispose();
            Environment.Dispose();
        }
    }
}