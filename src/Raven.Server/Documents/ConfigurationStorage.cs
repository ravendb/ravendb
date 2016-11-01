using System;
using System.IO;
using Raven.Server.ServerWide.Context;
using Voron;

namespace Raven.Server.Documents
{
    public class ConfigurationStorage : IDisposable
    {
        private readonly DocumentDatabase _db;

        private TransactionContextPool _contextPool;

        public AlertsStorage AlertsStorage { get; private set; }

        public StorageEnvironment Environment { get; private set; }

        public ConfigurationStorage(DocumentDatabase db)
        {
            _db = db;

            var options = _db.Configuration.Core.RunInMemory
                ? StorageEnvironmentOptions.CreateMemoryOnly(Path.Combine(_db.Configuration.Core.DataDirectory, "Configuration"))
                : StorageEnvironmentOptions.ForPath(Path.Combine(_db.Configuration.Core.DataDirectory, "Configuration"));

            options.SchemaVersion = 1;
            options.TransactionsMode = TransactionsMode.Lazy;
            Environment = new StorageEnvironment(options);

            AlertsStorage = new AlertsStorage(_db.Name);
        }

        public void Initialize()
        {
            _contextPool = new TransactionContextPool(Environment);
            AlertsStorage.Initialize(Environment, _contextPool);
        }

        public void Dispose()
        {
            _contextPool?.Dispose();
            Environment.Dispose();
        }
    }
}