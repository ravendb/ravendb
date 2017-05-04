using System;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.PeriodicExport;
using Raven.Server.Documents.Transformers;
using Raven.Server.NotificationCenter;
using Raven.Server.ServerWide.Context;
using Voron;

namespace Raven.Server.Documents
{
    public class ConfigurationStorage : IDisposable
    {
        private TransactionContextPool _contextPool;

        public TransactionContextPool ContextPool => _contextPool;

        public NotificationsStorage NotificationsStorage { get; }

        public OperationsStorage OperationsStorage { get; }

        public StorageEnvironment Environment { get; }

        public EtlStorage EtlStorage { get; }

        public ConfigurationStorage(DocumentDatabase db)
        {
            var path = db.Configuration.Core.DataDirectory.Combine("Configuration");

            var options = db.Configuration.Core.RunInMemory
                ? StorageEnvironmentOptions.CreateMemoryOnly(path.FullPath, null, db.IoChanges, db.CatastrophicFailureNotification)
                : StorageEnvironmentOptions.ForPath(path.FullPath, null, null, db.IoChanges, db.CatastrophicFailureNotification);

            options.OnNonDurableFileSystemError += db.HandleNonDurableFileSystemError;
            options.OnRecoveryError += db.HandleOnRecoveryError;

            options.SchemaVersion = 1;
            options.ForceUsing32BitsPager = db.Configuration.Storage.ForceUsing32BitsPager;
            options.TimeToSyncAfterFlashInSeconds = db.Configuration.Storage.TimeToSyncAfterFlashInSeconds;
            options.NumOfCocurrentSyncsPerPhysDrive = db.Configuration.Storage.NumOfCocurrentSyncsPerPhysDrive;
            options.MasterKey = db.MasterKey;

            Environment = new StorageEnvironment(options);
            
            NotificationsStorage = new NotificationsStorage(db.Name);

            OperationsStorage = new OperationsStorage();

            EtlStorage = new EtlStorage(db.Name);

            PeriodicBackupStorage = new PeriodicBackupStore();

            _contextPool = new TransactionContextPool(Environment);
        }

        public PeriodicBackupStore PeriodicBackupStorage { get; set; }

        public void InitializeNotificationsStorage()
        {
            NotificationsStorage.Initialize(Environment, _contextPool);
        }

        public void Initialize(IndexStore indexStore, TransformerStore transformerStore)
        {
            OperationsStorage.Initialize(Environment, _contextPool);
            EtlStorage.Initialize(Environment, _contextPool);
        }

        public void Dispose()
        {
            _contextPool?.Dispose();
            Environment.Dispose();
        }
    }
}