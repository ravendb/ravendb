using System;
using System.Linq;
using Raven.Server.Documents.Operations;
using Raven.Server.NotificationCenter;
using Raven.Server.ServerWide.Context;
using Raven.Server.Storage.Layout;
using Raven.Server.Storage.Schema;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Logging;
using Voron;

namespace Raven.Server.Documents
{
    public class ConfigurationStorage : IDisposable
    {
        private const string ResourceName = nameof(ConfigurationStorage);

        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<ConfigurationStorage>(ResourceName);

        public TransactionContextPool ContextPool { get; }

        public NotificationsStorage NotificationsStorage { get; }

        public OperationsStorage OperationsStorage { get; }

        public StorageEnvironment Environment { get; }

        public ConfigurationStorage(DocumentDatabase db)
        {
            var path = db.Configuration.Core.DataDirectory.Combine("Configuration");
            string tempPath = null;
            if (db.Configuration.Storage.TempPath != null)
            {
                tempPath = db.Configuration.Storage.TempPath.Combine("Configuration").ToFullPath();
            }

            var options = db.Configuration.Core.RunInMemory
                ? StorageEnvironmentOptions.CreateMemoryOnly(path.FullPath, tempPath, db.IoChanges, db.CatastrophicFailureNotification)
                : StorageEnvironmentOptions.ForPath(path.FullPath, tempPath, null, db.IoChanges, db.CatastrophicFailureNotification);

            options.OnNonDurableFileSystemError += db.HandleNonDurableFileSystemError;
            options.OnRecoveryError += db.HandleOnConfigurationRecoveryError;
            options.OnIntegrityErrorOfAlreadySyncedData += db.HandleOnConfigurationIntegrityErrorOfAlreadySyncedData;
            options.CompressTxAboveSizeInBytes = db.Configuration.Storage.CompressTxAboveSize.GetValue(SizeUnit.Bytes);
            options.SchemaVersion = SchemaUpgrader.CurrentVersion.ConfigurationVersion;
            options.SchemaUpgrader = SchemaUpgrader.Upgrader(SchemaUpgrader.StorageType.Configuration, this, null, null);
            options.ForceUsing32BitsPager = db.Configuration.Storage.ForceUsing32BitsPager;
            options.EnablePrefetching = db.Configuration.Storage.EnablePrefetching;
            options.TimeToSyncAfterFlushInSec = (int)db.Configuration.Storage.TimeToSyncAfterFlush.AsTimeSpan.TotalSeconds;
            options.NumOfConcurrentSyncsPerPhysDrive = db.Configuration.Storage.NumberOfConcurrentSyncsPerPhysicalDrive;
            options.Encryption.MasterKey = db.MasterKey?.ToArray();

            options.DoNotConsiderMemoryLockFailureAsCatastrophicError = db.Configuration.Security.DoNotConsiderMemoryLockFailureAsCatastrophicError;
            if (db.Configuration.Storage.MaxScratchBufferSize.HasValue)
                options.MaxScratchBufferSize = db.Configuration.Storage.MaxScratchBufferSize.Value.GetValue(SizeUnit.Bytes);
            options.PrefetchSegmentSize = db.Configuration.Storage.PrefetchBatchSize.GetValue(SizeUnit.Bytes);
            options.PrefetchResetThreshold = db.Configuration.Storage.PrefetchResetThreshold.GetValue(SizeUnit.Bytes);
            options.SyncJournalsCountThreshold = db.Configuration.Storage.SyncJournalsCountThreshold;
            options.SyncJournalsMaxSizeInMegabytesThreshold = db.Configuration.Storage.SyncJournalsMaxSizeInMegabytesThreshold;
            options.IgnoreInvalidJournalErrors = db.Configuration.Storage.IgnoreInvalidJournalErrors;
            options.SkipChecksumValidationOnDatabaseLoading = db.Configuration.Storage.SkipChecksumValidationOnDatabaseLoading;
            options.IgnoreDataIntegrityErrorsOfAlreadySyncedTransactions = db.Configuration.Storage.IgnoreDataIntegrityErrorsOfAlreadySyncedTransactions;

            DirectoryExecUtils.SubscribeToOnDirectoryInitializeExec(options, db.Configuration.Storage, db.Name, DirectoryExecUtils.EnvironmentType.Configuration, Logger);

            NotificationsStorage = new NotificationsStorage(db.Name);

            OperationsStorage = new OperationsStorage();

            Environment = StorageLoader.OpenEnvironment(options, StorageEnvironmentWithType.StorageEnvironmentType.Configuration);

            ContextPool = new TransactionContextPool(Environment, db.Configuration.Memory.MaxContextSizeToKeep);
        }

        public void Initialize()
        {
            NotificationsStorage.Initialize(Environment, ContextPool);
            OperationsStorage.Initialize(Environment, ContextPool);
        }

        public void Dispose()
        {
            ContextPool?.Dispose();
            Environment.Dispose();
        }
    }
}
