using System;
using System.Linq;
using Raven.Server.Documents.Operations;
using Raven.Server.Logging;
using Raven.Server.ServerWide.Context;
using Raven.Server.Storage.Layout;
using Raven.Server.Storage.Schema;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Logging;
using Sparrow.Server.Logging;
using Voron;

namespace Raven.Server.Documents
{
    public sealed class ConfigurationStorage : IDisposable
    {
        private readonly DocumentDatabase _db;

        private readonly RavenLogger _logger;

        public TransactionContextPool ContextPool { get; private set; }

        public OperationsStorage OperationsStorage { get; }

        public StorageEnvironment Environment { get; private set; }

        public ConfigurationStorage(DocumentDatabase db)
        {
            _db = db;

            OperationsStorage = new OperationsStorage();

            _logger = RavenLogManager.Instance.GetLoggerForDatabase<ConfigurationStorage>(db);
        }

        public void Initialize()
        {
            var path = _db.Configuration.Core.DataDirectory.Combine("Configuration");
            string tempPath = null;
            if (_db.Configuration.Storage.TempPath != null)
            {
                tempPath = _db.Configuration.Storage.TempPath.Combine("Configuration").ToFullPath();
            }

            var options = _db.Configuration.Core.RunInMemory
                ? StorageEnvironmentOptions.CreateMemoryOnly(path.FullPath, tempPath, _db.IoChanges, _db.CatastrophicFailureNotification, LoggingResource.Database(_db.Name), LoggingComponent.Configuration)
                : StorageEnvironmentOptions.ForPath(path.FullPath, tempPath, null, _db.IoChanges, _db.CatastrophicFailureNotification, LoggingResource.Database(_db.Name), LoggingComponent.Configuration);

            options.OnNonDurableFileSystemError += _db.HandleNonDurableFileSystemError;
            options.OnRecoverableFailure += _db.HandleRecoverableFailure;
            options.OnRecoveryError += _db.HandleOnConfigurationRecoveryError;
            options.OnIntegrityErrorOfAlreadySyncedData += _db.HandleOnConfigurationIntegrityErrorOfAlreadySyncedData;
            options.CompressTxAboveSizeInBytes = _db.Configuration.Storage.CompressTxAboveSize.GetValue(SizeUnit.Bytes);
            options.SchemaVersion = SchemaUpgrader.CurrentVersion.ConfigurationVersion;
            options.SchemaUpgrader = SchemaUpgrader.Upgrader(SchemaUpgrader.StorageType.Configuration, this, null, null);
            options.ForceUsing32BitsPager = _db.Configuration.Storage.ForceUsing32BitsPager;
            options.EnablePrefetching = _db.Configuration.Storage.EnablePrefetching;
            options.DiscardVirtualMemory = _db.Configuration.Storage.DiscardVirtualMemory;
            options.TimeToSyncAfterFlushInSec = (int)_db.Configuration.Storage.TimeToSyncAfterFlush.AsTimeSpan.TotalSeconds;
            options.Encryption.MasterKey = _db.MasterKey?.ToArray();

            options.DoNotConsiderMemoryLockFailureAsCatastrophicError = _db.Configuration.Security.DoNotConsiderMemoryLockFailureAsCatastrophicError;
            if (_db.Configuration.Storage.MaxScratchBufferSize.HasValue)
                options.MaxScratchBufferSize = _db.Configuration.Storage.MaxScratchBufferSize.Value.GetValue(SizeUnit.Bytes);
            options.PrefetchSegmentSize = _db.Configuration.Storage.PrefetchBatchSize.GetValue(SizeUnit.Bytes);
            options.PrefetchResetThreshold = _db.Configuration.Storage.PrefetchResetThreshold.GetValue(SizeUnit.Bytes);
            options.SyncJournalsCountThreshold = _db.Configuration.Storage.SyncJournalsCountThreshold;
            options.IgnoreInvalidJournalErrors = _db.Configuration.Storage.IgnoreInvalidJournalErrors;
            options.SkipChecksumValidationOnDatabaseLoading = _db.Configuration.Storage.SkipChecksumValidationOnDatabaseLoading;
            options.IgnoreDataIntegrityErrorsOfAlreadySyncedTransactions = _db.Configuration.Storage.IgnoreDataIntegrityErrorsOfAlreadySyncedTransactions;
            options.MaxNumberOfRecyclableJournals = _db.Configuration.Storage.MaxNumberOfRecyclableJournals;

            try
            {
                DirectoryExecUtils.SubscribeToOnDirectoryInitializeExec(options, _db.Configuration.Storage, _db.Name, DirectoryExecUtils.EnvironmentType.Configuration, _logger);

                Environment = StorageLoader.OpenEnvironment(options, StorageEnvironmentWithType.StorageEnvironmentType.Configuration);
            }
            catch
            {
                options.Dispose();
                throw;
            }

            ContextPool = new TransactionContextPool(_logger, Environment, _db.Configuration.Memory.MaxContextSizeToKeep);

            OperationsStorage.Initialize(Environment, ContextPool);
        }

        public void Dispose()
        {
            ContextPool?.Dispose();
            Environment?.Dispose();
        }
    }
}
