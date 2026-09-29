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
using static Raven.Server.Utils.MetricCacher.Keys;

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

            _logger = db.Loggers.GetLogger<ConfigurationStorage>();
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
            options.SchemaVersion = SchemaUpgrader.CurrentVersion.ConfigurationVersion;
            options.SchemaUpgrader = SchemaUpgrader.Upgrader(SchemaUpgrader.StorageType.Configuration, this, null, null);
            options.Encryption.MasterKey = _db.MasterKey?.ToArray();
            VoronOptionsFromConfiguration.Apply(options, _db.Configuration);
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
