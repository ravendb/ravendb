using System;
using System.Collections.Generic;
using Raven.Client.Server.Operations;
using Raven.Server.Documents.Expiration;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.Versioning;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Documents
{
    public class BundleLoader : IDisposable
    {
        private readonly Logger _logger;

        private readonly ServerStore _serverStore;
        private readonly DocumentDatabase _database;
        public VersioningStorage VersioningStorage;
        public ExpiredDocumentsCleaner ExpiredDocumentsCleaner;
        public PeriodicBackupRunner PeriodicBackupRunner;

        public BundleLoader(DocumentDatabase database, ServerStore serverStore)
        {
            _database = database;
            _serverStore = serverStore;
            
            _logger = LoggingSource.Instance.GetLogger<BundleLoader>(_database.Name);

            if (_serverStore == null)
                return;

            InitializeBundles();
        }

        public void HandleDatabaseRecordChange()
        {
            lock(this)
            {
                var dbRecord = _serverStore.LoadDatabaseRecord(_database.Name);
                if (dbRecord == null)
                    return;

                VersioningStorage = VersioningStorage.LoadConfigurations(_database, dbRecord, VersioningStorage);
                ExpiredDocumentsCleaner = ExpiredDocumentsCleaner.LoadConfigurations(_database, dbRecord, ExpiredDocumentsCleaner);
                PeriodicBackupRunner.UpdateConfigurations(dbRecord);
            }
        }

        /// <summary>
        /// Configure the database bundles if no changes has accord or when server start
        /// </summary>
        public void InitializeBundles()
        {
            PeriodicBackupRunner = new PeriodicBackupRunner(_database, _serverStore);
            HandleDatabaseRecordChange();
        }

        public List<string> GetActiveBundles()
        {
            var res = new List<string>();
            if (ExpiredDocumentsCleaner != null)
                res.Add(BundleTypeToName[ExpiredDocumentsCleaner.GetType()]);
            if (VersioningStorage != null)
                res.Add(BundleTypeToName[VersioningStorage.GetType()]);
            if (PeriodicBackupRunner != null)
                res.Add(BundleTypeToName[PeriodicBackupRunner.GetType()]);
            return res;
        }

        private static readonly Dictionary<Type, string> BundleTypeToName = new Dictionary<Type, string>
        {
            {typeof(VersioningStorage), "Versioning"},
            {typeof(ExpiredDocumentsCleaner), "Expiration"},
            {typeof(PeriodicBackupRunner), "PeriodicBackup"}
        };


        public void Dispose()
        {
            var exceptionAggregator = new ExceptionAggregator(_logger, $"Could not dispose {nameof(BundleLoader)}");
            exceptionAggregator.Execute(() =>
            {
                ExpiredDocumentsCleaner?.Dispose();
                ExpiredDocumentsCleaner = null;
            });
            exceptionAggregator.Execute(() =>
            {
                PeriodicBackupRunner?.Dispose();
                PeriodicBackupRunner = null;
            });
            exceptionAggregator.ThrowIfNeeded();
        }

        public DynamicJsonValue GetBackupInfo()
        {
            return PeriodicBackupRunner?.GetBackupInfo()?.ToJson();
        }
    }
}