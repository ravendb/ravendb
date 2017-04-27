using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Server.Operations;
using Raven.Server.Documents.Expiration;
using Raven.Server.Documents.PeriodicExport;
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
        public PeriodicExportRunner PeriodicExportRunner;

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
                TransactionOperationContext context;
                using (_serverStore.ContextPool.AllocateOperationContext(out context))
                {
                    context.OpenReadTransaction();
                    var dbRecord = _serverStore.Cluster.ReadDatabase(context, _database.Name);
                    if (dbRecord == null)
                        return;
                    VersioningStorage = VersioningStorage.LoadConfigurations(_database, dbRecord, VersioningStorage);
                    ExpiredDocumentsCleaner = ExpiredDocumentsCleaner.LoadConfigurations(_database, dbRecord, ExpiredDocumentsCleaner);
                    PeriodicExportRunner = PeriodicExportRunner.LoadConfigurations(_database, dbRecord, PeriodicExportRunner);
                }
            }
        }

        /// <summary>
        /// Configure the database bundles if no changes has accord or when server start
        /// </summary>
        public void InitializeBundles()
        {

            HandleDatabaseRecordChange();
        }

        public List<string> GetActiveBundles()
        {
            var res = new List<string>();
            if (ExpiredDocumentsCleaner != null)
                res.Add(BundleTypeToName[ExpiredDocumentsCleaner.GetType()]);
            if (VersioningStorage != null)
                res.Add(BundleTypeToName[VersioningStorage.GetType()]);
            if (PeriodicExportRunner != null)
                res.Add(BundleTypeToName[PeriodicExportRunner.GetType()]);
            return res;
        }

        private static readonly Dictionary<Type, string> BundleTypeToName = new Dictionary<Type, string>
        {
            {typeof(VersioningStorage), "Versioning"},
            {typeof(ExpiredDocumentsCleaner), "Expiration"},
            {typeof(PeriodicExportRunner), "PeriodicExport"}
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
                PeriodicExportRunner?.Dispose();
                PeriodicExportRunner = null;
            });
            exceptionAggregator.ThrowIfNeeded();
        }

        public DynamicJsonValue GetBackupInfo()
        {
            if (PeriodicExportRunner == null)
            {
                return null;
            }

            return new DynamicJsonValue
            {
                [nameof(BackupInfo.IncrementalBackupInterval)] = PeriodicExportRunner.IncrementalInterval,
                [nameof(BackupInfo.FullBackupInterval)] = PeriodicExportRunner.FullExportInterval,
                [nameof(BackupInfo.LastIncrementalBackup)] = PeriodicExportRunner.ExportTime,
                [nameof(BackupInfo.LastFullBackup)] = PeriodicExportRunner.FullExportTime
            };
        }
    }
}