using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Server.Config;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.PeriodicBackup.Restore.Sharding
{
    internal class SingleShardRestoreBackupTask : RestoreBackupTask
    {
        private readonly int _shardNumber;

        public SingleShardRestoreBackupTask(ServerStore serverStore, RestoreBackupConfigurationBase restoreConfiguration, List<string> filesToRestore, 
            IRestoreSource restoreSource, OperationCancelToken operationCancelToken) : base(serverStore, restoreConfiguration, restoreSource, filesToRestore, operationCancelToken)
        {
            DatabaseValidation = false;

            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Aviv, DevelopmentHelper.Severity.Major, "test encrypted backup & restore");
            HasEncryptionKey = false;

            _shardNumber = ShardHelper.GetShardNumber(DatabaseName);
        }
        
        protected override Task OnBeforeRestoreAsync()
        {
            CreateDocumentDatabase();
            
            var topology = RestoreSettings.DatabaseRecord.Sharding.Shards[_shardNumber];
            Database.SetIds(topology, RestoreSettings.DatabaseRecord.Sharding.DatabaseId);

            return Task.CompletedTask;
        }

        protected override Task OnAfterRestoreAsync()
        {
            SmugglerBase.EnsureProcessed(Result, skipped: false);
            Progress.Invoke(Result.Progress);
            return Task.CompletedTask;
        }

        protected override DatabaseRecord GetDatabaseRecord()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverContext))
            using (serverContext.OpenReadTransaction())
            {
                var databaseRecord = ServerStore.Cluster.ReadDatabase(serverContext, ShardHelper.ToDatabaseName(DatabaseName));
                Debug.Assert(databaseRecord != null);

                return databaseRecord;
            }
        }

        protected override RavenConfiguration CreateDatabaseConfiguration()
        {
            return DatabasesLandlord.CreateDatabaseConfiguration(ServerStore, DatabaseName, RestoreSettings.DatabaseRecord.Settings);
        }

        protected override async Task ImportLastBackupFileAsync(DocumentDatabase database, DatabaseDestination destination, JsonOperationContext context,
            DatabaseSmugglerOptionsServerSide options, DatabaseRecord databaseRecord, string lastFilePath)
        {
            if (_shardNumber > 0)
                options.OperateOnTypes &= ~DatabaseItemType.Subscriptions;

            await ImportSingleBackupFileAsync(database, Progress, Result, lastFilePath, context, destination, options, isLastFile: true, 
            onDatabaseRecordAction: smugglerDatabaseRecord =>
            {
                databaseRecord.ConflictSolverConfig = smugglerDatabaseRecord.ConflictSolverConfig;
                foreach (var setting in smugglerDatabaseRecord.Settings)
                {
                    databaseRecord.Settings[setting.Key] = setting.Value;
                }

                databaseRecord.SqlEtls = smugglerDatabaseRecord.SqlEtls;
                databaseRecord.RavenEtls = smugglerDatabaseRecord.RavenEtls;
                databaseRecord.PeriodicBackups = smugglerDatabaseRecord.PeriodicBackups;
                databaseRecord.ExternalReplications = smugglerDatabaseRecord.ExternalReplications;
                databaseRecord.Sorters = smugglerDatabaseRecord.Sorters;
                databaseRecord.Analyzers = smugglerDatabaseRecord.Analyzers;
                databaseRecord.SinkPullReplications = smugglerDatabaseRecord.SinkPullReplications;
                databaseRecord.HubPullReplications = smugglerDatabaseRecord.HubPullReplications;
                databaseRecord.Revisions = smugglerDatabaseRecord.Revisions;
                databaseRecord.Expiration = smugglerDatabaseRecord.Expiration;
                databaseRecord.RavenConnectionStrings = smugglerDatabaseRecord.RavenConnectionStrings;
                databaseRecord.SqlConnectionStrings = smugglerDatabaseRecord.SqlConnectionStrings;
                databaseRecord.Client = smugglerDatabaseRecord.Client;
                databaseRecord.TimeSeries = smugglerDatabaseRecord.TimeSeries;
                databaseRecord.DocumentsCompression = smugglerDatabaseRecord.DocumentsCompression;
                databaseRecord.LockMode = smugglerDatabaseRecord.LockMode;
                databaseRecord.OlapConnectionStrings = smugglerDatabaseRecord.OlapConnectionStrings;
                databaseRecord.OlapEtls = smugglerDatabaseRecord.OlapEtls;
                databaseRecord.ElasticSearchEtls = smugglerDatabaseRecord.ElasticSearchEtls;
                databaseRecord.ElasticSearchConnectionStrings = smugglerDatabaseRecord.ElasticSearchConnectionStrings;
                databaseRecord.QueueEtls = smugglerDatabaseRecord.QueueEtls;
                databaseRecord.QueueConnectionStrings = smugglerDatabaseRecord.QueueConnectionStrings;

                // need to enable revisions before import
                database.DocumentsStorage.RevisionsStorage.InitializeFromDatabaseRecord(smugglerDatabaseRecord);
            });
        }
    }
}
