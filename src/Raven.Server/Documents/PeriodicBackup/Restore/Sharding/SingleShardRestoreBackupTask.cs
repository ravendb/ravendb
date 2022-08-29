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
            return SaveDatabaseRecordAsync(RestoreSettings.DatabaseRecord.DatabaseName, RestoreSettings.DatabaseRecord, databaseValues: null, Result, Progress);
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

        protected override Task ImportLastBackupFileAsync(DocumentDatabase database, DatabaseDestination destination, JsonOperationContext context,
            DatabaseSmugglerOptionsServerSide options, DatabaseRecord databaseRecord, string lastFilePath)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Aviv, DevelopmentHelper.Severity.Normal, 
                "RavenDB-19202 : consider using the most up-to-date database record");

            if (_shardNumber > 0)
                options.OperateOnTypes &= ~DatabaseItemType.Subscriptions;

            return base.ImportLastBackupFileAsync(database, destination, context, options, databaseRecord, lastFilePath);
        }
    }
}
