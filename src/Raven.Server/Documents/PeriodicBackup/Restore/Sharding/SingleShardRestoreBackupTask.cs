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

            await ImportSingleBackupFileAsync(database, Progress, Result, lastFilePath, context, destination, options, isLastFile: true);
        }
    }
}
