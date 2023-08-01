using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.Config;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Utils;
using Sparrow.Utils;

namespace Raven.Server.Documents.PeriodicBackup.Restore.Sharding
{
    internal sealed class SingleShardRestoreBackupTask : RestoreBackupTask
    {
        private readonly int _shardNumber;

        public SingleShardRestoreBackupTask(ServerStore serverStore, RestoreBackupConfigurationBase restoreConfiguration, List<string> filesToRestore, 
            IRestoreSource restoreSource, OperationCancelToken operationCancelToken) : base(serverStore, restoreConfiguration, restoreSource, filesToRestore, operationCancelToken)
        {
            DatabaseValidation = false;
            DeleteDatabaseOnFailure = false; // orchestrator will handle that

            _shardNumber = ShardHelper.GetShardNumberFromDatabaseName(DatabaseName);
        }

        protected override Task<IDisposable> OnBeforeRestoreAsync()
        {
            CreateDocumentDatabase();
            
            var topology = RestoreSettings.DatabaseRecord.Sharding.Shards[_shardNumber];
            Database.SetIds(topology, RestoreSettings.DatabaseRecord.Sharding.DatabaseId);

            return Task.FromResult<IDisposable>(Database);
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

        protected override void ConfigureSettingsForSmugglerRestore(DocumentDatabase database, SmugglerBase smuggler, string filePath, bool isLastFile)
        {
            base.ConfigureSettingsForSmugglerRestore(database, smuggler, filePath, isLastFile);

            smuggler.OnDatabaseRecordAction += smugglerDatabaseRecord =>
            {
                if (smugglerDatabaseRecord.Sharding == null)
                    throw new InvalidDataException($"'{nameof(DatabaseRecord.Sharding)}' is missing in backup file '{filePath}'. Aborting the restore process");

                RestoreSettings.DatabaseRecord.Sharding.BucketRanges = smugglerDatabaseRecord.Sharding.BucketRanges;
                RestoreSettings.DatabaseRecord.Sharding.Prefixed = smugglerDatabaseRecord.Sharding.Prefixed;

                ShardedDocumentDatabase.CastToShardedDocumentDatabase(database).ShardingConfiguration = smugglerDatabaseRecord.Sharding;
            };

            if (isLastFile == false) 
                return;

            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Aviv, DevelopmentHelper.Severity.Normal,
                "RavenDB-19202 : consider using the most up-to-date database record");
            
            // add the subscription data to just one of the shards
            // get the minimum in order to ensure we get the same shard every time we reach here
            if (GetMinShard(RestoreSettings.DatabaseRecord.Sharding) != _shardNumber)
                smuggler._options.OperateOnTypes &= ~DatabaseItemType.Subscriptions;
        }
        
        private int GetMinShard(ShardingConfiguration config)
        {
            int min = int.MaxValue;
            foreach (var shardNumber in config.Shards.Keys)
            {
                if (shardNumber < min)
                    min = shardNumber;
            }

            return min;
        }
    }
}
