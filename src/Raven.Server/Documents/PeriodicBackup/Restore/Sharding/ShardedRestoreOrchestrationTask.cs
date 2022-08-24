using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Nito.AsyncEx;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Backups.Sharding;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Utils;

namespace Raven.Server.Documents.PeriodicBackup.Restore.Sharding
{
    public class ShardedRestoreOrchestrationTask : AbstractRestoreBackupTask
    {
        private IOperationResult _result;

        public ShardedRestoreOrchestrationTask(ServerStore serverStore,
            RestoreBackupConfigurationBase configuration,
            OperationCancelToken operationCancelToken) : base(serverStore, configuration, restoreSource: null, filesToRestore: null, operationCancelToken)
        {
        }

        protected override async Task InitializeAsync()
        {
            await base.InitializeAsync();
            CreateRestoreSettings();
        }

        protected override async Task OnBeforeRestoreAsync()
        {
            ModifyDatabaseRecordSettings();
            InitializeShardingConfiguration();

            var databaseRecord = RestoreSettings.DatabaseRecord;

            // we are currently restoring, shouldn't try to access it
            databaseRecord.DatabaseState = DatabaseStateStatus.RestoreInProgress;
            var index = await SaveDatabaseRecordAsync(DatabaseName, databaseRecord, RestoreSettings.DatabaseValues, Result, Progress);

            var dbSearchResult = ServerStore.DatabasesLandlord.TryGetOrCreateDatabase(DatabaseName);
            var shardedDbContext = dbSearchResult.DatabaseContext;

            var op = new WaitForIndexNotificationOnServerOperation(index);
            await shardedDbContext.AllNodesExecutor.ExecuteParallelForAllAsync(op);
        }

        protected override async Task RestoreAsync()
        {
            var dbSearchResult = ServerStore.DatabasesLandlord.TryGetOrCreateDatabase(DatabaseName);
            var shardedDbContext = dbSearchResult.DatabaseContext;

            _result = await RestoreOnAllShardsAsync(Progress, RestoreConfiguration, shardedDbContext);
        }

        protected override async Task OnAfterRestoreAsync()
        {
            DisableOngoingTasksIfNeeded(RestoreSettings.DatabaseRecord);

            Progress.Invoke(Result.Progress);

            // after the db for restore is done, we can safely set the db state to normal and write the DatabaseRecord
            DatabaseRecord databaseRecord;
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverContext))
            using (serverContext.OpenReadTransaction())
            {
                databaseRecord = ServerStore.Cluster.ReadDatabase(serverContext, DatabaseName);
                databaseRecord.DatabaseState = DatabaseStateStatus.Normal;
            }

            await SaveDatabaseRecordAsync(DatabaseName, databaseRecord, databaseValues: null, Result, Progress);
        }

        protected override IOperationResult OperationResult() => _result;

        private void InitializeShardingConfiguration()
        {
            var databaseRecord = RestoreSettings.DatabaseRecord;

            databaseRecord.Sharding = new ShardingConfiguration
            {
                Shards = new DatabaseTopology[RestoreConfiguration.ShardRestoreSettings.Shards.Length],
                Orchestrator = new OrchestratorConfiguration
                {
                    Topology = new OrchestratorTopology
                    {
                        Members = new List<string>()
                    }
                }
            };

            var clusterTransactionIdBase64 = Guid.NewGuid().ToBase64Unpadded();
            var nodes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            for (var i = 0; i < RestoreConfiguration.ShardRestoreSettings.Shards.Length; i++)
            {
                var shardRestoreSetting = RestoreConfiguration.ShardRestoreSettings.Shards[i];
                databaseRecord.Sharding.Shards[i] = new DatabaseTopology
                {
                    Members = new List<string>
                    {
                        shardRestoreSetting.NodeTag
                    },
                    ClusterTransactionIdBase64 = clusterTransactionIdBase64,
                    ReplicationFactor = 1
                };

                if (nodes.Add(shardRestoreSetting.NodeTag))
                {
                    databaseRecord.Sharding.Orchestrator.Topology.Members.Add(shardRestoreSetting.NodeTag);
                    databaseRecord.Sharding.Orchestrator.Topology.ClusterTransactionIdBase64 = clusterTransactionIdBase64;
                }
            }

            databaseRecord.Sharding.Orchestrator.Topology.ReplicationFactor = databaseRecord.Sharding.Orchestrator.Topology.Members.Count;
        }

        private async Task<IOperationResult> RestoreOnAllShardsAsync(Action<IOperationProgress> onProgress, RestoreBackupConfigurationBase configuration,
            ShardedDatabaseContext shardedDatabaseContext)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Aviv, DevelopmentHelper.Severity.Normal, "try to replace this with a proper impl. of ServerStore.AddRemoteOperation");

            var shardSettings = configuration.ShardRestoreSettings.Shards;
            var tasks = new Task<IOperationResult>[shardSettings.Length];

            for (int i = 0; i < tasks.Length; i++)
            {
                using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
                {
                    var cmd = GenerateCommandForShard(ctx, position: i, configuration.Clone());
                    var nodeTag = shardSettings[i].NodeTag;
                    var executor = shardedDatabaseContext.AllNodesExecutor.GetRequestExecutorForNode(nodeTag);
                    await executor.ExecuteAsync(cmd, ctx, token: OperationCancelToken.Token);
                    var operationIdResult = cmd.Result;

                    var serverOp = new ServerWideOperation(executor, DocumentConventions.DefaultForServer, operationIdResult.OperationId, nodeTag);
                    tasks[i] = serverOp.WaitForCompletionAsync();
                }
            }

            await tasks.WhenAll();

            return CombineResults(tasks, shardSettings);
        }

        private static RavenCommand<OperationIdResult> GenerateCommandForShard(JsonOperationContext context, int position, RestoreBackupConfigurationBase configuration)
        {
            Debug.Assert(position < configuration.ShardRestoreSettings?.Shards.Length);

            var shardRestoreSetting = configuration.ShardRestoreSettings.Shards[position];
            configuration.DatabaseName = ShardHelper.ToShardName(configuration.DatabaseName, shardRestoreSetting.ShardNumber);
            configuration.ShardRestoreSettings = null;

            switch (configuration)
            {
                case RestoreBackupConfiguration restoreBackupConfiguration:
                    restoreBackupConfiguration.BackupLocation = shardRestoreSetting.FolderName;
                    break;
                case RestoreFromS3Configuration restoreFromS3Configuration:
                    restoreFromS3Configuration.Settings.RemoteFolderName = shardRestoreSetting.FolderName;
                    break;
                case RestoreFromAzureConfiguration restoreFromAzureConfiguration:
                    restoreFromAzureConfiguration.Settings.RemoteFolderName = shardRestoreSetting.FolderName;
                    break;
                case RestoreFromGoogleCloudConfiguration restoreFromGoogleCloudConfiguration:
                    restoreFromGoogleCloudConfiguration.Settings.RemoteFolderName = shardRestoreSetting.FolderName;
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(configuration));
            }

            return new RestoreBackupOperation(configuration).GetCommand(DocumentConventions.DefaultForServer, context);
        }

        private static IOperationResult CombineResults(IReadOnlyList<Task<IOperationResult>> tasks, IReadOnlyList<SingleShardRestoreSetting> shardSettings)
        {
            var result = new ShardedSmugglerResult();
            for (var i = 0; i < tasks.Count; i++)
            {
                var r = tasks[i].Result;
                result.CombineWith(r, shardSettings[i].ShardNumber, shardSettings[i].NodeTag);
            }

            return result;
        }
    }
}
