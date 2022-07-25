using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Nito.AsyncEx;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    public class ShardedRestoreOrchestrationTask : AbstractRestoreBackupTask
    {
        private IOperationResult _result;

        public ShardedRestoreOrchestrationTask(ServerStore serverStore,
            RestoreBackupConfigurationBase configuration,
            OperationCancelToken operationCancelToken) : base(serverStore, configuration, restoreSource: null, filesToRestore: null, operationCancelToken)
        {
        }

        protected override async Task Initialize()
        {
            await base.Initialize();
            CreateRestoreSettings();
        }

        protected override async Task OnBeforeRestore()
        {
            ModifyDatabaseRecordSettings();
            InitializeShardingConfiguration();

            var databaseRecord = RestoreSettings.DatabaseRecord;

            // we are currently restoring, shouldn't try to access it
            databaseRecord.DatabaseState = DatabaseStateStatus.RestoreInProgress;
            await SaveDatabaseRecordAsync(DatabaseName, databaseRecord, RestoreSettings.DatabaseValues, Result, Progress);
        }

        protected override async Task Restore()
        {
            var dbSearchResult = ServerStore.DatabasesLandlord.TryGetOrCreateDatabase(DatabaseName);
            var shardedDbContext = dbSearchResult.DatabaseContext;

            _result = await RestoreOnAllShards(Progress, RestoreConfiguration, shardedDbContext);
        }

        protected override void OnAfterRestore()
        {
            DisableOngoingTasksIfNeeded(RestoreSettings.DatabaseRecord);

            Progress.Invoke(Result.Progress);

            // after the db for restore is done, we can safely set the db state to normal and write the DatabaseRecord
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverContext))
            using (serverContext.OpenReadTransaction())
            {
                var databaseRecord = ServerStore.Cluster.ReadDatabase(serverContext, DatabaseName);
                databaseRecord.DatabaseState = DatabaseStateStatus.Normal;

                RestoreSettings.DatabaseRecord = databaseRecord;
            }
        }

        protected override IOperationResult OperationResult() => _result;

        private void InitializeShardingConfiguration()
        {
            var databaseRecord = RestoreSettings.DatabaseRecord;

            databaseRecord.Sharding = new ShardingConfiguration
            {
                Shards = new DatabaseTopology[RestoreConfiguration.ShardRestoreSettings.Length],
                Orchestrator = new OrchestratorConfiguration
                {
                    Topology = new OrchestratorTopology
                    {
                        Members = new List<string>()
                    }
                }
            };

            var nodes = new HashSet<string>();
            for (var i = 0; i < RestoreConfiguration.ShardRestoreSettings.Length; i++)
            {
                var shardRestoreSetting = RestoreConfiguration.ShardRestoreSettings[i];
                databaseRecord.Sharding.Shards[i] = new DatabaseTopology
                {
                    Members = new List<string>
                    {
                        shardRestoreSetting.NodeTag
                    }
                };

                if (nodes.Add(shardRestoreSetting.NodeTag))
                    databaseRecord.Sharding.Orchestrator.Topology.Members.Add(shardRestoreSetting.NodeTag);
            }
        }

        private async Task<IOperationResult> RestoreOnAllShards(Action<IOperationProgress> onProgress, RestoreBackupConfigurationBase configuration,
            ShardedDatabaseContext shardedDatabaseContext)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Aviv, DevelopmentHelper.Severity.Normal, "try to replace this with a proper impl. of ServerStore.AddRemoteOperation");

            var shardSettings = configuration.ShardRestoreSettings;
            var tasks = new Task<IOperationResult>[shardSettings.Length];

            for (int i = 0; i < tasks.Length; i++)
            {
                using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
                {
                    var cmd = GenerateCommandForShard(ctx, position: i, configuration.Clone());
                    var nodeTag = shardSettings[i].NodeTag;
                    var executor = shardedDatabaseContext.AllNodesExecutor.GetRequestExecutorFoNode(nodeTag);
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
            Debug.Assert(position < configuration.ShardRestoreSettings?.Length);

            var shardRestoreSetting = configuration.ShardRestoreSettings[position];
            configuration.DatabaseName = ShardHelper.ToShardName(configuration.DatabaseName, shardRestoreSetting.ShardNumber);
            configuration.ShardRestoreSettings = null;

            switch (configuration)
            {
                case RestoreBackupConfiguration restoreBackupConfiguration:
                    restoreBackupConfiguration.BackupLocation = shardRestoreSetting.BackupPath;
                    break;
                case RestoreFromAzureConfiguration restoreFromAzureConfiguration:
                case RestoreFromGoogleCloudConfiguration restoreFromGoogleCloudConfiguration:
                case RestoreFromS3Configuration restoreFromS3Configuration:
                    DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Aviv, DevelopmentHelper.Severity.Major, "implement sharded cloud restore");
                    throw new NotImplementedException();
                default:
                    throw new ArgumentOutOfRangeException(nameof(configuration));
            }

            return new RestoreBackupOperation(configuration).GetCommand(DocumentConventions.DefaultForServer, context);
        }

        private static IOperationResult CombineResults(IReadOnlyList<Task<IOperationResult>> tasks, IReadOnlyList<ShardRestoreSetting> shardSettings)
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
