using System;
using System.Collections.Generic;
using System.Linq;
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
using Raven.Client.Util;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Sharding;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.System;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    public class ShardedRestoreOrchestrator
    {
        private readonly RestoreBackupConfigurationBase _restoreConfiguration;
        private readonly ServerStore _serverStore;
        private readonly OperationCancelToken _operationCancelToken;
        private readonly bool _hasEncryptionKey;

        public ShardedRestoreOrchestrator(ServerStore serverStore,
            RestoreBackupConfigurationBase configuration,
            OperationCancelToken operationCancelToken)
        {
            _serverStore = serverStore;
            _operationCancelToken = operationCancelToken;
            _restoreConfiguration = configuration;

            var dataDirectoryThatWillBeUsed = string.IsNullOrWhiteSpace(_restoreConfiguration.DataDirectory) ?
                                       _serverStore.Configuration.Core.DataDirectory.FullPath :
                                       new PathSetting(_restoreConfiguration.DataDirectory, _serverStore.Configuration.Core.DataDirectory.FullPath).FullPath;

            if (ResourceNameValidator.IsValidResourceName(_restoreConfiguration.DatabaseName, dataDirectoryThatWillBeUsed, out string errorMessage) == false)
                throw new InvalidOperationException(errorMessage);

            _serverStore.EnsureNotPassiveAsync().Wait(_operationCancelToken.Token);

            if (string.IsNullOrWhiteSpace(_restoreConfiguration.EncryptionKey) == false)
            {
                _hasEncryptionKey = true;
                var key = Convert.FromBase64String(_restoreConfiguration.EncryptionKey);
                if (key.Length != 256 / 8)
                    throw new InvalidOperationException($"The size of the key must be 256 bits, but was {key.Length * 8} bits.");

                ClusterTopology clusterTopology;
                using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    if (_serverStore.Cluster.DatabaseExists(context, _restoreConfiguration.DatabaseName))
                        throw new ArgumentException($"Cannot restore data to an existing database named {_restoreConfiguration.DatabaseName}");

                    clusterTopology = _serverStore.GetClusterTopology(context);
                }

                if (AdminDatabasesHandler.NotUsingHttps(clusterTopology.GetUrlFromTag(_serverStore.NodeTag)))
                    throw new InvalidOperationException("Cannot restore an encrypted database to a node which doesn't support SSL!");
            }
        }

        public async Task<IOperationResult> Execute(Action<IOperationProgress> onProgress)
        {
            try
            {
                var databaseName = _restoreConfiguration.DatabaseName;
                var databaseRecord = new DatabaseRecord(databaseName)
                {
                    // we only have a smuggler restore
                    // use the encryption key to encrypt the database
                    Encrypted = _hasEncryptionKey,
                };
                DatabaseHelper.Validate(databaseName, databaseRecord, _serverStore.Configuration);

                if (_hasEncryptionKey)
                {
                    // save the encryption key so we'll be able to access the database
                    _serverStore.PutSecretKey(_restoreConfiguration.EncryptionKey,
                        _restoreConfiguration.DatabaseName, overwrite: false);
                }

                InitializeShardingConfiguration(databaseRecord);

                databaseRecord.DatabaseState = DatabaseStateStatus.RestoreInProgress;

                await SaveDatabaseRecord(databaseName, databaseRecord);

                return await RestoreOnAllShards(onProgress, databaseRecord, _restoreConfiguration);
            }
            catch (Exception e)
            {
                var alert = AlertRaised.Create(
                    _restoreConfiguration.DatabaseName,
                    "Failed to restore sharded database",
                    $"Could not restore sharded database named {_restoreConfiguration.DatabaseName}",
                    AlertType.RestoreError,
                    NotificationSeverity.Error,
                    details: new ExceptionDetails(e));

                _serverStore.NotificationCenter.Add(alert);
                throw;
            }
            finally
            {
                _operationCancelToken?.Dispose();
            }
        }

        private void InitializeShardingConfiguration(DatabaseRecord databaseRecord)
        {
            databaseRecord.Sharding = new ShardingConfiguration
            {
                Shards = new DatabaseTopology[_restoreConfiguration.ShardRestoreSettings.Length], //todo
                Orchestrator = new OrchestratorConfiguration { Topology = new OrchestratorTopology { Members = new List<string>() } }
            };

            var nodes = new HashSet<string>();
            for (var i = 0; i < _restoreConfiguration.ShardRestoreSettings.Length; i++)
            {
                var shardRestoreSetting = _restoreConfiguration.ShardRestoreSettings[i];
                databaseRecord.Sharding.Shards[i] = new DatabaseTopology { Members = new List<string> { shardRestoreSetting.NodeTag } };

                if (nodes.Add(shardRestoreSetting.NodeTag))
                    databaseRecord.Sharding.Orchestrator.Topology.Members.Add(shardRestoreSetting.NodeTag);
            }
        }

        private async Task SaveDatabaseRecord(string databaseName, DatabaseRecord databaseRecord, bool isRestore = true)
        {
            var result = await _serverStore.WriteDatabaseRecordAsync(databaseName, databaseRecord, index: null, RaftIdGenerator.NewId(), databaseValues: null, isRestore: isRestore);
            await _serverStore.Cluster.WaitForIndexNotification(result.Index, TimeSpan.FromSeconds(30));
        }

        private async Task<IOperationResult> RestoreOnAllShards(Action<IOperationProgress> onProgress,
            DatabaseRecord databaseRecord, RestoreBackupConfigurationBase configuration)
        {
            var databaseContext = new ShardedDatabaseContext(_serverStore, databaseRecord);
            var tasks = new Task<IOperationResult>[databaseContext.NumberOfShardNodes];
            
            for (int i = 0; i < tasks.Length; i++)
            {
                using (databaseContext.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
                {
                    var cmd = GenerateCommandForShard(ctx, shardNumber: i, configuration.Clone());
                    var nodeTag = configuration.ShardRestoreSettings[i].NodeTag;
                    var operationIdResult = await databaseContext.AllNodesExecutor.ExecuteForNodeAsync(cmd, nodeTag);

                    var requestExecutor = databaseContext.AllNodesExecutor.GetRequestExecutorForNode(nodeTag);
                    var serverOp = new ServerWideOperation(requestExecutor, DocumentConventions.DefaultForServer, operationIdResult.OperationId, nodeTag);
                    tasks[i] = serverOp.WaitForCompletionAsync();
                }
            }

            await tasks.WhenAll();

            var result = new ShardedSmugglerResult();
            for (var i = 0; i < tasks.Length; i++)
            {
                var nodeTag = configuration.ShardRestoreSettings[i].NodeTag;
                var r = tasks[i].Result;
                result.CombineWith(r, i, nodeTag);
            }

            await SetDatabaseStateBackToNormal();

            return result;
        }

        private async Task SetDatabaseStateBackToNormal()
        {
            // after we finished restoring on all nodes, change database state back to normal
            var databaseName = _restoreConfiguration.DatabaseName;
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverContext))
            using (serverContext.OpenReadTransaction())
            {
                var databaseRecord = _serverStore.Cluster.ReadDatabase(serverContext, databaseName);
                databaseRecord.DatabaseState = DatabaseStateStatus.Normal;
                await SaveDatabaseRecord(databaseName, databaseRecord, false);
            }
        }

        private static RavenCommand<OperationIdResult> GenerateCommandForShard(JsonOperationContext context, int shardNumber, RestoreBackupConfigurationBase configuration)
        {
            var shardRestoreSetting = configuration.ShardRestoreSettings?.SingleOrDefault(s => s.ShardNumber == shardNumber);
            if (shardRestoreSetting == null)
                return default; //todo

            string databaseName = configuration.DatabaseName;
            configuration.DatabaseName = $"{databaseName}${shardNumber}";
            configuration.ShardRestoreSettings = null;

            switch (configuration)
            {
                case RestoreBackupConfiguration restoreBackupConfiguration:
                    restoreBackupConfiguration.BackupLocation = shardRestoreSetting.BackupPath;
                    break;
                case RestoreFromAzureConfiguration restoreFromAzureConfiguration:
                case RestoreFromGoogleCloudConfiguration restoreFromGoogleCloudConfiguration:
                case RestoreFromS3Configuration restoreFromS3Configuration:
                    Sparrow.Utils.DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Aviv, DevelopmentHelper.Severity.Major, "implement sharded cloud restore");
                    throw new NotImplementedException();
                default:
                    throw new ArgumentOutOfRangeException(nameof(configuration));
            }

            return new RestoreBackupOperation(configuration, shardRestoreSetting.NodeTag)
                .GetCommand(DocumentConventions.DefaultForServer, context);
        }

    }
}
