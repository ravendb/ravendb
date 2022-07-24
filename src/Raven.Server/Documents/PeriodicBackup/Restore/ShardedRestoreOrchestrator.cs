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
using Raven.Client.Util;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Sharding;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
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
        private readonly Dictionary<string, RequestExecutor> _singleNodeRequestExecutors = new();
        private ClusterTopology _clusterTopology;

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

            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                if (_serverStore.Cluster.DatabaseExists(context, _restoreConfiguration.DatabaseName))
                    throw new ArgumentException($"Cannot restore data to an existing database named {_restoreConfiguration.DatabaseName}");

                _clusterTopology = _serverStore.GetClusterTopology(context);
            }

            if (string.IsNullOrWhiteSpace(_restoreConfiguration.EncryptionKey) == false)
            {
                _hasEncryptionKey = true;
                var key = Convert.FromBase64String(_restoreConfiguration.EncryptionKey);
                if (key.Length != 256 / 8)
                    throw new InvalidOperationException($"The size of the key must be 256 bits, but was {key.Length * 8} bits.");

                if (AdminDatabasesHandler.NotUsingHttps(_clusterTopology.GetUrlFromTag(_serverStore.NodeTag)))
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


                var x = _serverStore.DatabasesLandlord.TryGetOrCreateDatabase(databaseName);

                await SaveDatabaseRecord(databaseName, databaseRecord);

                var result = await RestoreOnAllShards(onProgress, _restoreConfiguration);

                await SetDatabaseStateBackToNormal();

                return result;
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
                Dispose();
            }
        }

        private void InitializeShardingConfiguration(DatabaseRecord databaseRecord)
        {
            databaseRecord.Sharding = new ShardingConfiguration
            {
                Shards = new DatabaseTopology[_restoreConfiguration.ShardRestoreSettings.Length],
                Orchestrator = new OrchestratorConfiguration
                {
                    Topology = new OrchestratorTopology
                    {
                        Members = new List<string>()
                    }
                }
            };

            var nodes = new HashSet<string>();
            for (var i = 0; i < _restoreConfiguration.ShardRestoreSettings.Length; i++)
            {
                var shardRestoreSetting = _restoreConfiguration.ShardRestoreSettings[i];
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

        private async Task SaveDatabaseRecord(string databaseName, DatabaseRecord databaseRecord, bool isRestore = true)
        {
            var result = await _serverStore.WriteDatabaseRecordAsync(databaseName, databaseRecord, index: null, RaftIdGenerator.NewId(), databaseValues: null, isRestore: isRestore);
            await _serverStore.Cluster.WaitForIndexNotification(result.Index, TimeSpan.FromSeconds(30));
        }

        private RequestExecutor GetRequestExecutorForNode(string tag)
        {
            if (_clusterTopology == null)
            {
                using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    _clusterTopology = _serverStore.GetClusterTopology(context);
                }
            }

            if (_clusterTopology.AllNodes.TryGetValue(tag, out var url) == false)
                throw new InvalidOperationException($"Node tag {tag} is not in cluster topology");

            if (_singleNodeRequestExecutors.TryGetValue(tag, out var requestExecutor) == false)
            {
                _singleNodeRequestExecutors[tag] = requestExecutor = 
                    RequestExecutor.CreateForSingleNodeWithoutConfigurationUpdates(url, databaseName: null, _serverStore.Server.Certificate.Certificate, DocumentConventions.DefaultForServer);
            }

            return requestExecutor;
        }

        private async Task<IOperationResult> RestoreOnAllShards(Action<IOperationProgress> onProgress, RestoreBackupConfigurationBase configuration)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Aviv, DevelopmentHelper.Severity.Normal, "try to replace this with a proper impl. of ServerStore.AddRemoteOperation");

            var shardSettings = configuration.ShardRestoreSettings;
            var tasks = new Task<IOperationResult>[shardSettings.Length];

            for (int i = 0; i < tasks.Length; i++)
            {
                using (_serverStore.ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
                {
                    var cmd = GenerateCommandForShard(ctx, shardNumber: i, configuration.Clone());
                    var nodeTag = shardSettings[i].NodeTag;
                    var executor = GetRequestExecutorForNode(nodeTag);
                    await executor.ExecuteAsync(cmd, ctx, token: _operationCancelToken.Token);
                    var operationIdResult = cmd.Result;

                    var serverOp = new ServerWideOperation(executor, DocumentConventions.DefaultForServer, operationIdResult.OperationId, nodeTag);
                    tasks[i] = serverOp.WaitForCompletionAsync();
                }
            }

            await tasks.WhenAll();

            var result = new ShardedSmugglerResult();
            for (var i = 0; i < tasks.Length; i++)
            {
                var nodeTag = shardSettings[i].NodeTag;
                var r = tasks[i].Result;
                result.CombineWith(r, i, nodeTag);
            }

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
            Debug.Assert(shardNumber < configuration.ShardRestoreSettings?.Length);

            var shardRestoreSetting = configuration.ShardRestoreSettings[shardNumber];

            configuration.DatabaseName = ShardHelper.ToShardName(configuration.DatabaseName, shardNumber);
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

        private void Dispose()
        {
            _operationCancelToken?.Dispose();

            foreach (var kvp in _singleNodeRequestExecutors)
            {
                kvp.Value?.Dispose();
            }
        }

    }
}
