using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Sharding;
using Raven.Client.Util;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.Sharding;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Server.Collections;
using Sparrow.Utils;

namespace Raven.Server.ServerWide
{
    public sealed class ShardingStore
    {
        public bool BlockPrefixedSharding = true;

        private readonly ServerStore _serverStore;
        public bool ManualMigration = false;

        public ShardingStore([NotNull] ServerStore serverStore)
        {
            _serverStore = serverStore ?? throw new ArgumentNullException(nameof(serverStore));
        }

        public Task<(long Index, object Result)> StartBucketMigration(string database, int bucket, int toShard, string raftId = null)
        {
            var cmd = new StartBucketMigrationCommand(bucket, toShard, database, raftId ?? RaftIdGenerator.NewId());
            return _serverStore.SendToLeaderAsync(cmd);
        }

        public Task<(long Index, object Result)> SourceMigrationCompleted(string database, int bucket, long migrationIndex, string lastChangeVector, string raftId = null)
        {
            var cmd = new SourceMigrationSendCompletedCommand(bucket, migrationIndex, lastChangeVector, database, raftId ?? RaftIdGenerator.NewId());
            return _serverStore.SendToLeaderAsync(cmd);
        }
        
        public static string GenerateDestinationMigrationConfirmRaftId(int bucket, long migrationIndex, string node) => $"Confirm-{bucket}@{migrationIndex}/{node}";

        public Task<(long Index, object Result)> DestinationMigrationConfirm(string database, int bucket, long migrationIndex)
        {
            var raftId = GenerateDestinationMigrationConfirmRaftId(bucket, migrationIndex, _serverStore.NodeTag);
            var cmd = new DestinationMigrationConfirmCommand(bucket, migrationIndex, _serverStore.NodeTag, database, raftId);
            return _serverStore.SendToLeaderAsync(cmd);
        }

        public static string GenerateSourceMigrationCleanupRaftId(int bucket, long migrationIndex, string node) => $"{bucket}@{migrationIndex}-Cleaned-{node}";

        public Task<(long Index, object Result)> SourceMigrationCleanup(string database, int bucket, long migrationIndex)
        {
            var raftId = GenerateSourceMigrationCleanupRaftId(bucket, migrationIndex, _serverStore.NodeTag);
            var cmd = new SourceMigrationCleanupCommand(bucket, migrationIndex, _serverStore.NodeTag, database, raftId);
            return _serverStore.SendToLeaderAsync(cmd);
        }

        public IDisposable RegisterForReshardingStatusUpdate(string database, AsyncQueue<string> messages)
        {
            var del = CreateDelegateForReshardingStatus(database, messages);
            _serverStore.Engine.StateMachine.Changes.DatabaseChanged += del;

            return new DisposableAction(() =>
            {
                _serverStore.Engine.StateMachine.Changes.DatabaseChanged -= del;
            });
        }

        public bool HasActiveMigrations(string database)
        {
            var config = _serverStore.Cluster.ReadShardingConfiguration(database);

            return config.HasActiveMigrations();
        }

        public DocumentConventions DocumentConventionsForShard =>
            new()
            {
                SendApplicationIdentifier = DocumentConventions.DefaultForServer.SendApplicationIdentifier,
                MaxContextSizeToKeep = DocumentConventions.DefaultForServer.MaxContextSizeToKeep,
                HttpPooledConnectionLifetime = DocumentConventions.DefaultForServer.HttpPooledConnectionLifetime,
                UseHttpCompression = _serverStore.Configuration.Sharding.ShardExecutorUseHttpCompression,
                UseHttpDecompression = _serverStore.Configuration.Sharding.ShardExecutorUseHttpDecompression,
                GlobalHttpClientTimeout = _serverStore.Configuration.Sharding.OrchestratorTimeout.AsTimeSpan,
                HttpClientType = typeof(ShardingStore),
                DisableTopologyCache = DocumentConventions.DefaultForServer.DisableTopologyCache,
                DisposeCertificate = DocumentConventions.DefaultForServer.DisposeCertificate,
                CreateHttpClient = handler =>
                {
                    handler.ServerCertificateCustomValidationCallback = ShardingCustomValidationCallback;
                    return new HttpClient(handler);
                }
            };

        public DocumentConventions DocumentConventionsForOrchestrator => DocumentConventionsForShard;

        public bool ShardingCustomValidationCallback(object message, X509Certificate cert, X509Chain chain, SslPolicyErrors errors)
        {
            // We only care about the certificate that the orchestrator going to use and the shard is going to respond
            // In most cases is simply going to be the same certificate

            var cert2 = cert as X509Certificate2;

            if (cert2!.Thumbprint == _serverStore.Server.Certificate.Certificate.Thumbprint)
                return true;

            // Here we handle the case of the server certificate replacement 
            if (cert2.GetPublicKeyPinningHash() == _serverStore.Server.Certificate.Certificate.GetPublicKeyPinningHash())
                return true;

            return RequestExecutor.OnServerCertificateCustomValidationCallback(message, cert, chain, errors);
        }

        private ClusterChanges.DatabaseChangedDelegate CreateDelegateForReshardingStatus(string database, AsyncQueue<string> messages)
        {
            return (name, index, type, _, __) =>
            {
                if (string.Equals(name, database, StringComparison.OrdinalIgnoreCase) == false)
                    return Task.CompletedTask;

                switch (type)
                {
                    case nameof(StartBucketMigrationCommand):
                    case nameof(SourceMigrationSendCompletedCommand):
                    case nameof(DestinationMigrationConfirmCommand):
                    case nameof(SourceMigrationCleanupCommand):
                        var config = _serverStore.Cluster.ReadShardingConfiguration(database);
                        if (config == null)
                            break;

                        if (config.BucketMigrations.Count == 0)
                        {
                            messages.Enqueue($"command {type} was skipped.");
                        }

                        foreach (var (_, migration) in config.BucketMigrations)
                        {
                            messages.Enqueue(migration.ToString());
                        }
                        break;

                    default:
                        break;
                }

                return Task.CompletedTask;
            };
        }

        public void FillShardingConfiguration(DatabaseRecord record, ClusterTopology clusterTopology, long? index)
        {
            var shardingConfiguration = record.Sharding;
            if (shardingConfiguration.BucketRanges == null ||
                shardingConfiguration.BucketRanges.Count == 0)
            {
                shardingConfiguration.BucketRanges = new List<ShardBucketRange>();
                var start = 0;
                var step = ShardHelper.NumberOfBuckets / shardingConfiguration.Shards.Count;
                foreach (var (shardNumber, _) in shardingConfiguration.Shards)
                {
                    shardingConfiguration.BucketRanges.Add(new ShardBucketRange
                    {
                        ShardNumber = shardNumber,
                        BucketRangeStart = start
                    });
                    start += step;
                }
            }

            if (index is null or 0)
            {
                FillPrefixedSharding(shardingConfiguration);
            }

            var orchestratorTopology = shardingConfiguration.Orchestrator.Topology;
            if (orchestratorTopology.Count == 0)
            {
                _serverStore.AssignNodesToDatabase(clusterTopology, record.DatabaseName, record.Encrypted, orchestratorTopology);
            }

            Debug.Assert(orchestratorTopology.Count != 0, "Empty orchestrator topology after AssignNodesToDatabase");

            var pool = GetNodesDistribution(clusterTopology, shardingConfiguration.Shards);
            var i = 0;
            var keys = pool.Keys.ToList();
            foreach (var (_, shardTopology) in shardingConfiguration.Shards)
            {
                while (shardTopology.ReplicationFactor > shardTopology.Count)
                {
                    var tag = keys[i++ % keys.Count];

                    if (pool[tag] > 0 && shardTopology.AllNodes.Contains(tag) == false)
                    {
                        pool[tag]--;
                        shardTopology.Members.Add(tag);
                    }

                    if (pool[tag] == 0)
                        keys.Remove(tag);
                }

                Debug.Assert(shardTopology.Count != 0, "Empty shard topology after AssignNodesToDatabase");
            }
        }

        public async Task UpdatePrefixedShardingIfNeeded(ClusterOperationContext context, DatabaseRecord databaseRecord)
        {
            if (_serverStore.Cluster.DatabaseExists(context, databaseRecord.DatabaseName) == false)
                return;

            var clusterTopology = _serverStore.GetClusterTopology(context);
            var existingConfiguration = _serverStore.Cluster.ReadShardingConfiguration(context, databaseRecord.DatabaseName);
            if (databaseRecord.Sharding.Prefixed.SequenceEqual(existingConfiguration.Prefixed))
                return;

            var urls = databaseRecord.Sharding.Orchestrator.Topology.Members.Select(clusterTopology.GetUrlFromTag).ToArray();
            using (var requestExecutor = RequestExecutor.CreateForServer(urls, databaseRecord.DatabaseName, _serverStore.Server.Certificate.Certificate, DocumentConventions.DefaultForServer))
            {
                await HandlePrefixSettingsUpdate(context, databaseRecord, existingConfiguration.Prefixed, requestExecutor);
            }
        }

        private static void FillPrefixedSharding(ShardingConfiguration shardingConfiguration)
        {
            if (shardingConfiguration.Prefixed is not { Count: > 0 })
                return;

            var start = ShardHelper.NumberOfBuckets;
            foreach (var setting in shardingConfiguration.Prefixed)
            {
                AddPrefixedBucketRange(setting, start, shardingConfiguration);
                start += ShardHelper.NumberOfBuckets;
            }
        }

        private static void AddPrefixedBucketRange(PrefixedShardingSetting setting, int rangeStart, ShardingConfiguration shardingConfiguration)
        {
            if (setting.Prefix.EndsWith('/') == false && setting.Prefix.EndsWith('-') == false)
                throw new InvalidOperationException(
                    $"Cannot add prefix '{setting.Prefix}' to {nameof(ShardingConfiguration)}.{nameof(ShardingConfiguration.Prefixed)}. " +
                    "In order to define sharding by prefix, the prefix string must end with '/' or '-' characters.");

            setting.BucketRangeStart = rangeStart;

            var shards = setting.Shards;
            var step = ShardHelper.NumberOfBuckets / shards.Count;

            foreach (var shardNumber in shards)
            {
                if (shardingConfiguration.Shards.ContainsKey(shardNumber) == false)
                {
                    throw new InvalidDataException($"Cannot assign shard number {shardNumber} to prefix {setting.Prefix}, " +
                                                   $"there's no shard '{shardNumber}' in sharding topology!");
                }

                shardingConfiguration.BucketRanges.Add(new ShardBucketRange
                {
                    ShardNumber = shardNumber,
                    BucketRangeStart = rangeStart
                });
                rangeStart += step;
            }
        }



        private static async Task HandlePrefixSettingsUpdate(JsonOperationContext context, DatabaseRecord databaseRecord, List<PrefixedShardingSetting> existingSettings, RequestExecutor requestExecutor)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Aviv, DevelopmentHelper.Severity.Minor,
                "optimize this and reuse deleted bucket ranges");

            var shardingConfiguration = databaseRecord.Sharding;
            var safeToRemove = new List<PrefixedShardingSetting>();
            var maxBucketRangeStart = 0;

            foreach (var existingSetting in existingSettings)
            {
                bool found = false;
                foreach (var setting in shardingConfiguration.Prefixed)
                {
                    if (setting.Prefix != existingSetting.Prefix)
                        continue;

                    found = true;

                    if (setting.Shards.SequenceEqual(existingSetting.Shards) == false)
                    {
                        // todo

                        // assigned shards were changed for this prefix settings
                        // check if we can change it in Sharding.BucketRanges (no existing docs)
                    }

                    setting.BucketRangeStart = existingSetting.BucketRangeStart;
                    if (maxBucketRangeStart < setting.BucketRangeStart)
                        maxBucketRangeStart = setting.BucketRangeStart;

                    break;
                }

                if (found)
                    continue;

                // existingSetting.Prefix was removed
                if (await AssertNoDocsStartingWith(existingSetting.Prefix, context, requestExecutor) == false)
                    throw new InvalidOperationException(
                        $"Cannot remove prefix '{existingSetting.Prefix}' from {nameof(ShardingConfiguration)}.{nameof(ShardingConfiguration.Prefixed)}. " +
                        $"There are existing documents in database '{databaseRecord.DatabaseName}' that start with '{existingSetting.Prefix}'. " +
                        "In order to remove a sharding by prefix setting, you cannot have any documents in the database that starts with this prefix.");

                safeToRemove.Add(existingSetting);
            }

            // remove deleted prefixes from Sharding.BucketRanges
            foreach (var setting in safeToRemove)
            {
                for (int index = 0; index < shardingConfiguration.BucketRanges.Count; index++)
                {
                    var range = shardingConfiguration.BucketRanges[index];
                    if (range.BucketRangeStart != setting.BucketRangeStart)
                        continue;

                    shardingConfiguration.BucketRanges.RemoveRange(index, setting.Shards.Count);
                    break;
                }
            }

            var start = maxBucketRangeStart + ShardHelper.NumberOfBuckets;

            // add new prefixed settings to Sharding.BucketRanges
            foreach (var setting in shardingConfiguration.Prefixed)
            {
                if (setting.BucketRangeStart != 0)
                    continue; // already added to BucketRanges

                if (await AssertNoDocsStartingWith(setting.Prefix, context, requestExecutor) == false)
                    throw new InvalidOperationException(
                        $"Cannot add prefix '{setting.Prefix}' to {nameof(ShardingConfiguration)}.{nameof(ShardingConfiguration.Prefixed)}. " +
                        $"There are existing documents in database '{databaseRecord.DatabaseName}' that start with '{setting.Prefix}'. " +
                        "In order to define sharding by prefix, you cannot have any documents in the database that starts with this prefix.");

                AddPrefixedBucketRange(setting, start, shardingConfiguration);
                start += ShardHelper.NumberOfBuckets;
            }

        }

        private static async Task<bool> AssertNoDocsStartingWith(string prefix, JsonOperationContext context, RequestExecutor requestExecutor)
        {
            var command = new GetDocumentsCommand(requestExecutor.Conventions, startWith: prefix,
                startAfter: null, matches: null, exclude: null, start: 0, pageSize: int.MaxValue, metadataOnly: false);

            await requestExecutor.ExecuteAsync(command, context, sessionInfo: null);
            return command.Result.Results.Length == 0;
        }

        private static Dictionary<string, int> GetNodesDistribution(ClusterTopology clusterTopology, Dictionary<int, DatabaseTopology> shards)
        {
            var total = 0;
            var pool = new Dictionary<string, int>(); // tag, number of occurrences

            foreach (var node in clusterTopology.AllNodes)
            {
                pool[node.Key] = 0;
            }

            foreach (var (shardNumber, shardTopology) in shards)
            {
                total += shardTopology.ReplicationFactor;
            }

            var perNode = total / pool.Count;
            foreach (var node in pool.Keys)
            {
                pool[node] = perNode;
                total -= perNode;
            }

            foreach (var node in pool.Keys)
            {
                if (total == 0)
                    break;

                pool[node]++;
                total--;
            }

            return pool;
        }
    }
}
