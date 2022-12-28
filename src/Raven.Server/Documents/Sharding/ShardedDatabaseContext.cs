using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Commands;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Sharding;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Sharding.Executors;
using Raven.Server.Documents.Sharding.NotificationCenter;
using Raven.Server.Documents.Sharding.Queries;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Utils;
using Voron;
using DictionaryExtensions = Raven.Server.Extensions.DictionaryExtensions;

namespace Raven.Server.Documents.Sharding
{
    public partial class ShardedDatabaseContext : IDisposable
    {
        private readonly CancellationTokenSource _databaseShutdown = new CancellationTokenSource();
        public CancellationToken DatabaseShutdown => _databaseShutdown.Token;

        public readonly ServerStore ServerStore;

        private DatabaseRecord _record;
        public QueryMetadataCache QueryMetadataCache = new();
        private readonly Logger _logger;

        public ShardExecutor ShardExecutor;
        public AllOrchestratorNodesExecutor AllOrchestratorNodesExecutor;
        public DatabaseRecord DatabaseRecord => _record;

        public RavenConfiguration Configuration { get; internal set; }

        public readonly SystemTime Time;

        public readonly RachisLogIndexNotifications RachisLogIndexNotifications;

        public readonly ConcurrentSet<TcpConnectionOptions> RunningTcpConnections = new ConcurrentSet<TcpConnectionOptions>();

        public readonly MetricCounters Metrics;

        public ShardedDatabaseContext(ServerStore serverStore, DatabaseRecord record)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "RavenDB-19086 reduce the record to the needed fields");
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "RavenDB-19086 Need to refresh all this in case we will add/remove new shard");

            ServerStore = serverStore;
            _record = record;
            _logger = LoggingSource.Instance.GetLogger<ShardedDatabaseContext>(DatabaseName);

            Time = serverStore.Server.Time;
            Metrics = new MetricCounters();

            UpdateConfiguration(record.Settings);

            Indexes = new ShardedIndexesContext(this, serverStore);

            ShardExecutor = new ShardExecutor(ServerStore, _record, _record.DatabaseName);
            AllOrchestratorNodesExecutor = new AllOrchestratorNodesExecutor(ServerStore, record);

            NotificationCenter = new ShardedDatabaseNotificationCenter(this);
            NotificationCenter.Initialize();

            Streaming = new ShardedStreaming();
            Cluster = new ShardedCluster(this);
            Changes = new ShardedDocumentsChanges(this);
            Operations = new ShardedOperations(this);
            Subscriptions = new ShardedSubscriptions(this, serverStore);
            QueryRunner = new ShardedQueryRunner();
            Smuggler = new ShardedSmugglerContext(this, serverStore);

            RachisLogIndexNotifications = new RachisLogIndexNotifications(_databaseShutdown.Token);
            Replication = new ShardedReplicationContext(this, serverStore);
        }

        public IDisposable AllocateContext(out JsonOperationContext context) => ServerStore.ContextPool.AllocateOperationContext(out context);

        public void UpdateDatabaseRecord(RawDatabaseRecord record, long index)
        {
            UpdateConfiguration(record.Settings);
            
            if (DictionaryExtensions.KeysEqual(record.Sharding.Shards, _record.Sharding.Shards) == false)
            {
                using (var se = ShardExecutor)
                {
                    ShardExecutor = new ShardExecutor(ServerStore, record, record.DatabaseName);
                }
            }

            if (EnumerableExtension.ElementsEqual(record.Sharding.Orchestrator.Topology.Members, _record.Sharding.Orchestrator.Topology.Members) == false)
            {
                using (var ne = AllOrchestratorNodesExecutor)
                {
                    AllOrchestratorNodesExecutor = new AllOrchestratorNodesExecutor(ServerStore, record);
                }
            }

            Indexes.Update(record, index);

            Subscriptions.Update(record);

            Interlocked.Exchange(ref _record, record);

            RachisLogIndexNotifications.NotifyListenersAbout(index, e: null);
        }

        public string DatabaseName => _record.DatabaseName;

        public char IdentityPartsSeparator => _record.Client?.IdentityPartsSeparator ?? Constants.Identities.DefaultSeparator;

        public bool Encrypted => _record.Encrypted;

        public int ShardCount => _record.Sharding.Shards.Count;

        public Dictionary<int, DatabaseTopology> ShardsTopology => _record.Sharding.Shards;

        public int GetShardNumberFor(TransactionOperationContext context, string id) => ShardHelper.GetShardNumberFor(_record.Sharding, context, id);

        public int GetShardNumberFor(ByteStringContext allocator, string id) => ShardHelper.GetShardNumberFor(_record.Sharding, allocator, id);

        public int GetShardNumberFor(ByteStringContext allocator, LazyStringValue id) => ShardHelper.GetShardNumberFor(_record.Sharding, allocator, id);

        public int GetShardNumberFor(Slice id) => ShardHelper.GetShardNumberFor(_record.Sharding, id);

        public int GetShardNumberForIdentity(TransactionOperationContext context, string id) => ShardHelper.GetShardNumberForIdentity(_record.Sharding, context, id, IdentityPartsSeparator);

        public bool HasTopologyChanged(long etag)
        {
            // TODO fix this
            return _record.Topology?.Stamp?.Index > etag;
        }

        private void UpdateConfiguration(Dictionary<string, string> settings)
        {
            Configuration = DatabasesLandlord.CreateDatabaseConfiguration(ServerStore, DatabaseName, settings);
        }

        public static void FillShardingConfiguration(ServerStore serverStore, AddDatabaseCommand addDatabase, ClusterTopology clusterTopology)
        {
            var shardingConfiguration = addDatabase.Record.Sharding;
            if (shardingConfiguration.BucketRanges == null ||
                shardingConfiguration.BucketRanges.Count == 0)
            {
                shardingConfiguration.BucketRanges = new List<ShardBucketRange>();
                var start = 0;
                var step = ShardHelper.NumberOfBuckets / shardingConfiguration.Shards.Count;
                for (int i = 0; i < shardingConfiguration.Shards.Count; i++)
                {
                    shardingConfiguration.BucketRanges.Add(new ShardBucketRange
                    {
                        ShardNumber = i,
                        BucketRangeStart = start
                    });
                    start += step;
                }
            }

            if (addDatabase.RaftCommandIndex == null)
            {
                FillPrefixedSharding(shardingConfiguration);
            }

            var orchestratorTopology = shardingConfiguration.Orchestrator.Topology;
            if (orchestratorTopology.Count == 0)
            {
                serverStore.AssignNodesToDatabase(clusterTopology, addDatabase.Record.DatabaseName, addDatabase.Encrypted, orchestratorTopology);
            }

            Debug.Assert(orchestratorTopology.Count != 0, "Empty orchestrator topology after AssignNodesToDatabase");

            var pool = GetNodesDistribution(clusterTopology, shardingConfiguration.Shards);
            var index = 0;
            var keys = pool.Keys.ToList();
            foreach (var (_, shardTopology) in shardingConfiguration.Shards)
            {
                while (shardTopology.ReplicationFactor > shardTopology.Count)
                {
                    var tag = keys[index++ % keys.Count];

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

        public static async Task UpdatePrefixedShardingIfNeeded(ServerStore serverStore, TransactionOperationContext context, DatabaseRecord databaseRecord, ClusterTopology clusterTopology)
        {
            var existingConfiguration = serverStore.Cluster.ReadShardingConfiguration(context, databaseRecord.DatabaseName);
            if (databaseRecord.Sharding.Prefixed.SequenceEqual(existingConfiguration.Prefixed))
                return;

            var leaderUrl = clusterTopology.GetUrlFromTag(serverStore.LeaderTag);
            using (var clusterRequestExecutor = serverStore.CreateNewClusterRequestExecutor(leaderUrl))
            {
                await HandlePrefixSettingsUpdate(context, databaseRecord, existingConfiguration.Prefixed, clusterRequestExecutor);
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
                if (await AssertNoDocsStartingWith(existingSetting.Prefix, databaseRecord.DatabaseName, context, requestExecutor) == false)
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

                if (await AssertNoDocsStartingWith(setting.Prefix, databaseRecord.DatabaseName, context, requestExecutor) == false)
                    throw new InvalidOperationException(
                        $"Cannot add prefix '{setting.Prefix}' to {nameof(ShardingConfiguration)}.{nameof(ShardingConfiguration.Prefixed)}. " +
                        $"There are existing documents in database '{databaseRecord.DatabaseName}' that start with '{setting.Prefix}'. " +
                        "In order to define sharding by prefix, you cannot have any documents in the database that starts with this prefix.");
                
                AddPrefixedBucketRange(setting, start, shardingConfiguration);
                start += ShardHelper.NumberOfBuckets;
            }

        }

        private static async Task<bool> AssertNoDocsStartingWith(string prefix, string database, JsonOperationContext context, RequestExecutor requestExecutor)
        {
            var command = new GetDocumentsCommand(startWith: prefix,
                startAfter: null, matches: null, exclude: null, start: 0, pageSize: int.MaxValue, metadataOnly: false)
            {
                _database = database
            };
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
        public void Dispose()
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "RavenDB-19086 needs an ExceptionAggregator like DocumentDatabase");

            if (_logger.IsInfoEnabled)
                _logger.Info($"Disposing {nameof(ShardedDatabaseContext)} of {DatabaseName}.");

            _databaseShutdown.Cancel();

            try
            {
                Replication.Dispose();
            }
            catch
            {
                // ignored
            }
            try
            {
                ShardExecutor.Dispose();
            }
            catch
            {
                // ignored
            }

            try
            {
                AllOrchestratorNodesExecutor.Dispose();
            }
            catch
            {
                // ignored
            }

            foreach (var connection in Subscriptions.SubscriptionsConnectionsState)
            {
                connection.Value.Dispose();
            }

            _databaseShutdown.Dispose();
        }
    }
}
