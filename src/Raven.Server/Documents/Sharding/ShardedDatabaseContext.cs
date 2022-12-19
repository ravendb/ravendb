using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Client;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Sharding.Executors;
using Raven.Server.Documents.Sharding.NotificationCenter;
using Raven.Server.Documents.Sharding.Queries;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Utils;

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

        public ShardedDatabaseContext(ServerStore serverStore, DatabaseRecord record)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "RavenDB-19086 reduce the record to the needed fields");
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "RavenDB-19086 Need to refresh all this in case we will add/remove new shard");

            ServerStore = serverStore;
            _record = record;
            _logger = LoggingSource.Instance.GetLogger<ShardedDatabaseContext>(DatabaseName);

            Time = serverStore.Server.Time;

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
            
            if (AreDictionaryKeysEqual(record.Sharding.Shards, _record.Sharding.Shards) == false)
            {
                ShardExecutor.Dispose();
                ShardExecutor = new ShardExecutor(ServerStore, record, record.DatabaseName);
            }

            if (AreElementsEqual(record.Sharding.Orchestrator.Topology.Members, _record.Sharding.Orchestrator.Topology.Members) == false)
            {
                AllOrchestratorNodesExecutor.Dispose();
                AllOrchestratorNodesExecutor = new AllOrchestratorNodesExecutor(ServerStore, record);
            }

            Indexes.Update(record, index);

            Subscriptions.Update(record);

            Interlocked.Exchange(ref _record, record);

            RachisLogIndexNotifications.NotifyListenersAbout(index, e: null);
        }

        private bool AreElementsEqual(List<string> list1, List<string> list2)
        {
            if (list1.Count != list2.Count)
                return false;

            foreach (var item in list1)
            {
                if (list2.Contains(item) == false)
                    return false;
            }

            return true;
        }

        private bool AreDictionaryKeysEqual(Dictionary<int, DatabaseTopology> dict1, Dictionary<int, DatabaseTopology> dict2)
        {
            if (dict1.Count != dict2.Count)
                return false;

            foreach (var item in dict1)
            {
                if (dict2.ContainsKey(item.Key) == false)
                    return false;
            }

            return true;
        }

        public string DatabaseName => _record.DatabaseName;

        public char IdentityPartsSeparator => _record.Client?.IdentityPartsSeparator ?? Constants.Identities.DefaultSeparator;

        public bool Encrypted => _record.Encrypted;

        public int ShardCount => _record.Sharding.Shards.Count;

        public Dictionary<int, DatabaseTopology> ShardsTopology => _record.Sharding.Shards;

        public int GetShardNumber(int shardBucket) => ShardHelper.GetShardNumber(_record.Sharding.BucketRanges, shardBucket);

        public int GetShardNumber(TransactionOperationContext context, string id)
        {
            var bucket = ShardHelper.GetBucket(context, id);

            return ShardHelper.GetShardNumber(_record.Sharding.BucketRanges, bucket);
        }

        public bool HasTopologyChanged(long etag)
        {
            // TODO fix this
            return _record.Topology?.Stamp?.Index > etag;
        }

        private void UpdateConfiguration(Dictionary<string, string> settings)
        {
            Configuration = DatabasesLandlord.CreateDatabaseConfiguration(ServerStore, DatabaseName, settings);
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
