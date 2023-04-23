using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Client;
using Raven.Client.Extensions;
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

        private readonly DatabasesLandlord.StateChange _orchestratorStateChange;
        private readonly DatabasesLandlord.StateChange _urlUpdateStateChange;

        public ShardedDatabaseContext(ServerStore serverStore, DatabaseRecord record)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "RavenDB-19086 reduce the record to the needed fields");
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "RavenDB-19086 Need to refresh all this in case we will add/remove new shard");

            ServerStore = serverStore;
            _record = record;
            _logger = LoggingSource.Instance.GetLogger<ShardedDatabaseContext>(DatabaseName);

            _orchestratorStateChange = new DatabasesLandlord.StateChange(ServerStore, record.DatabaseName, _logger, OnDatabaseRecordChange, 0, _databaseShutdown.Token);
            _urlUpdateStateChange = new DatabasesLandlord.StateChange(ServerStore, record.DatabaseName, _logger, OnUrlChange, 0, _databaseShutdown.Token);

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

            SubscriptionsStorage = new ShardedSubscriptionsStorage(this, serverStore);
            OngoingTasks = new ShardedOngoingTasks(this);

            QueryRunner = new ShardedQueryRunner();
            Smuggler = new ShardedSmugglerContext(this, serverStore);

            RachisLogIndexNotifications = new RachisLogIndexNotifications(_databaseShutdown.Token);
            Replication = new ShardedReplicationContext(this, serverStore);

            CompareExchangeStorage = new ShardedCompareExchangeStorage(this);
            CompareExchangeStorage.Initialize(DatabaseName);
        }

        public IDisposable AllocateOperationContext(out JsonOperationContext context) => ServerStore.ContextPool.AllocateOperationContext(out context);

        public void UpdateDatabaseRecord(DatabaseRecord record, long index)
        {
            try
            {
                DatabasesLandlord.NotifyFeaturesAboutStateChange(record, index, _orchestratorStateChange);
                RachisLogIndexNotifications.NotifyListenersAbout(index, e: null);
            }
            catch (Exception e)
            {
                RachisLogIndexNotifications.NotifyListenersAbout(index, e);
                throw;
            }
        }

        private void OnDatabaseRecordChange(DatabaseRecord record, long index)
        {
            UpdateConfiguration(record.Settings);

            if (DictionaryExtensions.KeysEqual(record.Sharding.Shards, _record.Sharding.Shards) == false)
            {
                ShardExecutor = new ShardExecutor(ServerStore, record, record.DatabaseName);
            }

            if (EnumerableExtension.ElementsEqual(record.Sharding.Orchestrator.Topology.Members, _record.Sharding.Orchestrator.Topology.Members) == false)
            {
                AllOrchestratorNodesExecutor = new AllOrchestratorNodesExecutor(ServerStore, record);
            }

            Indexes.Update(record, index);

            SubscriptionsStorage.Update(record);

            Interlocked.Exchange(ref _record, record);
        }

        private void OnUrlChange(DatabaseRecord record, long index)
        {
            // we explicitly do not dispose the old executors here to avoid possible memory invalidation and since this is expected to be rare.
            // So we rely on the GC to dispose them via the finalizer
            
            ShardExecutor = new ShardExecutor(ServerStore, _record, _record.DatabaseName);

            AllOrchestratorNodesExecutor = new AllOrchestratorNodesExecutor(ServerStore, _record);
        }

        public void UpdateUrls(long index) => DatabasesLandlord.NotifyFeaturesAboutStateChange(_record, index, _urlUpdateStateChange);

        public string DatabaseName => _record.DatabaseName;

        public char IdentityPartsSeparator => _record.Client?.IdentityPartsSeparator ?? Constants.Identities.DefaultSeparator;

        public bool Encrypted => _record.Encrypted;

        public int ShardCount => _record.Sharding.Shards.Count;

        public Dictionary<int, DatabaseTopology> ShardsTopology => _record.Sharding.Shards;

        public (int ShardNumber, int Bucket) GetShardNumberAndBucketFor(TransactionOperationContext context, string id) => ShardHelper.GetShardNumberAndBucketFor(_record.Sharding, context, id);

        public int GetShardNumberFor(TransactionOperationContext context, string id) => ShardHelper.GetShardNumberFor(_record.Sharding, context, id);

        public int RecalculateShardNumberFor(TransactionOperationContext context, string id) => ShardHelper.GetShardNumberFor(_record.Sharding, context, id, IdentityPartsSeparator);

        public int GetShardNumberFor(ByteStringContext allocator, string id) => ShardHelper.GetShardNumberFor(_record.Sharding, allocator, id);

        public int GetShardNumberFor(ByteStringContext allocator, LazyStringValue id) => ShardHelper.GetShardNumberFor(_record.Sharding, allocator, id);

        public int GetShardNumberFor(Slice id) => ShardHelper.GetShardNumberFor(_record.Sharding, id);

        public (int ShardNumber, int Bucket) GetShardNumberAndBucketForIdentity(TransactionOperationContext context, string id) => ShardHelper.GetShardNumberAndBucketForIdentity(_record.Sharding, context, id, IdentityPartsSeparator);

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

            var exceptionAggregator = new ExceptionAggregator(_logger, $"Could not dispose {nameof(ShardedDatabaseContext)} {DatabaseName}");

            exceptionAggregator.Execute(() => Replication?.Dispose());

            exceptionAggregator.Execute(() => ShardExecutor?.Dispose());

            exceptionAggregator.Execute(() => AllOrchestratorNodesExecutor?.Dispose());

            exceptionAggregator.Execute(() => SubscriptionsStorage.Dispose());

            exceptionAggregator.Execute(() => _databaseShutdown.Dispose());

            exceptionAggregator.ThrowIfNeeded();
        }
    }
}
