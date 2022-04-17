using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Client;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes.Analysis;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.ShardedTcpHandlers;
using Raven.Server.Documents.Sharding.Executors;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Utils;
using Voron;

namespace Raven.Server.Documents.Sharding
{
    public partial class ShardedDatabaseContext : IDisposable
    {
        private readonly CancellationTokenSource _databaseShutdown = new CancellationTokenSource();
        public CancellationToken DatabaseShutdown => _databaseShutdown.Token;

        private readonly ServerStore _serverStore;

        private DatabaseRecord _record;
        public QueryMetadataCache QueryMetadataCache = new();
        private readonly Logger _logger;

        public readonly ShardExecutor ShardExecutor;
        public readonly AllNodesExecutor AllNodesExecutor;
        public DatabaseRecord DatabaseRecord => _record;

        public RavenConfiguration Configuration { get; internal set; }

        public readonly SystemTime Time;

        public RachisLogIndexNotifications RachisLogIndexNotifications;

        public ShardedDatabaseContext(ServerStore serverStore, DatabaseRecord record)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "reduce the record to the needed fields");
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "Need to refresh all this in case we will add/remove new shard");

            _serverStore = serverStore;
            _record = record;
            _logger = LoggingSource.Instance.GetLogger<ShardedDatabaseContext>(DatabaseName);

            Time = serverStore.Server.Time;

            UpdateConfiguration(record.Settings);

            Indexes = new ShardedIndexesContext(this, serverStore);

            ShardExecutor = new ShardExecutor(_serverStore, this);
            AllNodesExecutor = new AllNodesExecutor(_serverStore, DatabaseName);

            Streaming = new ShardedStreaming();
            Cluster = new ShardedCluster(this);
            RachisLogIndexNotifications = new RachisLogIndexNotifications(_databaseShutdown.Token);
        }

        public IDisposable AllocateContext(out JsonOperationContext context) => _serverStore.ContextPool.AllocateOperationContext(out context);


        public void UpdateDatabaseRecord(RawDatabaseRecord record, long index)
        {
            UpdateConfiguration(record.Settings);

            Indexes.Update(record, index);

            Interlocked.Exchange(ref _record, record);

            RachisLogIndexNotifications.NotifyListenersAbout(index, e: null);
        }

        public string DatabaseName => _record.DatabaseName;

        public int NumberOfShardNodes => _record.Sharding.Shards.Length;

        public char IdentityPartsSeparator => _record.Client?.IdentityPartsSeparator ?? Constants.Identities.DefaultSeparator;

        public bool Encrypted => _record.Encrypted;

        public int ShardCount => _record.Sharding.Shards.Length;

        public DatabaseTopology[] ShardsTopology => _record.Sharding.Shards;

        public int GetShardNumberFor(string id) => ShardHelper.GetShardNumberFor(_record.Sharding, id);
        public int GetShardNumberFor(Slice id) => ShardHelper.GetShardNumberFor(_record.Sharding, id);
        
        public int GetShardNumberFor(ByteStringContext allocator, LazyStringValue id) => ShardHelper.GetShardNumberFor(_record.Sharding, allocator, id);

        public int GetShardNumber(TransactionOperationContext context, string id)
        {
            return ShardHelper.GetShardNumberFor(_record.Sharding, context, id);
        }

        public bool HasTopologyChanged(long etag)
        {
            // TODO fix this
            return _record.Topology?.Stamp?.Index > etag;
        }

        private void UpdateConfiguration(Dictionary<string, string> settings)
        {
            Configuration = DatabasesLandlord.CreateDatabaseConfiguration(_serverStore, DatabaseName, settings);
        }

        public void Dispose()
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "needs an ExceptionAggregator like DocumentDatabase");

            if (_logger.IsInfoEnabled)
                _logger.Info($"Disposing {nameof(ShardedDatabaseContext)} of {DatabaseName}.");

            _databaseShutdown.Cancel();

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
                AllNodesExecutor.Dispose();
            }
            catch
            {
                // ignored
            }

            foreach (var connection in ShardedSubscriptionConnection.Connections)
            {
                connection.Value.Dispose();
            }

            _databaseShutdown.Dispose();
        }
    }
}
