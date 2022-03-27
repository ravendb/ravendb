using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Client;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.ShardedTcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding
{
    public partial class ShardedDatabaseContext : IDisposable
    {
        private readonly CancellationTokenSource _databaseShutdown = new CancellationTokenSource();
        public CancellationToken DatabaseShutdown => _databaseShutdown.Token;

        private readonly ServerStore _serverStore;

        private DatabaseRecord _record;
        public RequestExecutor[] RequestExecutors;
        public QueryMetadataCache QueryMetadataCache = new();
        private readonly Logger _logger;

        public readonly ShardExecutor ShardExecutor;
        public DatabaseRecord DatabaseRecord => _record;

        public RavenConfiguration Configuration { get; internal set; }

        public readonly SystemTime Time = new SystemTime();

        public int[] FullRange;

        public ShardedDatabaseContext(ServerStore serverStore, DatabaseRecord record)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "reduce the record to the needed fields");
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "Need to refresh all this in case we will add/remove new shard");

            _serverStore = serverStore;
            _record = record;

            UpdateConfiguration(record.Settings);

            Indexes = new ShardedIndexesContext(this, serverStore);
            _logger = LoggingSource.Instance.GetLogger<ShardedDatabaseContext>(DatabaseName);

            RequestExecutors = new RequestExecutor[record.Shards.Length];
            for (int i = 0; i < record.Shards.Length; i++)
            {
                var allNodes = serverStore.GetClusterTopology().AllNodes;
                var urls = record.Shards[i].AllNodes.Select(tag => allNodes[tag]).ToArray();
                // TODO: pool request executors?
                RequestExecutors[i] = RequestExecutor.CreateForServer(
                    urls,
                    ShardHelper.ToShardName(DatabaseName, i),
                    serverStore.Server.Certificate.Certificate,
                    DocumentConventions.DefaultForServer);
            }

            FullRange = Enumerable.Range(0, _record.Shards.Length).ToArray();
            ShardExecutor = new ShardExecutor(this);
            Streaming = new ShardedStreaming();
            Cluster = new ShardedCluster(this);
        }

        public IDisposable AllocateContext(out JsonOperationContext context) => _serverStore.ContextPool.AllocateOperationContext(out context);

        public void UpdateDatabaseRecord(RawDatabaseRecord record)
        {
            UpdateConfiguration(record.Settings);

            Indexes.Update(record);

            Interlocked.Exchange(ref _record, record);
        }

        public string DatabaseName => _record.DatabaseName;

        public int NumberOfShardNodes => _record.Shards.Length;

        public char IdentityPartsSeparator => _record.Client?.IdentityPartsSeparator ?? Constants.Identities.DefaultSeparator;

        public bool Encrypted => _record.Encrypted;

        public int ShardCount => _record.Shards.Length;

        public DatabaseTopology[] ShardsTopology => _record.Shards;

        public int GetShardNumber(int shardBucket) => ShardHelper.GetShardNumber(_record.ShardBucketRanges, shardBucket);
        
        public int GetShardNumber(TransactionOperationContext context, string id)
        {
            var bucket = ShardHelper.GetBucket(context, id);

            return ShardHelper.GetShardNumber(_record.ShardBucketRanges, bucket);
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
            if (_logger.IsInfoEnabled)
                _logger.Info($"Disposing {nameof(ShardedDatabaseContext)} of {DatabaseName}.");

            _databaseShutdown.Cancel();

            foreach (var re in RequestExecutors)
            {
                try
                {
                    re.Dispose();
                }
                catch
                {
                    // ignored
                }
            }

            foreach (var connection in ShardedSubscriptionConnection.Connections)
            {
                connection.Value.Dispose();
            }

            _databaseShutdown.Dispose();
        }
    }
}
