using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Client;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
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
using Voron;

namespace Raven.Server.Documents.Sharding
{
    public partial class ShardedDatabaseContext : IDisposable
    {
        public const int NumberOfShards = 1024 * 1024;
        private readonly CancellationTokenSource _databaseShutdown = new CancellationTokenSource();
        public CancellationToken DatabaseShutdown => _databaseShutdown.Token;

        private readonly ServerStore _serverStore;
        private readonly long _lastClientConfigurationIndex;

        private DatabaseRecord _record;
        public RequestExecutor[] RequestExecutors;
        public QueryMetadataCache QueryMetadataCache = new();
        private readonly Logger _logger;

        public readonly ShardExecutor ShardExecutor;
        public DatabaseRecord DatabaseRecord => _record;

        public RavenConfiguration Configuration { get; internal set; }


        public int[] FullRange;

        public ShardedDatabaseContext(ServerStore serverStore, DatabaseRecord record)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "reduce the record to the needed fields");
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "Need to refresh all this in case we will add/remove new shard");

            _serverStore = serverStore;
            _record = record;

            UpdateConfiguration(record.Settings);

            Indexes = new ShardedIndexesCache(this, serverStore);
            _logger = LoggingSource.Instance.GetLogger<ShardedDatabaseContext>(DatabaseName);
            _lastClientConfigurationIndex = serverStore.LastClientConfigurationIndex;

            RequestExecutors = new RequestExecutor[record.Shards.Length];
            for (int i = 0; i < record.Shards.Length; i++)
            {
                var allNodes = serverStore.GetClusterTopology().AllNodes;
                var urls = record.Shards[i].AllNodes.Select(tag => allNodes[tag]).ToArray();
                // TODO: pool request executors?
                RequestExecutors[i] = RequestExecutor.Create(
                    urls,
                    ShardHelper.ToShardName(DatabaseName, i),
                    serverStore.Server.Certificate.Certificate,
                    new DocumentConventions());
            }

            FullRange = Enumerable.Range(0, _record.Shards.Length).ToArray();
            ShardExecutor = new ShardExecutor(this);
            Streaming = new ShardedStreaming();
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

        public char IdentitySeparator => _record.Client?.IdentityPartsSeparator ?? Constants.Identities.DefaultSeparator;

        public bool Encrypted => _record.Encrypted;

        public int ShardCount => _record.Shards.Length;

        public DatabaseTopology[] ShardsTopology => _record.Shards;

        /// <summary>
        /// The shard id is a hash of the document id, lower case, reduced to
        /// 20 bits. This gives us 0 .. 1M range of shard ids and means that assuming
        /// perfect distribution of data, each shard is going to have about 1MB of data
        /// per TB of overall db size. That means that even for *very* large databases, the
        /// size of the shard is still going to be manageable.
        /// </summary>
        public static int GetShardId<TTransaction>(TransactionOperationContext<TTransaction> context, string key) where TTransaction : RavenTransaction
        {
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, key, out var lowerId, out _))
            {
                return GetShardId(lowerId);
            }
        }

        private static unsafe int GetShardId(Slice lowerId)
        {
            byte* buffer = lowerId.Content.Ptr;
            int size = lowerId.Size;

            AdjustAfterSeparator((byte)'$', ref buffer, ref size);

            if (size == 0)
                throw new ArgumentException("Key '" + lowerId + "', has a shard id length of 0");

            var hash = Hashing.XXHash64.Calculate(buffer, (ulong)size);
            return (int)(hash % NumberOfShards);
        }

        private static unsafe void AdjustAfterSeparator(byte expected, ref byte* ptr, ref int len)
        {
            for (int i = len - 1; i > 0; i--)
            {
                if (ptr[i] != expected)
                    continue;
                ptr += i + 1;
                len -= i + 1;
                break;
            }
        }

        public int GetShardIndex(Slice lowerId)
        {
            var shardId = GetShardId(lowerId);
            return GetShardIndex(shardId);
        }

        private static int GetShardIndex(List<DatabaseRecord.ShardRangeAssignment> shardAllocations, int shardId)
        {
            for (int i = 0; i < shardAllocations.Count - 1; i++)
            {
                if (shardId < shardAllocations[i + 1].RangeStart)
                    return shardAllocations[i].Shard;
            }

            return shardAllocations[^1].Shard;
        }
        public int GetShardIndex(int shardId) => GetShardIndex(_record.ShardAllocations, shardId);
        public int GetShardIndex(TransactionOperationContext context, string key) => GetShardIndex(context, _record.ShardAllocations, key);

        public static int GetShardIndex<TTransaction>(TransactionOperationContext<TTransaction> context, List<DatabaseRecord.ShardRangeAssignment> shardAllocations, string key)
            where TTransaction : RavenTransaction
        {
            var shardId = GetShardId(context, key);
            return GetShardIndex(shardAllocations, shardId);
        }

        public bool HasTopologyChanged(long etag)
        {
            // TODO fix this
            return _record.Topology?.Stamp?.Index > etag;
        }

        public bool HasClientConfigurationChanged(long clientConfigurationEtag)
        {
            var lastClientConfigurationIndex = _record.Client?.Etag ?? 0;
            var actual = Hashing.Combine(lastClientConfigurationIndex, _lastClientConfigurationIndex);
            return actual > clientConfigurationEtag;
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
