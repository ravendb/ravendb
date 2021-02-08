using System;
using System.Linq;
using System.Threading;
using Raven.Client;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Queries;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding
{
    public unsafe partial class ShardedContext
    {
        public const int NumberOfShards = 1024 * 1024;

        private readonly ServerStore _server;
        public QueryMetadataCache QueryMetadataCache = new QueryMetadataCache();

        private DatabaseRecord _record;
        public RequestExecutor[] RequestExecutors;
        private readonly long _lastClientConfigurationIndex;

        private ShardExecutor _shardExecutor;
        public ShardExecutor ShardExecutor => _shardExecutor;
        public DatabaseRecord DatabaseRecord => _record;

        public int[] FullRange;

        public ShardedContext(ServerStore server, RawDatabaseRecord record)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "reduce the record to the needed fields");
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "Need to refresh all this in case we will add/remove new shard");

            _server = server;
            _record = record;
            _lastClientConfigurationIndex = server.LastClientConfigurationIndex;

            RequestExecutors = new RequestExecutor[record.Shards.Length];
            for (int i = 0; i < record.Shards.Length; i++)
            {
                var allNodes = server.GetClusterTopology().AllNodes;
                var urls = record.Shards[i].AllNodes.Select(tag => allNodes[tag]).ToArray();
                // TODO: pool request executors?
                RequestExecutors[i] = RequestExecutor.Create(
                    urls,
                    record.DatabaseName + "$" + i,
                    server.Server.Certificate.Certificate,
                    new DocumentConventions());
            }

            FullRange = Enumerable.Range(0, _record.Shards.Length).ToArray();
            _shardExecutor = new ShardExecutor(this);
            Streaming = new ShardedStreaming(this);
        }

        public IDisposable AllocateContext(out JsonOperationContext context) => _server.ContextPool.AllocateOperationContext(out context);

        public void UpdateDatabaseRecord(DatabaseRecord record)
        {
            Interlocked.Exchange(ref _record, record);
        }

        public string DatabaseName => _record.DatabaseName;

        public int NumberOfShardNodes => _record.Shards.Length;
        
        public char IdentitySeparator => _record.Client?.IdentityPartsSeparator ?? Constants.Identities.DefaultSeparator;

        public bool Encrypted => _record.Encrypted;

        public int ShardCount => _record.Shards.Length;

        /// <summary>
        /// The shard id is a hash of the document id, lower case, reduced to
        /// 20 bits. This gives us 0 .. 1M range of shard ids and means that assuming
        /// perfect distribution of data, each shard is going to have about 1MB of data
        /// per TB of overall db size. That means that even for *very* large databases, the
        /// size of the shard is still going to be manageable.
        /// </summary>
        public int GetShardId(TransactionOperationContext context, string key)
        {
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, key, out var lowerId, out _))
            {
                byte* buffer = lowerId.Content.Ptr;
                int size = lowerId.Size;

                AdjustAfterSeparator((byte)'$', ref buffer, ref size);

                if (size == 0)
                    throw new ArgumentException("Key '" + key + "', has a shard id length of 0");

                var hash = Hashing.XXHash64.Calculate(buffer, (ulong)size);
                return (int)(hash % NumberOfShards);
            }
        }

        private static void AdjustAfterSeparator(byte expected, ref byte* ptr, ref int len)
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

        public int GetShardIndex(int shardId)
        {
            for (int i = 0; i < _record.ShardAllocations.Count - 1; i++)
            {
                if (shardId < _record.ShardAllocations[i + 1].RangeStart)
                    return _record.ShardAllocations[i].Shard;
            }

            return _record.ShardAllocations[^1].Shard;
        }
        
        public int GetShardIndex(TransactionOperationContext context, string key)
        {
            var shardId = GetShardId(context, key);
            for (int i = 0; i < _record.ShardAllocations.Count - 1; i++)
            {
                if (shardId < _record.ShardAllocations[i + 1].RangeStart)
                    return _record.ShardAllocations[i].Shard;
            }

            return _record.ShardAllocations[^1].Shard;
        }

        public int GetShardIndex(ByteStringContext context, LazyStringValue key)
        {
            var shardId = ShardHelper.GetBucket(context, key);
            for (int i = 0; i < _record.ShardAllocations.Count - 1; i++)
            {
                if (shardId < _record.ShardAllocations[i + 1].RangeStart)
                    return _record.ShardAllocations[i].Shard;
            }

            return _record.ShardAllocations[^1].Shard;
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
    }
}
