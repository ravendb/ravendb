using System;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;

namespace Raven.Server.Documents.Sharding
{
    public unsafe class ShardedContext
    {
        public const int NumberOfShards = 1024 * 1024;

        private readonly DatabaseRecord _record;
        public RequestExecutor[] RequestExecutors;
        private long _lastClientConfigurationIndex;

        public ShardedContext(ServerStore server, DatabaseRecord record)
        {
            //TODO: reduce the record to the needed fields
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
        }

        public string DatabaseName => _record.DatabaseName;

        public char IdentitySeparator => _record.Client?.IdentityPartsSeparator ?? Constants.Identities.DefaultSeparator;

        public bool Encrypted => _record.Encrypted;

        public int ShardCount => _record.Shards.Length;

        public int GetShardIndex(int shardId)
        {
            for (int i = 0; i < _record.ShardAllocations.Count - 1; i++)
            {
                if (shardId < _record.ShardAllocations[i + 1].RangeStart)
                    return _record.ShardAllocations[i].Shard;
            }

            return _record.ShardAllocations[_record.ShardAllocations.Count - 1].Shard;
        }

        public int GetShardIndex(TransactionOperationContext context, string key)
        {
            var shardId =ShardHelper.GetShardId(context, key);
            for (int i = 0; i < _record.ShardAllocations.Count - 1; i++)
            {
                if (shardId < _record.ShardAllocations[i + 1].RangeStart)
                    return _record.ShardAllocations[i].Shard;
            }

            return _record.ShardAllocations[_record.ShardAllocations.Count - 1].Shard;
        }

        public bool HasTopologyChanged(long etag)
        {
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
