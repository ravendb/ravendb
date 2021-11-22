using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Client;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Logging;

namespace Raven.Server.Documents.Sharding
{
    public unsafe class ShardedContext : IDisposable
    {
        public const int NumberOfShards = 1024 * 1024;
        public CancellationToken DatabaseShutdown => _databaseShutdown.Token;
        public RequestExecutor[] RequestExecutors;
        public int Count => _record.Shards.Length;
        public DatabaseTopology[] ShardsTopology => _record.Shards;

        private readonly CancellationTokenSource _databaseShutdown = new CancellationTokenSource();
        private readonly Logger _logger;
        private readonly DatabaseRecord _record;
        private readonly long _lastClientConfigurationIndex;

        public ShardedContext(ServerStore server, DatabaseRecord record)
        {
            //TODO: reduce the record to the needed fields
            _record = record;
            _lastClientConfigurationIndex = server.LastClientConfigurationIndex;
            _logger = LoggingSource.Instance.GetLogger<ShardedContext>(DatabaseName);

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
                len -= i - 1;
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

        public string GetShardedDatabaseName(int index = 0)
        {
            if (index >= _record.Shards.Length)
                throw new InvalidOperationException($"Requested shard '{index}' of database '{DatabaseName}' but shards length '{_record.Shards.Length}'.");

            return _record.DatabaseName + "$" + index;
        }

        public List<string> GetShardedDatabaseNames()
        {
            var list = new List<string>();
            for (int i = 0; i < Count; i++)
            {
                list.Add(GetShardedDatabaseName(i));
            }
            return list;
        }

        private long _lastValueChangeIndex;

        public long LastValueChangeIndex
        {
            get => Volatile.Read(ref _lastValueChangeIndex);
            private set => _lastValueChangeIndex = value; // we write this always under lock
        }

        private bool CanSkipValueChange(string database, long index)
        {
            if (LastValueChangeIndex > index)
            {
                // index and LastDatabaseRecordIndex could have equal values when we transit from/to passive and want to update the tasks.
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Skipping value change for index {index} (current {LastValueChangeIndex}) for {database} because it was already precessed.");
                return true;
            }

            return false;
        }

        public void NotifyFeaturesAboutValueChange(RawDatabaseRecord record, long index)
        {
            if (CanSkipValueChange(record.DatabaseName, index))
                return;
        }

        public void Dispose()
        {
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

            _databaseShutdown.Dispose();
        }
    }
}
