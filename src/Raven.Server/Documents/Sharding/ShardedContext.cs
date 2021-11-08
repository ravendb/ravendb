using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Sharding.Commands;
using Raven.Server.Documents.Sharding.Documents;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Sharding
{
    public class ShardedContext : IDisposable
    {
        public const int NumberOfShards = 1024 * 1024;
        public ShardedSubscriptionContext ShardedSubscriptionStorage { get; }
        private readonly CancellationTokenSource _databaseShutdown = new CancellationTokenSource();
        public CancellationToken DatabaseShutdown => _databaseShutdown.Token;

        private readonly DatabaseRecord _record;
        public RequestExecutor[] RequestExecutors;
        private readonly long _lastClientConfigurationIndex;
        private readonly Logger _logger;
        public ShardedContext(ServerStore server, DatabaseRecord record)
        {
            //TODO: reduce the record to the needed fields
            _record = record;
            _lastClientConfigurationIndex = server.LastClientConfigurationIndex;
            ShardedSubscriptionStorage = new ShardedSubscriptionContext(this, server);
            _logger = LoggingSource.Instance.GetLogger<ShardedContext>(DatabaseName);

            RequestExecutors = new RequestExecutor[record.Shards.Length];
            for (int i = 0; i < record.Shards.Length; i++)
            {
                var allNodes = server.GetClusterTopology().AllNodes;
                var urls = record.Shards[i].AllNodes.Select(tag => allNodes[tag]).ToArray();
                // TODO: pool request executors?
                RequestExecutors[i] = RequestExecutor.Create(
                    urls,
                    GetShardedDatabaseName(i),
                    server.Server.Certificate.Certificate,
                    new DocumentConventions());
            }
        }

        public string DatabaseName => _record.DatabaseName;

        public char IdentitySeparator => _record.Client?.IdentityPartsSeparator ?? Constants.Identities.DefaultSeparator;

        public bool Encrypted => _record.Encrypted;

        public int Count => _record.Shards.Length;

        public DatabaseTopology[] ShardsTopology => _record.Shards;

        /// <summary>
        /// The shard id is a hash of the document id, lower case, reduced to
        /// 20 bits. This gives us 0 .. 1M range of shard ids and means that assuming
        /// perfect distribution of data, each shard is going to have about 1MB of data
        /// per TB of overall db size. That means that even for *very* large databases, the
        /// size of the shard is still going to be manageable.
        /// </summary>
        public unsafe int GetShardId(TransactionOperationContext context, string key)
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

        private unsafe static void AdjustAfterSeparator(byte expected, ref byte* ptr, ref int len)
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

        public async Task<string> GetLastDocumentChangeVectorForCollection(string subCollection)
        {
            var disposables = new List<IDisposable>();
            var cvs = new List<string>();
            try
            {
                var cmds = new List<LastChangeVectorForCollectionCommand>();
                var tasks = new List<Task>();
                foreach (var re in RequestExecutors)
                {
                    disposables.Add(re.ContextPool.AllocateOperationContext(out JsonOperationContext ctx));

                    var cmd = new LastChangeVectorForCollectionCommand(subCollection);
                    cmds.Add(cmd);
                    tasks.Add(re.ExecuteAsync(cmd, ctx));

                }
                await Task.WhenAll(tasks);
                foreach (var cmd in cmds)
                {
                    //TODO: egor throw on failed request
                    cvs.Add(cmd.Result.LastChangeVector);
                }
            }
            finally
            {
                disposables.ForEach(x => x.Dispose());
            }

            return ChangeVectorUtils.MergeVectors(cvs);
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

            ShardedSubscriptionStorage?.HandleDatabaseRecordChange(record);
        }

        public void Dispose()
        {
            _databaseShutdown.Cancel();

            try
            {
                ShardedSubscriptionStorage.Dispose();
            }
            catch
            {
                // ignored
            }

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

            //TODO: egor check if need to dispose tcp connection of subscription

            _databaseShutdown.Dispose();
        }
    }
}
