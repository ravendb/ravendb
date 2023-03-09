using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Executors
{
    public class ShardExecutor : AbstractExecutor
    {
        private readonly DatabaseRecord _databaseRecord;
        private readonly string _databaseName;
        private Dictionary<int, RequestExecutor> _requestExecutors;
        private readonly int[] _fullRange;

        private readonly DocumentConventions _conventions;

        public ShardExecutor(ServerStore store, [NotNull] DatabaseRecord databaseRecord, [NotNull] string databaseName) : base(store)
        {
            _databaseRecord = databaseRecord ?? throw new ArgumentNullException(nameof(databaseRecord));
            _databaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));

            _fullRange = databaseRecord.Sharding.Shards.Keys.ToArray();

            _conventions = store.Sharding.DocumentConventionsForShard;

            _requestExecutors = CreateExecutors();
        }

        public async Task<TResult> ExecuteSingleShardAsync<TResult>(RavenCommand<TResult> command, int shardNumber, CancellationToken token = default)
        {
            var executor = GetRequestExecutorAt(shardNumber);
            using (executor.ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
            {
                await executor.ExecuteAsync(command, ctx, token: token);

#if DEBUG
                if (command.Result.ContainsBlittableObject())
                {
                    throw new InvalidOperationException("The return type is unmanaged, please use the overload with the context");
                }
#endif

                return command.Result;
            }
        }

        public async Task<TResult> ExecuteSingleShardAsync<TResult>(JsonOperationContext context, RavenCommand<TResult> command, int shardNumber, CancellationToken token = default)
        {
            var executor = GetRequestExecutorAt(shardNumber);
            await executor.ExecuteAsync(command, context, token: token);
            return command.Result;
        }

        public async Task ExecuteSingleShardAsync(RavenCommand command, int shardNumber, CancellationToken token = default)
        {
            var executor = GetRequestExecutorAt(shardNumber);
            using (executor.ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
            {
                await executor.ExecuteAsync(command, ctx, token: token);
            }
        }

        public override RequestExecutor GetRequestExecutorAt(int position) => _requestExecutors[position];

        protected override Memory<int> GetAllPositions() => new Memory<int>(_fullRange);

        protected override void OnCertificateChange(object sender, EventArgs e)
        {
            var oldRequestExecutors = _requestExecutors;

            try
            {
                _requestExecutors = CreateExecutors();
            }
            finally
            {
                SafelyDisposeExecutors(oldRequestExecutors.Values);
            }
        }

        private Dictionary<int, RequestExecutor> CreateExecutors()
        {
            var requestExecutors = new Dictionary<int, RequestExecutor>(_databaseRecord.Sharding.Shards.Count);
            
            PublishedUrls published;
            ClusterTopology clusterTopology;
            
            using (ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                clusterTopology = ServerStore.GetClusterTopology(context);
                published = PublishedUrls.Read(context);
            }

            foreach ((int shardNumber, var topology) in _databaseRecord.Sharding.Shards)
            {
                var urls = topology.AllNodes.Select(tag => published.SelectUrl(tag, clusterTopology)).ToArray();

                requestExecutors[shardNumber] = RequestExecutor.CreateForShard(
                    urls,
                    ShardHelper.ToShardName(_databaseName, shardNumber),
                    ServerStore.Server.Certificate.Certificate,
                    _conventions);
            }

            return requestExecutors;
        }

        public override void Dispose() => SafelyDisposeExecutors(_requestExecutors.Values);
    }
}
