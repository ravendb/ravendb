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

        public ShardExecutor(ServerStore store, [NotNull] DatabaseRecord databaseRecord, [NotNull] string databaseName) : base(store)
        {
            _databaseRecord = databaseRecord ?? throw new ArgumentNullException(nameof(databaseRecord));
            _databaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));

            _fullRange = databaseRecord.Sharding.Shards.Keys.ToArray();

            Conventions = new DocumentConventions
            {
                SendApplicationIdentifier = DocumentConventions.DefaultForServer.SendApplicationIdentifier,
                MaxContextSizeToKeep = DocumentConventions.DefaultForServer.MaxContextSizeToKeep,
                HttpPooledConnectionLifetime = DocumentConventions.DefaultForServer.HttpPooledConnectionLifetime,
                UseHttpCompression = store.Configuration.Sharding.ShardExecutorUseHttpCompression,
                UseHttpDecompression = store.Configuration.Sharding.ShardExecutorUseHttpDecompression,
                GlobalHttpClientTimeout = store.Configuration.Sharding.OrchestratorTimeoutInMin.AsTimeSpan
            };

            _requestExecutors = CreateExecutors();
        }

        public DocumentConventions Conventions { get; private set; }

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
                SafelyDisposeRequestExecutors(oldRequestExecutors);
            }
        }

        private Dictionary<int, RequestExecutor> CreateExecutors()
        {
            var requestExecutors = new Dictionary<int, RequestExecutor>(_databaseRecord.Sharding.Shards.Count);

            var allNodes = ServerStore.GetClusterTopology().AllNodes;

            foreach ((int shardNumber, var topology) in _databaseRecord.Sharding.Shards)
            {
                var urls = topology.AllNodes.Select(tag => allNodes[tag]).ToArray();
                requestExecutors[shardNumber] = RequestExecutor.CreateForServer(
                    urls,
                    ShardHelper.ToShardName(_databaseName, shardNumber),
                    ServerStore.Server.Certificate.Certificate,
                    Conventions);
            }

            return requestExecutors;
        }

        public override void Dispose()
        {
            SafelyDisposeRequestExecutors(_requestExecutors);
        }

        private void SafelyDisposeRequestExecutors(Dictionary<int, RequestExecutor> requestExecutors)
        {
            if (requestExecutors == null)
                return;

            foreach (var executor in requestExecutors.Values)
            {
                try
                {
                    executor.Dispose();
                }
                catch
                {
                    // ignored
                }
            }
        }
    }
}
