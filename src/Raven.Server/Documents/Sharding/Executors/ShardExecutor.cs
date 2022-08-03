using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Executors
{
    public class ShardExecutor : AbstractExecutor
    {
        private readonly RequestExecutor[] _requestExecutors;
        private readonly int[] _fullRange;

        public ShardExecutor(ServerStore store, ShardedDatabaseContext databaseContext) : base(store)
        {
            var record = databaseContext.DatabaseRecord;
            _fullRange = Enumerable.Range(0, record.Sharding.Shards.Length).ToArray();

            _requestExecutors = new RequestExecutor[record.Sharding.Shards.Length];
            for (int i = 0; i < record.Sharding.Shards.Length; i++)
            {
                var allNodes = store.GetClusterTopology().AllNodes;
                var urls = record.Sharding.Shards[i].AllNodes.Select(tag => allNodes[tag]).ToArray();
                _requestExecutors[i] = RequestExecutor.CreateForServer(
                    urls,
                    ShardHelper.ToShardName(databaseContext.DatabaseName, i),
                    store.Server.Certificate.Certificate,
                    DocumentConventions.DefaultForServer);
            }
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
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "RavenDB-19063 Handle server certificate change for ShardExecutor");
        }

        public override void Dispose()
        {
            foreach (var executor in _requestExecutors)
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
