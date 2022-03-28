using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Executors
{
    public class ShardExecutor : AbstractExecutor
    {
        private readonly ShardedDatabaseContext _databaseContext;

        public ShardExecutor(ServerStore store, ShardedDatabaseContext databaseContext) : base(store)
        {
            _databaseContext = databaseContext;
        }

        public async Task<TResult> ExecuteSingleShardAsync<TResult>(RavenCommand<TResult> command, int shardNumber, CancellationToken token = default)
        {
            var executor = GetRequestExecutorAt(shardNumber);
            using (executor.ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
            {
                await executor.ExecuteAsync(command, ctx, token: token);
                return command.Result;
            }
        }

        protected override RequestExecutor GetRequestExecutorAt(int position) => _databaseContext.RequestExecutors[position];
        protected override Memory<int> GetAllPositions() => new Memory<int>(_databaseContext.FullRange);
        protected override void OnCertificateChange(object sender, EventArgs e)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "Handle server certificate change for ShardExecutor");
        }
    }
}
