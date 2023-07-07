using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Processors.Stats;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Stats
{
    internal class ShardedStatsHandlerProcessorForDatabaseHealthCheck : AbstractStatsHandlerProcessorForDatabaseHealthCheck<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedStatsHandlerProcessorForDatabaseHealthCheck([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async Task GetNoContentStatusAsync()
        {
            using (var token = RequestHandler.CreateHttpRequestBoundOperationToken())
            {
                await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(new GetShardedDatabaseHealthCheckOperation(RequestHandler.HttpContext.Request), token.Token);
            }
        }

        private struct GetShardedDatabaseHealthCheckOperation : IShardedOperation
        {
            public GetShardedDatabaseHealthCheckOperation(HttpRequest request)
            {
                HttpRequest = request;
            }

            public HttpRequest HttpRequest { get; }


            public RavenCommand<object> CreateCommandForShard(int shardNumber) => new DatabaseHealthCheckOperation.DatabaseHealthCheckCommand();
        }
    }
}
