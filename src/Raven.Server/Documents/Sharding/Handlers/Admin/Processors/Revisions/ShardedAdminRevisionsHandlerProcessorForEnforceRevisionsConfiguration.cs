using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Admin.Processors.Revisions;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Admin.Processors.Revisions
{
    internal class ShardedAdminRevisionsHandlerProcessorForEnforceRevisionsConfiguration : AbstractAdminRevisionsHandlerProcessorForEnforceRevisionsConfiguration<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedAdminRevisionsHandlerProcessorForEnforceRevisionsConfiguration([NotNull] ShardedDatabaseRequestHandler requestHandler) 
            : base(requestHandler)
        {
        }

        protected override async ValueTask AddOperationAsync(long operationId, OperationCancelToken token)
        {
            //send negative operationId to avoid collisions
            await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(new ShardedEnforceRevisionsConfigurationOperation(HttpContext, operationId));
        }

        private readonly struct ShardedEnforceRevisionsConfigurationOperation : IShardedOperation<OperationIdResult>
        {
            private readonly HttpContext _httpContext;
            private readonly long _operationId;

            public ShardedEnforceRevisionsConfigurationOperation(HttpContext httpContext, long operationId)
            {
                _httpContext = httpContext;
                _operationId = operationId;
            }

            public HttpRequest HttpRequest => _httpContext.Request;
            public OperationIdResult Combine(Memory<OperationIdResult> results)
            {
                return new OperationIdResult();
            }

            public RavenCommand<OperationIdResult> CreateCommandForShard(int shardNumber) => new EnforceRevisionsConfigurationOperation.EnforceRevisionsConfigurationCommand(_operationId);
        }
    }
}
