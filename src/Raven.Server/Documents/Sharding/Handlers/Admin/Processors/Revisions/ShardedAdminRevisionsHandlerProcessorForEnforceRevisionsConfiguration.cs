using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.Handlers.Admin.Processors.Revisions;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Admin.Processors.Revisions
{
    internal class ShardedAdminRevisionsHandlerProcessorForEnforceRevisionsConfiguration : AbstractAdminRevisionsHandlerProcessorForEnforceRevisionsConfiguration<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedAdminRevisionsHandlerProcessorForEnforceRevisionsConfiguration([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override async ValueTask<OperationIdResults> AddOperation(long operationId, OperationCancelToken token)
        {
            return await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(new ShardedEnforceRevisionsConfigurationOperation(HttpContext));
        }

        protected override OperationCancelToken CreateTimeLimitedOperationToken()
        {
            return RequestHandler.CreateOperationToken(RequestHandler.DatabaseContext.Configuration.Databases.OperationTimeout.AsTimeSpan);
        }
    }

    internal readonly struct ShardedEnforceRevisionsConfigurationOperation : IShardedOperation<OperationIdResults>
    {
        private readonly HttpContext _httpContext;

        public ShardedEnforceRevisionsConfigurationOperation(HttpContext httpContext)
        {
            _httpContext = httpContext;
        }

        public HttpRequest HttpRequest => _httpContext.Request;

        public OperationIdResults Combine(Memory<OperationIdResults> results)
        {
            var span = results.Span;
            var combined = new OperationIdResults() { Results = new List<OperationIdResult>(span.Length) };
            foreach (var operationResult in span)
            {
                combined.Results.Add(operationResult.Results[0]);
            }

            return combined;
        }

        public RavenCommand<OperationIdResults> CreateCommandForShard(int shard) => new EnforceRevisionsConfigurationCommand();
    }

    internal class EnforceRevisionsConfigurationCommand : RavenCommand<OperationIdResults>
    {
        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post
            };

            var pathBuilder = new StringBuilder(node.Url)
                .Append("/databases/")
                .Append(node.Database)
                .Append("/admin/revisions/config/enforce");

            url = pathBuilder.ToString();
            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            Result = JsonDeserializationClient.OperationIdResults(response);
        }

        public override bool IsReadRequest => false;
    }
}
