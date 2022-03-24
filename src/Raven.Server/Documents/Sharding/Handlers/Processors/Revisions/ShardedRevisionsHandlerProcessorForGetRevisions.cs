using System.Net;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Revisions;
using Raven.Server.Documents.Sharding.Commands;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Revisions
{
    internal class ShardedRevisionsHandlerProcessorForGetRevisions : AbstractRevisionsHandlerProcessorForGetRevisions<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedRevisionsHandlerProcessorForGetRevisions([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override async ValueTask GetRevisionByChangeVectorAsync(Microsoft.Extensions.Primitives.StringValues changeVectors, bool metadataOnly, CancellationToken token)
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var cmd = new ShardedGetRevisionsByChangeVectorsOperation(changeVectors.ToArray(), metadataOnly, context);
                
                var res = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(cmd, token);

                if (res == null && changeVectors.Count == 1)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return;
                }

                var blittable = RequestHandler.GetBoolValueQueryString("blittable", required: false) ?? false;
                
                if (blittable)
                {
                    WriteRevisionsBlittable(context, res, out long numberOfResults, out long totalDocumentsSizeInBytes);
                }
                else
                {
                    await WriteRevisionsResult(context, res);
                }
            }
        }

        protected override async ValueTask GetRevisionsAsync(bool metadataOnly, CancellationToken token)
        {
            var id = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
            var before = RequestHandler.GetDateTimeQueryString("before", required: false);
            var start = RequestHandler.GetStart();
            var pageSize = RequestHandler.GetPageSize();

            GetRawRevisionsCommand cmd;
            if (before.HasValue)
            {
                cmd = new GetRawRevisionsCommand(RequestHandler, id, before.Value);
            }
            else
            {
                cmd = new GetRawRevisionsCommand(RequestHandler, id, start, pageSize, metadataOnly);
            }

            int shardIndex;
            using (RequestHandler.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                shardIndex = RequestHandler.DatabaseContext.GetShardNumber(context, id);
            }
            
            //cmd writes the response to stream as is
            await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(cmd, shardIndex, token);
        }

        protected override void CheckNotModified(string actualEtag)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Minor, "Implement AddPagingPerformanceHint");
        }

        private async ValueTask WriteRevisionsResult(JsonOperationContext context, RevisionsResult revisions)
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WriteArray(nameof(revisions.Results), revisions.Results);

                writer.WriteComma();

                writer.WritePropertyName(nameof(revisions.TotalResults));
                writer.WriteInteger(revisions.TotalResults);
                writer.WriteEndObject();
            }
        }

        protected override void AddPagingPerformanceHint(PagingOperationType operation, string action, string details, long numberOfResults, int pageSize, long duration,
            long totalDocumentsSizeInBytes)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Pawel, DevelopmentHelper.Severity.Minor, "Implement AddPagingPerformanceHint");
        }
    }
}
