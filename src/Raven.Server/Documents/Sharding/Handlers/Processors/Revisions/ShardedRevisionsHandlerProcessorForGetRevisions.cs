using System;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Json;
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

        protected override async ValueTask GetRevisionByChangeVectorAsync(TransactionOperationContext context, Microsoft.Extensions.Primitives.StringValues changeVectors, bool metadataOnly, CancellationToken token)
        {
            var cmd = new ShardedGetRevisionsByChangeVectorsOperation(HttpContext, changeVectors.ToArray(), metadataOnly, context);

            var res = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(cmd, token);

            if (res == null && changeVectors.Count == 1)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return;
            }

            string etag = null; //TODO
            if (NotModified(etag))
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                return;
            }

            if (string.IsNullOrEmpty(etag) == false)
                HttpContext.Response.Headers[Constants.Headers.Etag] = "\"" + etag + "\"";

            var blittable = RequestHandler.GetBoolValueQueryString("blittable", required: false) ?? false;

            if (blittable)
            {
                WriteRevisionsBlittable(context, res, out long numberOfResults, out long totalDocumentsSizeInBytes);
            }
            else
            {
                await WriteRevisionsResultAsync(context, res, totalResult: null);
            }

            AddPagingPerformanceHint(PagingOperationType.Revisions, "", "", 0, 0, 0, 0);
        }

        protected override async ValueTask GetRevisionsAsync(TransactionOperationContext context, string id, DateTime? before, int start, int pageSize, bool metadataOnly, CancellationToken token)
        {
            GetRevisionsCommand cmd;
            if (before.HasValue)
            {
                cmd = new GetRevisionsCommand(id, before.Value);
            }
            else
            {
                cmd = new GetRevisionsCommand(id, start, pageSize, metadataOnly);
            }

            string etag = null; //TODO
            if (NotModified(etag))
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                return;
            }

            if (string.IsNullOrEmpty(etag) == false)
                HttpContext.Response.Headers[Constants.Headers.Etag] = "\"" + etag + "\"";

            int shardNumber = RequestHandler.DatabaseContext.GetShardNumber(context, id);
            var result = await RequestHandler.ExecuteSingleShardAsync(context, cmd, shardNumber, token);
            var array = result.Results.Items.Select(x => ((BlittableJsonReaderObject)x).Clone(context)).ToArray();

            await WriteRevisionsResultAsync(context, array, result.TotalResults);

            AddPagingPerformanceHint(PagingOperationType.Revisions, "", "", 0, 0, 0, 0);
        }

        protected override bool NotModified(string actualEtag)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "Need to figure out the best way to combine ETags and send not modified");
            return false;
        }

        private async ValueTask WriteRevisionsResultAsync(JsonOperationContext context, BlittableJsonReaderObject[] array, long? totalResult)
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WriteArray(nameof(BlittableArrayResult.Results), array);
                if (totalResult.HasValue)
                {
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(BlittableArrayResult.TotalResults));
                    writer.WriteInteger(totalResult.Value);
                }
                writer.WriteEndObject();
            }
        }

        protected override void AddPagingPerformanceHint(PagingOperationType operation, string action, string details, long numberOfResults, int pageSize, long duration,
            long totalDocumentsSizeInBytes)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Pawel, DevelopmentHelper.Severity.Minor, "Implement AddPagingPerformanceHint, collect and pass real params");
        }
    }
}
