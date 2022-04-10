using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Commands;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal abstract class AbstractReplicationHandlerProcessorForGetConflicts<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        protected AbstractReplicationHandlerProcessorForGetConflicts([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool)
            : base(requestHandler, contextPool)
        {
        }

        protected abstract Task<GetConflictsResultByEtag> GetConflictsByEtagAsync(TOperationContext context, long etag);

        protected abstract Task GetConflictsForDocumentAsync(TOperationContext context, string documentId);

        public override async ValueTask ExecuteAsync()
        {
            var docId = RequestHandler.GetStringQueryString("docId", required: false);
            var etag = RequestHandler.GetLongQueryString("etag", required: false) ?? 0;

            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            {
                if (string.IsNullOrWhiteSpace(docId))
                {
                    var result = await GetConflictsByEtagAsync(context, etag);
                    await WriteResultsByEtagAsync(context, result);
                }
                else
                    await GetConflictsForDocumentAsync(context, docId);
            }
        }

        private async Task WriteResultsByEtagAsync(JsonOperationContext context, GetConflictsResultByEtag result)
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                var array = new DynamicJsonArray();

                foreach (var conflict in result.Results)
                {
                    array.Add(new DynamicJsonValue
                    {
                        [nameof(GetConflictsResultByEtag.ResultByEtag.Id)] = conflict.Id,
                        [nameof(GetConflictsResult.Conflict.LastModified)] = conflict.LastModified
                    });
                }

                context.Write(writer, new DynamicJsonValue
                {
                    [nameof(GetConflictsResult.TotalResults)] = result.TotalResults,
                    [nameof(GetConflictsResult.Results)] = array
                });
            }
        }
    }
}
