using System.Collections.Generic;
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

        protected abstract Task<GetConflictsPreviewResult> GetConflictsPreviewAsync(TOperationContext context, long etag, int pageSize);

        protected abstract Task GetConflictsForDocumentAsync(TOperationContext context, string documentId);

        public override async ValueTask ExecuteAsync()
        {
            var docId = RequestHandler.GetStringQueryString("docId", required: false);
            var start = RequestHandler.GetLongQueryString("start", required: false) ?? 0;
            var pageSize = RequestHandler.GetIntValueQueryString("pageSize", required: false) ?? int.MaxValue;

            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            {
                if (string.IsNullOrWhiteSpace(docId))
                {
                    var result = await GetConflictsPreviewAsync(context, start, pageSize);
                    await WriteResultsAsync(context, result);
                }
                else
                    await GetConflictsForDocumentAsync(context, docId);
            }
        }

        protected async ValueTask WriteResultsAsync(JsonOperationContext context, GetConflictsPreviewResult previewResult)
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                var array = new DynamicJsonArray();

                foreach (var conflict in previewResult.Results)
                {
                    array.Add(new DynamicJsonValue
                    {
                        [nameof(GetConflictsPreviewResult.ConflictPreview.Id)] = conflict.Id,
                        [nameof(GetConflictsPreviewResult.ConflictPreview.LastModified)] = conflict.LastModified
                    });
                }

                context.Write(writer, new DynamicJsonValue
                {
                    [nameof(GetConflictsPreviewResult.TotalResults)] = previewResult.TotalResults,
                    [nameof(GetConflictsPreviewResult.Results)] = array,
                    [nameof(GetConflictsPreviewResult.ContinuationToken)] = previewResult.ContinuationToken
                });
            }
        }
    }

    public class ConflictsPreviewComparer : Comparer<GetConflictsPreviewResult.ConflictPreview>
    {
        public override int Compare(GetConflictsPreviewResult.ConflictPreview x, GetConflictsPreviewResult.ConflictPreview y)
        {
            if (x == null)
                return -1;
            if (y == null)
                return -1;

            if (x.LastModified.Ticks == y.LastModified.Ticks)
                return 0;

            if (x.LastModified.Ticks < y.LastModified.Ticks)
                return -1;

            return 1;
        }

        public static ConflictsPreviewComparer Instance = new();
    }
}
