using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal abstract class AbstractReplicationHandlerProcessorForGetTombstones<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractReplicationHandlerProcessorForGetTombstones([NotNull] TRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected abstract ValueTask<GetTombstonesPreviewResult> GetTombstonesAsync(TOperationContext context, int start, int pageSize);

        public override async ValueTask ExecuteAsync()
        {
            var start = RequestHandler.GetStart();
            var pageSize = RequestHandler.GetPageSize();

            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            {
                var tombstones = await GetTombstonesAsync(context, start, pageSize);
                await WriteResultsAsync(context, tombstones, pageSize);
            }
        }

        protected async ValueTask WriteResultsAsync(JsonOperationContext context, GetTombstonesPreviewResult previewResult, int pageSize)
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                var array = new DynamicJsonArray();
                if (previewResult.Tombstones != null)
                {
                    foreach (var tombstone in previewResult.Tombstones)
                    {
                        if (pageSize-- <= 0)
                            break;

                        array.Add(tombstone.ToJson());
                    }
                }

                context.Write(writer, new DynamicJsonValue
                {
                    [nameof(GetTombstonesPreviewResult.Tombstones)] = array,
                    [nameof(GetTombstonesPreviewResult.ContinuationToken)] = previewResult.ContinuationToken
                });
            }
        }
    }

    public class GetTombstonesPreviewResult
    {
        public List<Tombstone> Tombstones { get; set; }
        public string ContinuationToken { get; set; }
    }
}
