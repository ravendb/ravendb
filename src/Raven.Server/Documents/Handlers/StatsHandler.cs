using System;
using System.Threading.Tasks;

using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers
{
    public class StatsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/stats", "GET")]
        public Task Stats()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    //TODO: Implement properly and split to dedicated endpoints
                    //TODO: So we don't get so much stuff to ignore in the stats
                    context.Write(writer, new DynamicJsonValue
                    {
                        // storage
                        ["StorageEngine"] = "Voron 4.0",
                        ["DatabaseTransactionVersionSizeInMB"] = -1,

                        // indexing - should be in its /stats/indexing
                        ["CountOfIndexes"] = 0,
                        ["StaleIndexes"] = new DynamicJsonArray(),
                        ["CountOfIndexesExcludingDisabledAndAbandoned"] = 0,
                        ["CountOfResultTransformers"] = 0,
                        ["InMemoryIndexingQueueSizes"] = new DynamicJsonArray(),
                        ["ApproximateTaskCount"] = 0,
                        ["CurrentNumberOfParallelTasks"] = 1,
                        ["CurrentNumberOfItemsToIndexInSingleBatch"] = 1,
                        ["CurrentNumberOfItemsToReduceInSingleBatch"] = 1,

                        ["Errors"] = new DynamicJsonArray(),
                        ["Prefetches"] = new DynamicJsonArray(),

                        // documents
                        ["LastDocEtag"] = DocumentsStorage.ReadLastEtag(context.Transaction.InnerTransaction),
                        ["CountOfDocuments"] = Database.DocumentsStorage.GetNumberOfDocuments(context),
                        ["DatabaseId"] = Database.DocumentsStorage.Environment.DbId.ToString(),
                        ["Is64Bits"] = IntPtr.Size == sizeof (long)
                    });
                }
            }
            return Task.CompletedTask;
        }
    }
}