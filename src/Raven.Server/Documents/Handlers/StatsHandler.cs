using System;
using System.Linq;
using System.Threading.Tasks;

using Raven.Client.Data.Indexes;
using Raven.Database.Util;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

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
                    var indexes = Database.IndexStore.GetIndexes().ToList();

                    //TODO: Implement properly and split to dedicated endpoints
                    //TODO: So we don't get so much stuff to ignore in the stats
                    context.Write(writer, new DynamicJsonValue
                    {
                        // storage
                        ["StorageEngine"] = "Voron 4.0",
                        ["DatabaseTransactionVersionSizeInMB"] = -1,

                        // indexing - should be in its /stats/indexing
                        ["CountOfIndexes"] = indexes.Count,
                        ["StaleIndexes"] = new DynamicJsonArray(),
                        ["CountOfIndexesExcludingDisabled"] = indexes.Count(index => index.Priority.HasFlag(IndexingPriority.Disabled) == false),
                        ["CountOfResultTransformers"] = 0,
                        ["InMemoryIndexingQueueSizes"] = new DynamicJsonArray(),
                        ["ApproximateTaskCount"] = 0,
                        ["CurrentNumberOfParallelTasks"] = 1,
                        ["CurrentNumberOfItemsToIndexInSingleBatch"] = 1,
                        ["CurrentNumberOfItemsToReduceInSingleBatch"] = 1,

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

        [RavenAction("/databases/*/metrics", "GET")]
        public Task Metrics()
        {
            JsonOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, Database.Metrics.CreateMetricsStatsJsonValue());
            }
            
            return Task.CompletedTask;
        }
    }
}