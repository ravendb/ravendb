using System;
using System.Linq;
using System.Threading.Tasks;

using Raven.Client.Data;
using Raven.Database.Util;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class StatsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/stats", "GET")]
        public Task Stats()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            using (context.OpenReadTransaction())
            {
                var indexes = Database.IndexStore.GetIndexes().ToList();
                var transformersCount = Database.TransformerStore.GetTransformersCount();

                var stats = new DatabaseStatistics();
                stats.CountOfDocuments = Database.DocumentsStorage.GetNumberOfDocuments(context);
                stats.CountOfRevisionDocuments = Database.BundleLoader.VersioningStorage?.GetNumberOfRevisionDocuments(context);
                stats.ApproximateTaskCount = 0; // TODO [ppekrol]
                stats.CountOfIndexes = indexes.Count;
                stats.CountOfTransformers = transformersCount;
                stats.CurrentNumberOfItemsToIndexInSingleBatch = 1; // TODO [ppekrol]
                stats.CurrentNumberOfItemsToReduceInSingleBatch = 1; // TODO [ppekrol]
                stats.CurrentNumberOfParallelTasks = 1; // TODO [ppekrol]
                stats.DatabaseId = Database.DocumentsStorage.Environment.DbId;
                stats.Is64Bit = IntPtr.Size == sizeof(long);

                stats.Indexes = new IndexInformation[indexes.Count];
                for (var i = 0; i < indexes.Count; i++)
                {
                    var index = indexes[i];
                    stats.Indexes[i] = new IndexInformation
                    {
                        Priority = index.Priority,
                        IsStale = index.IsStale(context),
                        Name = index.Name,
                        IndexId = index.IndexId,
                        LockMode = index.Definition.LockMode
                    };
                }

                writer.WriteDatabaseStatistics(context, stats);
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