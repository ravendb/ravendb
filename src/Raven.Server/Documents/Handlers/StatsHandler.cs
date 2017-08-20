using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Replication;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class StatsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/stats", "GET", AuthorizationStatus.ValidUser)]
        public Task Stats()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))            
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            using (context.OpenReadTransaction())
            {
                var indexes = Database.IndexStore.GetIndexes().ToList();

                var stats = new DatabaseStatistics
                {
                    LastDocEtag = DocumentsStorage.ReadLastDocumentEtag(context.Transaction.InnerTransaction),
                    CountOfDocuments = Database.DocumentsStorage.GetNumberOfDocuments(context),
                    CountOfRevisionDocuments = Database.DocumentsStorage.RevisionsStorage.GetNumberOfRevisionDocuments(context)
                };

                var attachments = Database.DocumentsStorage.AttachmentsStorage.GetNumberOfAttachments(context);
                stats.CountOfAttachments = attachments.AttachmentCount;
                stats.CountOfUniqueAttachments = attachments.StreamsCount;
                stats.CountOfIndexes = indexes.Count;
                var statsDatabaseChangeVector = DocumentsStorage.GetDatabaseChangeVector(context);
                
                stats.DatabaseChangeVector = statsDatabaseChangeVector;
                stats.DatabaseId = ChangeVectorEntry.GuidToTruncatedBase64(Database.DocumentsStorage.Environment.DbId);
                stats.Is64Bit = IntPtr.Size == sizeof(long);
                stats.Pager = Database.DocumentsStorage.Environment.Options.DataPager.GetType().ToString();

                stats.Indexes = new IndexInformation[indexes.Count];
                for (var i = 0; i < indexes.Count; i++)
                {
                    var index = indexes[i];
                    stats.Indexes[i] = new IndexInformation
                    {
                        State = index.State,
                        IsStale = index.IsStale(context),
                        Name = index.Name,
                        Etag = index.Etag,
                        LockMode = index.Definition.LockMode,
                        Priority = index.Definition.Priority,
                        Type = index.Type,
                        LastIndexingTime = index.LastIndexingTime
                    };

                    if (stats.LastIndexingTime.HasValue)
                        stats.LastIndexingTime = stats.LastIndexingTime >= index.LastIndexingTime ? stats.LastIndexingTime : index.LastIndexingTime;
                    else
                        stats.LastIndexingTime = index.LastIndexingTime;
                }

                writer.WriteDatabaseStatistics(context, stats);
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/metrics", "GET", AuthorizationStatus.ValidUser)]
        public Task Metrics()
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, Database.Metrics.CreateMetricsStatsJsonValue());
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/metrics/puts", "GET", AuthorizationStatus.ValidUser)]
        public Task PutsMetrics()
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, new DynamicJsonValue
                {
                    ["DocsPutsPerSec"] = Database.Metrics.DocPutsPerSecond.CreateMeterData(true, GetBoolValueQueryString("empty", required: false) ?? true)
                });
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/metrics/bytes", "GET", AuthorizationStatus.ValidUser)]
        public Task BytesMetrics()
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, new DynamicJsonValue
                {
                    ["BytesPutsPerSecond"] = Database.Metrics.BytesPutsPerSecond.CreateMeterData(true, GetBoolValueQueryString("empty", required: false) ?? true)
                });
            }

            return Task.CompletedTask;
        }
    }
}
