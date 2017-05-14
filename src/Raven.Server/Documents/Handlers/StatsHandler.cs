using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Server.Operations;
using Raven.Client.Server.PeriodicExport;
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
                stats.LastDocEtag = DocumentsStorage.ReadLastDocumentEtag(context.Transaction.InnerTransaction);
                
                stats.CountOfDocuments = Database.DocumentsStorage.GetNumberOfDocuments(context);
                stats.CountOfRevisionDocuments = Database.BundleLoader.VersioningStorage?.GetNumberOfRevisionDocuments(context);
                var attachments = Database.DocumentsStorage.AttachmentsStorage.GetNumberOfAttachments(context);
                stats.CountOfAttachments = attachments.AttachmentCount;
                stats.CountOfUniqueAttachments = attachments.StreamsCount;
                stats.CountOfIndexes = indexes.Count;
                stats.CountOfTransformers = transformersCount;
                var statsDatabaseChangeVector = Database.DocumentsStorage.GetDatabaseChangeVector(context).ToDictionary(x=>x.DbId, x=>x);

                statsDatabaseChangeVector[Database.DbId] = new ChangeVectorEntry()
                {
                    DbId = Database.DbId,
                    Etag = stats.LastDocEtag.Value
                };

                stats.DatabaseChangeVector = statsDatabaseChangeVector.Values.ToArray();
                stats.DatabaseId = Database.DocumentsStorage.Environment.DbId;
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

        [RavenAction("/databases/*/metrics/puts", "GET")]
        public Task PutsMetrics()
        {
            JsonOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, new DynamicJsonValue
                {
                    ["DocsPutsPerSec"] = Database.Metrics.DocPutsPerSecond.CreateMeterData(true, GetBoolValueQueryString("empty", required:false) ?? true),
                });
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/metrics/bytes", "GET")]
        public Task BytesMetrics()
        {
            JsonOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, new DynamicJsonValue
                {
                    ["BytesPutsPerSecond"] = Database.Metrics.BytesPutsPerSecond.CreateMeterData(true, GetBoolValueQueryString("empty", required: false) ?? true),
                });
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/periodic-backup/status", "GET")]
        public Task GetPeriodicExportBundleStatus()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.OpenReadTransaction();
                writer.WriteStartObject();
                writer.WritePropertyName(nameof(GetPeriodicBackupStatusOperationResult.Status));
                writer.WriteObject(Database.ConfigurationStorage.PeriodicBackupStorage.GetDatabasePeriodicBackupStatus(context));
                writer.WriteEndObject();
                writer.Flush();
            }
            return Task.CompletedTask;
        }
    }
}