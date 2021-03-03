using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Indexes;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class StatsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/stats/detailed", "GET", AuthorizationStatus.ValidUser)]
        public async Task DetailedStats()
        {
            using (var context = QueryOperationContext.Allocate(Database, needsServerContext: true))
            {
                var stats = new DetailedDatabaseStatistics();

                FillDatabaseStatistics(stats, context);

                using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverContext))
                using (serverContext.OpenReadTransaction())
                {
                    stats.CountOfIdentities = ServerStore.Cluster.GetNumberOfIdentities(serverContext, Database.Name);
                    stats.CountOfCompareExchange = ServerStore.Cluster.GetNumberOfCompareExchange(serverContext, Database.Name);
                    stats.CountOfCompareExchangeTombstones = ServerStore.Cluster.GetNumberOfCompareExchangeTombstones(serverContext, Database.Name);
                }

                await using (var writer = new AsyncBlittableJsonTextWriter(context.Documents, ResponseBodyStream()))
                    writer.WriteDetailedDatabaseStatistics(context.Documents, stats);
            }
        }

        [RavenAction("/databases/*/stats", "GET", AuthorizationStatus.ValidUser, IsDebugInformationEndpoint = true)]
        public async Task Stats()
        {
            using (var context = QueryOperationContext.Allocate(Database, needsServerContext: true))
            {
                var stats = new DatabaseStatistics();

                FillDatabaseStatistics(stats, context);

                await using (var writer = new AsyncBlittableJsonTextWriter(context.Documents, ResponseBodyStream()))
                    writer.WriteDatabaseStatistics(context.Documents, stats);
            }
        }

        [RavenAction("/databases/*/healthcheck", "GET", AuthorizationStatus.ValidUser)]
        public Task DatabaseHealthCheck()
        {
            NoContentStatus();
            return Task.CompletedTask;
        }

        private void FillDatabaseStatistics(DatabaseStatistics stats, QueryOperationContext context)
        {
            using (context.OpenReadTransaction())
            {
                var indexes = Database.IndexStore.GetIndexes().ToList();
                var size = Database.GetSizeOnDisk();

                stats.LastDocEtag = DocumentsStorage.ReadLastDocumentEtag(context.Documents.Transaction.InnerTransaction);
                stats.LastDatabaseEtag = DocumentsStorage.ReadLastEtag(context.Documents.Transaction.InnerTransaction);
                stats.DatabaseChangeVector = DocumentsStorage.GetDatabaseChangeVector(context.Documents);

                stats.CountOfDocuments = Database.DocumentsStorage.GetNumberOfDocuments(context.Documents);
                stats.CountOfRevisionDocuments = Database.DocumentsStorage.RevisionsStorage.GetNumberOfRevisionDocuments(context.Documents);
                stats.CountOfDocumentsConflicts = Database.DocumentsStorage.ConflictsStorage.GetNumberOfDocumentsConflicts(context.Documents);
                stats.CountOfTombstones = Database.DocumentsStorage.GetNumberOfTombstones(context.Documents);
                stats.CountOfConflicts = Database.DocumentsStorage.ConflictsStorage.ConflictsCount;
                stats.SizeOnDisk = size.Data;
                stats.NumberOfTransactionMergerQueueOperations = Database.TxMerger.NumberOfQueuedOperations;
                stats.TempBuffersSizeOnDisk = size.TempBuffers;
                stats.CountOfCounterEntries = Database.DocumentsStorage.CountersStorage.GetNumberOfCounterEntries(context.Documents);

                stats.CountOfTimeSeriesSegments = Database.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesSegments(context.Documents);

                var attachments = Database.DocumentsStorage.AttachmentsStorage.GetNumberOfAttachments(context.Documents);
                stats.CountOfAttachments = attachments.AttachmentCount;
                stats.CountOfUniqueAttachments = attachments.StreamsCount;
                stats.CountOfIndexes = indexes.Count;

                stats.DatabaseId = Database.DocumentsStorage.Environment.Base64Id;
                stats.Is64Bit = !Database.DocumentsStorage.Environment.Options.ForceUsing32BitsPager && IntPtr.Size == sizeof(long);
                stats.Pager = Database.DocumentsStorage.Environment.Options.DataPager.GetType().ToString();

                stats.Indexes = new IndexInformation[indexes.Count];
                for (var i = 0; i < indexes.Count; i++)
                {
                    var index = indexes[i];
                    bool isStale;
                    try
                    {
                        isStale = index.IsStale(context);
                    }
                    catch (OperationCanceledException)
                    {
                        // if the index has just been removed, let us consider it stale
                        // until it can be safely removed from the list of indexes in the
                        // database
                        isStale = true;
                    }
                    stats.Indexes[i] = new IndexInformation
                    {
                        State = index.State,
                        IsStale = isStale,
                        Name = index.Name,
                        LockMode = index.Definition.LockMode,
                        Priority = index.Definition.Priority,
                        Type = index.Type,
                        LastIndexingTime = index.LastIndexingTime,
                        SourceType = index.SourceType
                    };

                    if (stats.LastIndexingTime.HasValue)
                        stats.LastIndexingTime = stats.LastIndexingTime >= index.LastIndexingTime ? stats.LastIndexingTime : index.LastIndexingTime;
                    else
                        stats.LastIndexingTime = index.LastIndexingTime;
                }
            }
        }

        [RavenAction("/databases/*/metrics", "GET", AuthorizationStatus.ValidUser)]
        public async Task Metrics()
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, Database.Metrics.ToJson());
            }
        }

        [RavenAction("/databases/*/metrics/puts", "GET", AuthorizationStatus.ValidUser)]
        public async Task PutsMetrics()
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var empty = GetBoolValueQueryString("empty", required: false) ?? true;

                context.Write(writer, new DynamicJsonValue
                {
                    [nameof(Database.Metrics.Docs)] = new DynamicJsonValue
                    {
                        [nameof(Database.Metrics.Docs.PutsPerSec)] = Database.Metrics.Docs.PutsPerSec.CreateMeterData(true, empty)
                    },
                    [nameof(Database.Metrics.Attachments)] = new DynamicJsonValue
                    {
                        [nameof(Database.Metrics.Attachments.PutsPerSec)] = Database.Metrics.Attachments.PutsPerSec.CreateMeterData(true, empty)
                    },
                    [nameof(Database.Metrics.Counters)] = new DynamicJsonValue
                    {
                        [nameof(Database.Metrics.Counters.PutsPerSec)] = Database.Metrics.Counters.PutsPerSec.CreateMeterData(true, empty)
                    },
                    [nameof(Database.Metrics.TimeSeries)] = new DynamicJsonValue
                    {
                        [nameof(Database.Metrics.TimeSeries.PutsPerSec)] = Database.Metrics.TimeSeries.PutsPerSec.CreateMeterData(true, empty)
                    }
                });
            }
        }

        [RavenAction("/databases/*/metrics/bytes", "GET", AuthorizationStatus.ValidUser)]
        public async Task BytesMetrics()
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var empty = GetBoolValueQueryString("empty", required: false) ?? true;

                context.Write(writer, new DynamicJsonValue
                {
                    [nameof(Database.Metrics.Docs)] = new DynamicJsonValue
                    {
                        [nameof(Database.Metrics.Docs.BytesPutsPerSec)] = Database.Metrics.Docs.BytesPutsPerSec.CreateMeterData(true, empty)
                    },
                    [nameof(Database.Metrics.Attachments)] = new DynamicJsonValue
                    {
                        [nameof(Database.Metrics.Attachments.BytesPutsPerSec)] = Database.Metrics.Attachments.BytesPutsPerSec.CreateMeterData(true, empty)
                    },
                    [nameof(Database.Metrics.Counters)] = new DynamicJsonValue
                    {
                        [nameof(Database.Metrics.Counters.BytesPutsPerSec)] = Database.Metrics.Counters.BytesPutsPerSec.CreateMeterData(true, empty)
                    },
                    [nameof(Database.Metrics.TimeSeries)] = new DynamicJsonValue
                    {
                        [nameof(Database.Metrics.TimeSeries.BytesPutsPerSec)] = Database.Metrics.TimeSeries.BytesPutsPerSec.CreateMeterData(true, empty)
                    }
                });
            }
        }
    }
}
