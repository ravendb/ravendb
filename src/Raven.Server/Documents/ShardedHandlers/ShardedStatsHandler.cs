using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.Util;
using Raven.Server.Documents.ShardedHandlers.ShardedCommands;
using Raven.Server.Documents.Sharding;
using Raven.Server.Routing;
using Sparrow.Json;

namespace Raven.Server.Documents.ShardedHandlers
{
    public class ShardedStatsHandler : ShardedRequestHandler
    {
        [RavenShardedAction("/databases/*/stats", "GET")]
        public async Task Stats()
        {
            var op = new ShardedStatsOperation();

            var statistics = await ShardExecutor.ExecuteParallelForAllAsync(op);

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    WriteDatabaseStatisticsInternal(writer, statistics);

                    writer.WriteEndObject();
                }
            }
        }

        private static void WriteDatabaseStatisticsInternal(AbstractBlittableJsonTextWriter writer, DatabaseStatistics statistics)
        {
            writer.WritePropertyName(nameof(statistics.CountOfIndexes));
            writer.WriteInteger(statistics.CountOfIndexes);
            writer.WriteComma();

            writer.WritePropertyName(nameof(statistics.CountOfDocuments));
            writer.WriteInteger(statistics.CountOfDocuments);
            writer.WriteComma();

            if (statistics.CountOfRevisionDocuments > 0)
            {
                writer.WritePropertyName(nameof(statistics.CountOfRevisionDocuments));
                writer.WriteInteger(statistics.CountOfRevisionDocuments);
                writer.WriteComma();
            }

            writer.WritePropertyName(nameof(statistics.CountOfTombstones));
            writer.WriteInteger(statistics.CountOfTombstones);
            writer.WriteComma();

            writer.WritePropertyName(nameof(statistics.CountOfDocumentsConflicts));
            writer.WriteInteger(statistics.CountOfDocumentsConflicts);
            writer.WriteComma();

            writer.WritePropertyName(nameof(statistics.CountOfConflicts));
            writer.WriteInteger(statistics.CountOfConflicts);
            writer.WriteComma();

            writer.WritePropertyName(nameof(statistics.CountOfAttachments));
            writer.WriteInteger(statistics.CountOfAttachments);
            writer.WriteComma();

            writer.WritePropertyName(nameof(statistics.CountOfCounterEntries));
            writer.WriteInteger(statistics.CountOfCounterEntries);
            writer.WriteComma();

            writer.WritePropertyName(nameof(statistics.CountOfTimeSeriesSegments));
            writer.WriteInteger(statistics.CountOfTimeSeriesSegments);
            writer.WriteComma();

            writer.WritePropertyName(nameof(statistics.CountOfUniqueAttachments));
            writer.WriteInteger(statistics.CountOfUniqueAttachments);
            writer.WriteComma();

            writer.WritePropertyName(nameof(statistics.DatabaseChangeVector));
            writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(nameof(statistics.DatabaseId));
            writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(nameof(statistics.NumberOfTransactionMergerQueueOperations));
            writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(nameof(statistics.Is64Bit));
            writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(nameof(statistics.Pager));
            writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(nameof(statistics.LastDocEtag));
            writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(nameof(statistics.LastDatabaseEtag));
            writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName((nameof(statistics.DatabaseChangeVector)));
            writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(nameof(statistics.LastIndexingTime));
            writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(nameof(statistics.SizeOnDisk));
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(statistics.SizeOnDisk.HumaneSize));
            writer.WriteString(statistics.SizeOnDisk.HumaneSize);
            writer.WriteComma();

            writer.WritePropertyName(nameof(statistics.SizeOnDisk.SizeInBytes));
            writer.WriteInteger(statistics.SizeOnDisk.SizeInBytes);

            writer.WriteEndObject();
            writer.WriteComma();

            writer.WritePropertyName(nameof(statistics.TempBuffersSizeOnDisk));
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(statistics.TempBuffersSizeOnDisk.HumaneSize));
            writer.WriteString(statistics.TempBuffersSizeOnDisk.HumaneSize);
            writer.WriteComma();

            writer.WritePropertyName(nameof(statistics.TempBuffersSizeOnDisk.SizeInBytes));
            writer.WriteInteger(statistics.TempBuffersSizeOnDisk.SizeInBytes);

            writer.WriteEndObject();
        }

        private readonly struct ShardedStatsOperation : IShardedOperation<DatabaseStatistics>
        {
            public DatabaseStatistics Combine(Memory<DatabaseStatistics> results)
            {
                var span = results.Span;

                var combined = new DatabaseStatistics
                {
                    CountOfIndexes = -1
                };

                long totalSizeOnDisk = 0;
                long totalTempBuffersSizeOnDisk = 0;
                for (int i = 0; i < span.Length; i++)
                {
                    var result = span[i];
                    combined.CountOfDocuments += result.CountOfDocuments;
                    combined.CountOfAttachments += result.CountOfAttachments;
                    combined.CountOfConflicts += result.CountOfConflicts;
                    combined.CountOfCounterEntries += result.CountOfCounterEntries;
                    combined.CountOfDocumentsConflicts += result.CountOfDocumentsConflicts;
                    combined.CountOfRevisionDocuments += result.CountOfRevisionDocuments;
                    combined.CountOfTimeSeriesSegments += result.CountOfTimeSeriesSegments;
                    combined.CountOfTombstones += result.CountOfTombstones;
                    totalSizeOnDisk += result.SizeOnDisk.SizeInBytes;
                    totalTempBuffersSizeOnDisk += result.TempBuffersSizeOnDisk.SizeInBytes;

                    if (combined.CountOfIndexes == -1)
                        combined.CountOfIndexes = result.CountOfIndexes;
                }

                combined.SizeOnDisk = new Size(totalSizeOnDisk);
                combined.TempBuffersSizeOnDisk = new Size(totalTempBuffersSizeOnDisk);

                return combined;
            }

            public RavenCommand<DatabaseStatistics> CreateCommandForShard(int shard) => new GetStatisticsOperation.GetStatisticsCommand(null, null);
        }
    }

}
