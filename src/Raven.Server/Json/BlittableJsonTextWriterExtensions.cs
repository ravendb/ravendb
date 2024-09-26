using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Queries.Suggestions;
using Raven.Client.Documents.Queries.Timings;
using Raven.Client.Extensions;
using Raven.Server.Documents;
using Raven.Server.Documents.Commands.Indexes;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Debugging;
using Raven.Server.Documents.Indexes.Spatial;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Dynamic;
using Raven.Server.Documents.Queries.Facets;
using Raven.Server.Documents.Queries.Suggestions;
using Raven.Server.Documents.QueueSink.Stats.Performance;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.Documents.Sharding.Handlers.ContinuationTokens;
using Raven.Server.Documents.Sharding.Queries;
using Raven.Server.Documents.Sharding.Queries.Suggestions;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.Subscriptions.Stats;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron.Data.BTrees;
using IndexStats = Raven.Client.Documents.Indexes.IndexStats;

namespace Raven.Server.Json
{
    internal static class BlittableJsonTextWriterExtensions
    {
        public static void WritePerformanceStats(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, IEnumerable<IndexPerformanceStats> stats)
        {
            writer.WriteStartObject();
            writer.WriteArray(context, "Results", stats, (w, c, stat) =>
            {
                w.WriteStartObject();

                w.WritePropertyName(nameof(stat.Name));
                w.WriteString(stat.Name);
                w.WriteComma();

                w.WriteArray(c, nameof(stat.Performance), stat.Performance, (wp, cp, performance) => { wp.WriteIndexingPerformanceStats(context, performance); });

                w.WriteEndObject();
            });
            writer.WriteEndObject();
        }

        public static void WriteSubscriptionTaskPerformanceStats(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, IEnumerable<SubscriptionTaskPerformanceStats> stats)
        {
            writer.WriteStartObject();
            writer.WriteArray(context, "Results", stats, (w, c, taskStats) =>
            {
                w.WriteStartObject();

                w.WritePropertyName(nameof(taskStats.TaskId));
                w.WriteInteger(taskStats.TaskId);
                w.WriteComma();

                w.WritePropertyName(nameof(taskStats.TaskName));
                w.WriteString(taskStats.TaskName);

                if (taskStats.ConnectionPerformance != null && taskStats.ConnectionPerformance.Any())
                {
                    w.WriteComma();
                    w.WriteArray(c, nameof(taskStats.ConnectionPerformance), taskStats.ConnectionPerformance, (wp, cp, connectionStats) =>
                        wp.WriteSubscriptionConnectionPerformanceStats(cp, connectionStats));
                }

                if (taskStats.BatchPerformance != null && taskStats.BatchPerformance.Any())
                {
                    w.WriteComma();
                    w.WriteArray(c, nameof(taskStats.BatchPerformance), taskStats.BatchPerformance, (wp, cp, batchStats) =>
                        wp.WriteSubscriptionBatchPerformanceStats(cp, batchStats));
                }

                w.WriteEndObject();
            });

            writer.WriteEndObject();
        }

        public static void WriteEtlTaskPerformanceStats(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, IEnumerable<EtlTaskPerformanceStats> stats)
        {
            writer.WriteStartObject();
            writer.WriteArray(context, "Results", stats, (w, c, taskStats) =>
            {
                w.WriteStartObject();

                w.WritePropertyName(nameof(taskStats.TaskId));
                w.WriteInteger(taskStats.TaskId);
                w.WriteComma();

                w.WritePropertyName(nameof(taskStats.TaskName));
                w.WriteString(taskStats.TaskName);
                w.WriteComma();

                w.WritePropertyName(nameof(taskStats.EtlType));
                w.WriteString(taskStats.EtlType.ToString());
                w.WriteComma();

                w.WritePropertyName(nameof(taskStats.EtlSubType));
                w.WriteString(taskStats.EtlSubType?.ToString());
                w.WriteComma();

                w.WriteArray(c, nameof(taskStats.Stats), taskStats.Stats, (wp, cp, scriptStats) =>
                {
                    wp.WriteStartObject();

                    wp.WritePropertyName(nameof(scriptStats.TransformationName));
                    wp.WriteString(scriptStats.TransformationName);
                    wp.WriteComma();

                    wp.WriteArray(cp, nameof(scriptStats.Performance), scriptStats.Performance, (wpp, cpp, perfStats) => wpp.WriteEtlPerformanceStats(cpp, perfStats));

                    wp.WriteEndObject();
                });

                w.WriteEndObject();
            });
            writer.WriteEndObject();
        }

        public static void WriteQueueSinkTaskPerformanceStats(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, IEnumerable<QueueSinkTaskPerformanceStats> stats)
        {
            writer.WriteStartObject();
            writer.WriteArray(context, "Results", stats, (w, c, taskStats) =>
            {
                w.WriteStartObject();

                w.WritePropertyName(nameof(taskStats.TaskId));
                w.WriteInteger(taskStats.TaskId);
                w.WriteComma();

                w.WritePropertyName(nameof(taskStats.TaskName));
                w.WriteString(taskStats.TaskName);
                w.WriteComma();

                w.WritePropertyName(nameof(taskStats.BrokerType));
                w.WriteString(taskStats.BrokerType.ToString());
                w.WriteComma();


                w.WriteArray(c, nameof(taskStats.Stats), taskStats.Stats, (wp, cp, scriptStats) =>
                {
                    wp.WriteStartObject();

                    wp.WritePropertyName(nameof(scriptStats.ScriptName));
                    wp.WriteString(scriptStats.ScriptName);
                    wp.WriteComma();

                    wp.WriteArray(cp, nameof(scriptStats.Performance), scriptStats.Performance, (wpp, cpp, perfStats) => wpp.WriteQueueSinkPerformanceStats(cpp, perfStats));

                    wp.WriteEndObject();
                });

                w.WriteEndObject();
            });
            writer.WriteEndObject();
        }

        public static void WriteEtlTaskProgress<TWriter>(this TWriter writer, JsonOperationContext context, IEnumerable<EtlTaskProgress> progress)
            where TWriter : IBlittableJsonTextWriter
        {
            writer.WriteStartObject();
            writer.WriteArray(context, "Results", progress, (w, c, taskStats) =>
            {
                w.WriteStartObject();

                w.WritePropertyName(nameof(taskStats.TaskName));
                w.WriteString(taskStats.TaskName);
                w.WriteComma();

                w.WritePropertyName(nameof(taskStats.EtlType));
                w.WriteString(taskStats.EtlType.ToString());
                w.WriteComma();

                if (taskStats.QueueBrokerType.HasValue)
                {
                    w.WritePropertyName(nameof(taskStats.QueueBrokerType));
                    w.WriteString(taskStats.QueueBrokerType.ToString());
                    w.WriteComma();
                }

                w.WriteArray(c, nameof(taskStats.ProcessesProgress), taskStats.ProcessesProgress, (wp, cp, processProgress) =>
                {
                    wp.WriteStartObject();

                    wp.WritePropertyName(nameof(processProgress.TransformationName));
                    wp.WriteString(processProgress.TransformationName);
                    wp.WriteComma();

                    if (processProgress.TransactionalId is not null)
                    {
                        wp.WritePropertyName(nameof(processProgress.TransactionalId));
                        wp.WriteString(processProgress.TransactionalId);
                        wp.WriteComma();
                    }

                    wp.WritePropertyName(nameof(processProgress.Completed));
                    wp.WriteBool(processProgress.Completed);
                    wp.WriteComma();

                    wp.WritePropertyName(nameof(processProgress.Disabled));
                    wp.WriteBool(processProgress.Disabled);
                    wp.WriteComma();

                    wp.WritePropertyName(nameof(processProgress.AverageProcessedPerSecond));
                    wp.WriteDouble(processProgress.AverageProcessedPerSecond);
                    wp.WriteComma();

                    wp.WritePropertyName(nameof(processProgress.NumberOfDocumentsToProcess));
                    wp.WriteInteger(processProgress.NumberOfDocumentsToProcess);
                    wp.WriteComma();

                    wp.WritePropertyName(nameof(processProgress.TotalNumberOfDocuments));
                    wp.WriteInteger(processProgress.TotalNumberOfDocuments);
                    wp.WriteComma();

                    wp.WritePropertyName(nameof(processProgress.NumberOfDocumentTombstonesToProcess));
                    wp.WriteInteger(processProgress.NumberOfDocumentTombstonesToProcess);
                    wp.WriteComma();

                    wp.WritePropertyName(nameof(processProgress.TotalNumberOfDocumentTombstones));
                    wp.WriteInteger(processProgress.TotalNumberOfDocumentTombstones);
                    wp.WriteComma();

                    wp.WritePropertyName(nameof(processProgress.NumberOfCounterGroupsToProcess));
                    wp.WriteInteger(processProgress.NumberOfCounterGroupsToProcess);
                    wp.WriteComma();

                    wp.WritePropertyName(nameof(processProgress.TotalNumberOfCounterGroups));
                    wp.WriteInteger(processProgress.TotalNumberOfCounterGroups);
                    wp.WriteComma();

                    wp.WritePropertyName(nameof(processProgress.NumberOfTimeSeriesSegmentsToProcess));
                    wp.WriteInteger(processProgress.NumberOfTimeSeriesSegmentsToProcess);
                    wp.WriteComma();

                    wp.WritePropertyName(nameof(processProgress.TotalNumberOfTimeSeriesSegments));
                    wp.WriteInteger(processProgress.TotalNumberOfTimeSeriesSegments);
                    wp.WriteComma();

                    wp.WritePropertyName(nameof(processProgress.NumberOfTimeSeriesDeletedRangesToProcess));
                    wp.WriteInteger(processProgress.NumberOfTimeSeriesDeletedRangesToProcess);
                    wp.WriteComma();

                    wp.WritePropertyName(nameof(processProgress.TotalNumberOfTimeSeriesDeletedRanges));
                    wp.WriteInteger(processProgress.TotalNumberOfTimeSeriesDeletedRanges);

                    wp.WriteEndObject();
                });

                w.WriteEndObject();
            });
            writer.WriteEndObject();
        }

        public static void WriteReplicationTaskProgress<TWriter>(this TWriter writer, JsonOperationContext context, IEnumerable<ReplicationTaskProgress> progress)
        where TWriter : IBlittableJsonTextWriter
        {
            writer.WriteStartObject();
            writer.WriteArray(context, "Results", progress, (w, c, taskStats) =>
            {
                w.WriteStartObject();

                w.WritePropertyName(nameof(taskStats.TaskName));
                w.WriteString(taskStats.TaskName);
                w.WriteComma();

                w.WritePropertyName(nameof(taskStats.ReplicationType));
                w.WriteString(taskStats.ReplicationType.ToString());
                w.WriteComma();

                writer.WriteArray(context, nameof(taskStats.ProcessesProgress), taskStats.ProcessesProgress, (w, c, processProgress) =>
                {
                    w.WriteStartObject();

                    w.WritePropertyName(nameof(processProgress.FromToString));
                    w.WriteString(processProgress.FromToString);
                    w.WriteComma();

                    w.WritePropertyName(nameof(processProgress.Completed));
                    w.WriteBool(processProgress.Completed);
                    w.WriteComma();

                    w.WritePropertyName(nameof(processProgress.LastEtagSent));
                    w.WriteInteger(processProgress.LastEtagSent);
                    w.WriteComma();

                    w.WritePropertyName(nameof(processProgress.DestinationChangeVector));
                    w.WriteString(processProgress.DestinationChangeVector);
                    w.WriteComma();

                    w.WritePropertyName(nameof(processProgress.SourceChangeVector));
                    w.WriteString(processProgress.SourceChangeVector);
                    w.WriteComma();

                    w.WritePropertyName(nameof(processProgress.NumberOfDocumentsToProcess));
                    w.WriteInteger(processProgress.NumberOfDocumentsToProcess);
                    w.WriteComma();

                    w.WritePropertyName(nameof(processProgress.TotalNumberOfDocuments));
                    w.WriteInteger(processProgress.TotalNumberOfDocuments);
                    w.WriteComma();

                    w.WritePropertyName(nameof(processProgress.NumberOfDocumentTombstonesToProcess));
                    w.WriteInteger(processProgress.NumberOfDocumentTombstonesToProcess);
                    w.WriteComma();

                    w.WritePropertyName(nameof(processProgress.TotalNumberOfDocumentTombstones));
                    w.WriteInteger(processProgress.TotalNumberOfDocumentTombstones);
                    w.WriteComma();

                    w.WritePropertyName(nameof(processProgress.NumberOfCounterGroupsToProcess));
                    w.WriteInteger(processProgress.NumberOfCounterGroupsToProcess);
                    w.WriteComma();

                    w.WritePropertyName(nameof(processProgress.TotalNumberOfCounterGroups));
                    w.WriteInteger(processProgress.TotalNumberOfCounterGroups);
                    w.WriteComma();

                    w.WritePropertyName(nameof(processProgress.NumberOfTimeSeriesSegmentsToProcess));
                    w.WriteInteger(processProgress.NumberOfTimeSeriesSegmentsToProcess);
                    w.WriteComma();

                    w.WritePropertyName(nameof(processProgress.TotalNumberOfTimeSeriesSegments));
                    w.WriteInteger(processProgress.TotalNumberOfTimeSeriesSegments);
                    w.WriteComma();

                    w.WritePropertyName(nameof(processProgress.NumberOfTimeSeriesDeletedRangesToProcess));
                    w.WriteInteger(processProgress.NumberOfTimeSeriesDeletedRangesToProcess);
                    w.WriteComma();

                    w.WritePropertyName(nameof(processProgress.TotalNumberOfTimeSeriesDeletedRanges));
                    w.WriteInteger(processProgress.TotalNumberOfTimeSeriesDeletedRanges);
                    w.WriteComma();

                    w.WritePropertyName(nameof(processProgress.NumberOfRevisionsToProcess));
                    w.WriteInteger(processProgress.NumberOfRevisionsToProcess);
                    w.WriteComma();

                    w.WritePropertyName(nameof(processProgress.TotalNumberOfRevisions));
                    w.WriteInteger(processProgress.TotalNumberOfRevisions);
                    w.WriteComma();

                    w.WritePropertyName(nameof(processProgress.TotalNumberOfRevisionTombstones));
                    w.WriteInteger(processProgress.TotalNumberOfRevisionTombstones);
                    w.WriteComma();

                    w.WritePropertyName(nameof(processProgress.NumberOfAttachmentsToProcess));
                    w.WriteInteger(processProgress.NumberOfAttachmentsToProcess);
                    w.WriteComma();

                    w.WritePropertyName(nameof(processProgress.TotalNumberOfAttachments));
                    w.WriteInteger(processProgress.TotalNumberOfAttachments);
                    w.WriteComma();

                    w.WritePropertyName(nameof(processProgress.TotalNumberOfAttachmentTombstones));
                    w.WriteInteger(processProgress.TotalNumberOfAttachmentTombstones);

                    w.WriteEndObject();
                });

                w.WriteEndObject();
            });

            writer.WriteEndObject();
        }

        public static void WriteExplanation(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, DynamicQueryToIndexMatcher.Explanation explanation)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(explanation.Index));
            writer.WriteString(explanation.Index);
            writer.WriteComma();

            writer.WritePropertyName(nameof(explanation.Reason));
            writer.WriteString(explanation.Reason);

            writer.WriteEndObject();
        }

        public static async Task<(long NumberOfResults, long TotalDocumentsSizeInBytes)> WriteSuggestionQueryResultAsync(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, SuggestionQueryResult result, CancellationToken token)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(result.TotalResults));
            writer.WriteInteger(result.TotalResults);
            writer.WriteComma();

            if (result.CappedMaxResults != null)
            {
                writer.WritePropertyName(nameof(result.CappedMaxResults));
                writer.WriteInteger(result.CappedMaxResults.Value);
                writer.WriteComma();
            }

            writer.WritePropertyName(nameof(result.DurationInMs));
            writer.WriteInteger(result.DurationInMs);
            writer.WriteComma();

            if (result.IndexDefinitionRaftIndex.HasValue)
            {
                writer.WritePropertyName(nameof(result.IndexDefinitionRaftIndex));
                writer.WriteInteger(result.IndexDefinitionRaftIndex.Value);
                writer.WriteComma();
            }

            if (result.AutoIndexCreationRaftIndex.HasValue)
            {
                writer.WritePropertyName(nameof(result.AutoIndexCreationRaftIndex));
                writer.WriteInteger(result.AutoIndexCreationRaftIndex.Value);
                writer.WriteComma();
            }

            var numberOfResults = await writer.WriteQueryResultAsync(context, result, metadataOnly: false, partial: true, token)
                                                                                  .ConfigureAwait(false);

            writer.WriteEndObject();

            return numberOfResults;
        }

        public static async Task<long> WriteFacetedQueryResultAsync(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, FacetedQueryResult result, CancellationToken token)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(result.TotalResults));
            writer.WriteInteger(result.TotalResults);
            writer.WriteComma();

            if (result.CappedMaxResults != null)
            {
                writer.WritePropertyName(nameof(result.CappedMaxResults));
                writer.WriteInteger(result.CappedMaxResults.Value);
                writer.WriteComma();
            }

            writer.WritePropertyName(nameof(result.DurationInMs));
            writer.WriteInteger(result.DurationInMs);
            writer.WriteComma();

            var (numberOfResults, _) = await writer.WriteQueryResultAsync(context, result, metadataOnly: false, partial: true, token)
                                                        .ConfigureAwait(false);

            writer.WriteEndObject();

            return numberOfResults;
        }

        private static void WriteSpatialPropertyResult(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, SpatialProperty result)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(result.LatitudeProperty));
            writer.WriteString(result.LatitudeProperty);
            writer.WriteComma();

            writer.WritePropertyName(nameof(result.LongitudeProperty));
            writer.WriteString(result.LongitudeProperty);

            writer.WriteEndObject();
        }

        private static void WriteSpatialShapeResult(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, SpatialShapeBase result)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(result.Type));
            writer.WriteString(result.Type.ToString());
            writer.WriteComma();

            if (result.Type == SpatialShapeType.Circle)
            {
                var circle = result as Circle;
                writer.WritePropertyName(nameof(circle.Center));
                writer.WriteStartObject();
                writer.WritePoint(context, circle.Center);
                writer.WriteEndObject();
                writer.WriteComma();

                writer.WritePropertyName(nameof(circle.Radius));
                writer.WriteDouble(circle.Radius);
                writer.WriteComma();

                writer.WritePropertyName(nameof(circle.Units));
                writer.WriteString(circle.Units.ToString());
            }
            else if (result.Type == SpatialShapeType.Polygon)
            {
                var polygon = result as Polygon;
                writer.WriteArray(context, nameof(polygon.Vertices), polygon.Vertices,
                    (w, c, point) => w.WriteSpatialPointResult(c, point));
            }
            else
            {
                throw new NotSupportedException($"Shape type: {result.Type} is not supported.");
            }

            writer.WriteEndObject();
        }

        private static void WriteSpatialPointResult(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, Coordinates result)
        {
            writer.WriteStartObject();
            writer.WritePoint(context, result);
            writer.WriteEndObject();
        }

        private static void WritePoint(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, Coordinates result)
        {
            writer.WritePropertyName(nameof(result.Latitude));
            writer.WriteDouble(result.Latitude);
            writer.WriteComma();

            writer.WritePropertyName(nameof(result.Longitude));
            writer.WriteDouble(result.Longitude);
        }

        private static void WriteSuggestionResult(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, SuggestionResult result)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(result.Name));
            writer.WriteString(result.Name);
            writer.WriteComma();

            writer.WriteArray(nameof(result.Suggestions), result.Suggestions);

            if (result is ShardedSuggestionResult { SuggestionsWithPopularity: { Values.Count: > 0 } suggestionsPopularity })
            {
                writer.WriteComma();

                writer.WritePropertyName(Constants.Documents.Metadata.Key);
                writer.WriteStartObject();

                writer.WritePropertyName(Constants.Documents.Metadata.Sharding.Querying.SuggestionsPopularityFields);

                writer.WriteStartObject();
                writer.WritePropertyName(nameof(suggestionsPopularity.Values));

                writer.WriteStartArray();

                var firstPopularity = true;

                foreach (var popularity in suggestionsPopularity.Values)
                {
                    if (firstPopularity == false)
                        writer.WriteComma();

                    firstPopularity = false;

                    writer.WriteStartObject();

                    writer.WritePropertyName(nameof(popularity.Freq));
                    writer.WriteInteger(popularity.Freq);

                    writer.WriteComma();

                    writer.WritePropertyName(nameof(popularity.Score));
                    writer.WriteDouble(popularity.Score);

                    writer.WriteEndObject();
                }

                writer.WriteEndArray();

                writer.WriteEndObject();
                writer.WriteEndObject();

            }

            writer.WriteEndObject();
        }

        private static void WriteFacetResult(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, FacetResult result)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(result.Name));
            writer.WriteString(result.Name);
            writer.WriteComma();

            writer.WritePropertyName(nameof(result.Values));
            writer.WriteStartArray();
            var isFirstInternal = true;
            foreach (var value in result.Values)
            {
                if (isFirstInternal == false)
                    writer.WriteComma();

                isFirstInternal = false;

                writer.WriteStartObject();

                writer.WritePropertyName(nameof(value.Name));
                writer.WriteString(value.Name);
                writer.WriteComma();

                if (value.Average.HasValue)
                {
                    writer.WritePropertyName(nameof(value.Average));

                    using (var lazyStringValue = context.GetLazyString(value.Average.ToInvariantString()))
                        writer.WriteDouble(new LazyNumberValue(lazyStringValue));

                    writer.WriteComma();
                }

                if (value.Max.HasValue)
                {
                    writer.WritePropertyName(nameof(value.Max));

                    using (var lazyStringValue = context.GetLazyString(value.Max.ToInvariantString()))
                        writer.WriteDouble(new LazyNumberValue(lazyStringValue));

                    writer.WriteComma();
                }

                if (value.Min.HasValue)
                {
                    writer.WritePropertyName(nameof(value.Min));

                    using (var lazyStringValue = context.GetLazyString(value.Min.ToInvariantString()))
                        writer.WriteDouble(new LazyNumberValue(lazyStringValue));

                    writer.WriteComma();
                }

                if (value.Sum.HasValue)
                {
                    writer.WritePropertyName(nameof(value.Sum));

                    using (var lazyStringValue = context.GetLazyString(value.Sum.ToInvariantString()))
                        writer.WriteDouble(new LazyNumberValue(lazyStringValue));

                    writer.WriteComma();
                }

                writer.WritePropertyName(nameof(value.Count));
                writer.WriteInteger(value.Count);
                writer.WriteComma();

                writer.WritePropertyName(nameof(value.Range));
                writer.WriteString(value.Range);

                writer.WriteEndObject();
            }
            writer.WriteEndArray();
            writer.WriteComma();

            writer.WritePropertyName(nameof(result.RemainingHits));
            writer.WriteInteger(result.RemainingHits);
            writer.WriteComma();

            writer.WritePropertyName(nameof(result.RemainingTermsCount));
            writer.WriteInteger(result.RemainingTermsCount);
            writer.WriteComma();

            writer.WritePropertyName(nameof(result.RemainingTerms));
            writer.WriteStartArray();
            isFirstInternal = true;
            foreach (var term in result.RemainingTerms)
            {
                if (isFirstInternal == false)
                    writer.WriteComma();

                isFirstInternal = false;

                writer.WriteString(term);
            }
            writer.WriteEndArray();

            writer.WriteEndObject();
        }

        public static async Task WriteIndexEntriesQueryResultAsync(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, IndexEntriesQueryResult result, CancellationToken token)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(result.TotalResults));
            writer.WriteInteger(result.TotalResults);
            writer.WriteComma();

            if (result.CappedMaxResults != null)
            {
                writer.WritePropertyName(nameof(result.CappedMaxResults));
                writer.WriteInteger(result.CappedMaxResults.Value);
                writer.WriteComma();
            }

            writer.WritePropertyName(nameof(result.SkippedResults));
            writer.WriteInteger(result.SkippedResults);
            writer.WriteComma();

            writer.WritePropertyName(nameof(result.DurationInMs));
            writer.WriteInteger(result.DurationInMs);
            writer.WriteComma();

            await writer.WriteQueryResultAsync(context, result, metadataOnly: false, partial: true, token)
                        .ConfigureAwait(false);

            writer.WriteEndObject();
        }

        public static async Task<(long NumberOfResults, long TotalDocumentsSizeInBytes)> WriteDocumentQueryResultAsync<T>(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, QueryResultServerSide<T> result, bool metadataOnly, Action<AsyncBlittableJsonTextWriter> writeAdditionalData = null, CancellationToken token = default)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(result.TotalResults));
            writer.WriteInteger(result.TotalResults);
            writer.WriteComma();

            if (result.CappedMaxResults != null)
            {
                writer.WritePropertyName(nameof(result.CappedMaxResults));
                writer.WriteInteger(result.CappedMaxResults.Value);
                writer.WriteComma();
            }

            if (result.ScannedResults != null)
            {
                writer.WritePropertyName(nameof(result.ScannedResults));
                writer.WriteInteger(result.ScannedResults.Value);
                writer.WriteComma();
            }

            writer.WritePropertyName(nameof(result.SkippedResults));
            writer.WriteInteger(result.SkippedResults);
            writer.WriteComma();

            writer.WritePropertyName(nameof(result.DurationInMs));
            writer.WriteInteger(result.DurationInMs);
            writer.WriteComma();

            writer.WriteArray(nameof(result.IncludedPaths), result.IncludedPaths);
            writer.WriteComma();

            var numberOfResults = await writer.WriteQueryResultAsync(context, result, metadataOnly, partial: true, token)
                                                                                  .ConfigureAwait(false);

            if (result.Highlightings != null)
            {
                writer.WriteComma();

                writer.WritePropertyName(nameof(result.Highlightings));
                writer.WriteStartObject();
                var first = true;
                foreach (var kvp in result.Highlightings)
                {
                    if (first == false)
                        writer.WriteComma();
                    first = false;

                    writer.WritePropertyName(kvp.Key);
                    writer.WriteStartObject();
                    var firstInner = true;
                    foreach (var kvpInner in kvp.Value)
                    {
                        if (firstInner == false)
                            writer.WriteComma();
                        firstInner = false;

                        writer.WriteArray(kvpInner.Key, kvpInner.Value);
                    }

                    writer.WriteEndObject();
                }

                writer.WriteEndObject();
            }

            if (result.Explanations != null)
            {
                writer.WriteComma();

                writer.WritePropertyName(nameof(result.Explanations));
                writer.WriteStartObject();
                var first = true;
                foreach (var kvp in result.Explanations)
                {
                    if (first == false)
                        writer.WriteComma();
                    first = false;

                    writer.WriteArray(kvp.Key, kvp.Value);
                }

                writer.WriteEndObject();
            }

            var revisionIncludes = result.GetRevisionIncludes();
            if (revisionIncludes?.Count > 0)
            {
                writer.WriteComma();
                writer.WritePropertyName(nameof(result.RevisionIncludes));
                writer.WriteStartArray();
                await revisionIncludes.WriteIncludesAsync(writer, context, token)
                                      .ConfigureAwait(false);
                writer.WriteEndArray();
            }

            var counters = result.GetCounterIncludes();
            if (counters != null)
            {
                writer.WriteComma();
                writer.WritePropertyName(nameof(result.CounterIncludes));
                await counters.WriteIncludesAsync(writer, context, token)
                              .ConfigureAwait(false);

                writer.WriteComma();
                writer.WritePropertyName(nameof(result.IncludedCounterNames));
                writer.WriteIncludedCounterNames(result.IncludedCounterNames);
            }

            var timeSeries = result.GetTimeSeriesIncludes();
            if (timeSeries != null)
            {
                writer.WriteComma();
                writer.WritePropertyName(nameof(result.TimeSeriesIncludes));
                await timeSeries.WriteIncludesAsync(writer, context, token)
                                .ConfigureAwait(false);
            }

            if (result.TimeSeriesFields != null)
            {
                writer.WriteComma();
                writer.WriteArray(nameof(result.TimeSeriesFields), result.TimeSeriesFields);
            }

            var compareExchangeValues = result.GetCompareExchangeValueIncludes();
            if (compareExchangeValues != null)
            {
                writer.WriteComma();
                writer.WritePropertyName(nameof(result.CompareExchangeValueIncludes));
                await writer.WriteCompareExchangeValuesAsync(compareExchangeValues, token)
                            .ConfigureAwait(false);
            }

            var spatialProperties = result.SpatialProperties;
            if (spatialProperties != null)
            {
                writer.WriteComma();
                writer.WriteArray(context, nameof(result.SpatialProperties), spatialProperties,
                    (w, c, spatialProperty) => w.WriteSpatialPropertyResult(c, spatialProperty));
            }

            var spatialShapes = result.SpatialShapes;
            if (spatialShapes != null)
            {
                writer.WriteComma();
                writer.WriteArray(context, nameof(result.SpatialShapes), spatialShapes,
                    (w, c, spatialShape) => w.WriteSpatialShapeResult(c, spatialShape));
            }

            if (result.IndexDefinitionRaftIndex.HasValue)
            {
                writer.WriteComma();
                writer.WritePropertyName(nameof(result.IndexDefinitionRaftIndex));
                writer.WriteInteger(result.IndexDefinitionRaftIndex.Value);
            }

            if (result.AutoIndexCreationRaftIndex.HasValue)
            {
                writer.WriteComma();
                writer.WritePropertyName(nameof(result.AutoIndexCreationRaftIndex));
                writer.WriteInteger(result.AutoIndexCreationRaftIndex.Value);
            }

            writeAdditionalData?.Invoke(writer);

            writer.WriteEndObject();
            return numberOfResults;
        }


        public static void WritePutIndexResponse(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, IEnumerable<PutIndexResult> createdIndexes)
        {
            writer.WriteStartObject();

            writer.WriteArray(context, "Results", createdIndexes, (w, c, index) =>
            {
                w.WriteStartObject();
                w.WritePropertyName(nameof(PutIndexResult.Index));
                w.WriteString(index.Index);
                w.WriteComma();
                w.WritePropertyName(nameof(PutIndexResult.RaftCommandIndex));
                w.WriteInteger(index.RaftCommandIndex);
                w.WriteEndObject();
            });

            writer.WriteEndObject();
        }

        public static void WriteIncludedCounterNames(this AbstractBlittableJsonTextWriter writer, Dictionary<string, string[]> includedCounterNames)
        {
            writer.WriteStartObject();

            var first = true;
            foreach (var kvp in includedCounterNames)
            {
                if (first == false)
                    writer.WriteComma();

                first = false;

                writer.WriteArray(kvp.Key, kvp.Value);
            }

            writer.WriteEndObject();
        }

        private static async Task<(long NumberOfResults, long TotalDocumentsSizeInBytes)> WriteQueryResultAsync<TResult, TInclude>(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, QueryResultBase<TResult, TInclude> result, bool metadataOnly, bool partial = false, CancellationToken token = default)
        {
            long numberOfResults;
            long totalDocumentsSizeInBytes = -1; // Size of facet is constant - no need to count that - similar situation happens on suggestions

            if (partial == false)
                writer.WriteStartObject();
            writer.WritePropertyName(nameof(result.IndexName));
            writer.WriteString(result.IndexName);
            writer.WriteComma();

            var results = (object)result.Results;
            if (results is List<Document> documents)
            {
                writer.WritePropertyName(nameof(result.Results));
                (numberOfResults, totalDocumentsSizeInBytes) = await writer.WriteDocumentsAsync(context, documents, metadataOnly, token)
                                                                           .ConfigureAwait(false);
                writer.WriteComma();
            }
            else if (results is List<BlittableJsonReaderObject> objects)
            {
                writer.WritePropertyName(nameof(result.Results));
                (numberOfResults, totalDocumentsSizeInBytes) = await writer.WriteObjectsAsync(context, objects, token)
                                                                           .ConfigureAwait(false);
                writer.WriteComma();
            }
            else if (results is List<FacetResult> facets)
            {
                numberOfResults = facets.Count;
                writer.WriteArray(context, nameof(result.Results), facets, (w, c, facet) => w.WriteFacetResult(c, facet));
                writer.WriteComma();
                await writer.MaybeFlushAsync(token)
                            .ConfigureAwait(false);
            }
            else if (results is List<SuggestionResult> suggestions)
            {
                numberOfResults = suggestions.Count;
                writer.WriteArray(context, nameof(result.Results), suggestions, (w, c, suggestion) => w.WriteSuggestionResult(c, suggestion));
                writer.WriteComma();
                await writer.MaybeFlushAsync(token)
                            .ConfigureAwait(false);
            }
            else
                throw new NotSupportedException($"Cannot write query result of '{typeof(TResult)}' type in '{result.GetType()}'.");

            var includes = (object)result.Includes;
            if (includes is List<Document> includeDocuments)
            {
                writer.WritePropertyName(nameof(result.Includes));
                await writer.WriteIncludesAsync(context, includeDocuments, token)
                            .ConfigureAwait(false);
                writer.WriteComma();
            }
            else if (includes is List<BlittableJsonReaderObject> includeObjects)
            {
                writer.WritePropertyName(nameof(result.Includes));
                await writer.WriteIncludesAsync(includeObjects, token)
                            .ConfigureAwait(false);
                writer.WriteComma();
            }
            else
                throw new NotSupportedException($"Cannot write query includes of '{includes.GetType()}' type in '{result.GetType()}'.");

            writer.WritePropertyName(nameof(result.IndexTimestamp));
            writer.WriteString(result.IndexTimestamp.GetDefaultRavenFormat());
            writer.WriteComma();

            writer.WritePropertyName(nameof(result.LastQueryTime));
            writer.WriteString(result.LastQueryTime.GetDefaultRavenFormat());
            writer.WriteComma();

            writer.WritePropertyName(nameof(result.IsStale));
            writer.WriteBool(result.IsStale);
            writer.WriteComma();

            writer.WritePropertyName(nameof(result.ResultEtag));
            writer.WriteInteger(result.ResultEtag);
            writer.WriteComma();

            writer.WritePropertyName(nameof(result.NodeTag));
            writer.WriteString(result.NodeTag);

            if (result.Timings != null)
            {
                writer.WriteComma();
                writer.WritePropertyName(nameof(result.Timings));
                writer.WriteQueryTimings(context, result.Timings, attachQueryPlan: true);
            }

            if (partial == false)
                writer.WriteEndObject();

            return (numberOfResults, totalDocumentsSizeInBytes);
        }

        private static void WriteQueryTimings(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, QueryTimings queryTimings, bool attachQueryPlan)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(QueryTimings.DurationInMs));
            writer.WriteInteger(queryTimings.DurationInMs);
            if (attachQueryPlan && queryTimings.QueryPlan != null)
            {
                writer.WriteComma();
                writer.WritePropertyName(nameof(queryTimings.QueryPlan));
                var value = queryTimings.QueryPlan.ToJson();
                writer.WriteObject(context.ReadObject(value, nameof(queryTimings.QueryPlan)));
            }
            
            writer.WriteComma();
            writer.WritePropertyName(nameof(QueryTimings.Timings));
            if (queryTimings.Timings != null)
            {
                writer.WriteStartObject();
                var first = true;

                foreach (var kvp in queryTimings.Timings)
                {
                    if (first == false)
                        writer.WriteComma();

                    first = false;

                    writer.WritePropertyName(kvp.Key);
                    writer.WriteQueryTimings(context, kvp.Value, attachQueryPlan: false);
                }

                writer.WriteEndObject();
            }
            else
                writer.WriteNull();

            writer.WriteEndObject();
        }

        public static void WriteTermsQueryResult(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, TermsQueryResultServerSide queryResult)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(queryResult.IndexName));
            writer.WriteString(queryResult.IndexName);
            writer.WriteComma();

            writer.WritePropertyName(nameof(queryResult.ResultEtag));
            writer.WriteInteger(queryResult.ResultEtag);
            writer.WriteComma();

            writer.WriteArray(nameof(queryResult.Terms), queryResult.Terms);

            writer.WriteEndObject();
        }

        private static void WriteIndexingPerformanceStats(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, IndexingPerformanceStats stats)
        {
            var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(stats);
            writer.WriteObject(context.ReadObject(djv, "index/performance"));
        }

        private static void WriteEtlPerformanceStats(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, EtlPerformanceStats stats)
        {
            var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(stats);
            writer.WriteObject(context.ReadObject(djv, "etl/performance"));
        }

        private static void WriteQueueSinkPerformanceStats(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, QueueSinkPerformanceStats stats)
        {
            var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(stats);
            writer.WriteObject(context.ReadObject(djv, "queue-sink/performance"));
        }

        public static void WriteSubscriptionBatchPerformanceStats(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, SubscriptionBatchPerformanceStats batchStats)
        {
            var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(batchStats);
            writer.WriteObject(context.ReadObject(djv, "subscriptionBatch/performance"));
        }

        public static void WriteSubscriptionConnectionPerformanceStats(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, SubscriptionConnectionPerformanceStats connectionStats)
        {
            var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(connectionStats);
            writer.WriteObject(context.ReadObject(djv, "subscriptionConnection/performance"));
        }

        public static void WriteIndexQuery<TWriter>(this TWriter writer, JsonOperationContext context, IIndexQuery query)
            where TWriter : IBlittableJsonTextWriter
        {
            var indexQuery = query as IndexQueryServerSide;
            if (indexQuery != null)
            {
                writer.WriteIndexQuery(context, indexQuery);
                return;
            }

            throw new NotSupportedException($"Not supported query type: {query.GetType()}");
        }

        private static void WriteIndexQuery<TWriter>(this TWriter writer, JsonOperationContext context, IndexQueryServerSide query)
            where TWriter : IBlittableJsonTextWriter
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(query.PageSize));
            writer.WriteInteger(query.PageSize);
            writer.WriteComma();

            writer.WritePropertyName(nameof(query.Query));
            if (query.Query != null)
                writer.WriteString(query.Query);
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(nameof(query.SkipDuplicateChecking));
            writer.WriteBool(query.SkipDuplicateChecking);
            writer.WriteComma();

            writer.WritePropertyName(nameof(query.SkipStatistics));
            writer.WriteBool(query.SkipStatistics);
            writer.WriteComma();

            writer.WritePropertyName(nameof(query.Start));
            writer.WriteInteger(query.Start);
            writer.WriteComma();

            writer.WritePropertyName(nameof(query.WaitForNonStaleResults));
            writer.WriteBool(query.WaitForNonStaleResults);
            writer.WriteComma();

            writer.WritePropertyName(nameof(query.WaitForNonStaleResultsTimeout));
            if (query.WaitForNonStaleResultsTimeout.HasValue)
                writer.WriteString(query.WaitForNonStaleResultsTimeout.Value.ToString());
            else
                writer.WriteNull();

            writer.WriteEndObject();
        }

        public static void WriteDetailedDatabaseStatistics(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, DetailedDatabaseStatistics statistics)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(statistics.CountOfIdentities));
            writer.WriteInteger(statistics.CountOfIdentities);
            writer.WriteComma();

            writer.WritePropertyName(nameof(statistics.CountOfCompareExchange));
            writer.WriteInteger(statistics.CountOfCompareExchange);
            writer.WriteComma();

            writer.WritePropertyName(nameof(statistics.CountOfCompareExchangeTombstones));
            writer.WriteInteger(statistics.CountOfCompareExchangeTombstones);
            writer.WriteComma();

            writer.WritePropertyName(nameof(statistics.CountOfTimeSeriesDeletedRanges));
            writer.WriteInteger(statistics.CountOfTimeSeriesDeletedRanges);
            writer.WriteComma();

            WriteDatabaseStatisticsInternal(writer, statistics);

            writer.WriteEndObject();
        }

        public static void WriteEssentialDatabaseStatistics<TWriter>(this TWriter writer, JsonOperationContext context, EssentialDatabaseStatistics statistics)
            where TWriter : IBlittableJsonTextWriter
        {
            writer.WriteStartObject();

            WriteEssentialDatabaseStatisticsInternal(writer, statistics);

            writer.WriteEndObject();
        }

        public static void WriteDatabaseStatistics<TWriter>(this TWriter writer, JsonOperationContext context, DatabaseStatistics statistics)
            where TWriter : IBlittableJsonTextWriter
        {
            writer.WriteStartObject();

            WriteDatabaseStatisticsInternal(writer, statistics);

            writer.WriteEndObject();
        }

        private static void WriteDatabaseStatisticsInternal<TWriter>(this TWriter writer, DatabaseStatistics statistics)
            where TWriter : IBlittableJsonTextWriter
        {
            WriteEssentialDatabaseStatisticsInternal(writer, statistics);
            writer.WriteComma();

            writer.WritePropertyName(nameof(statistics.CountOfUniqueAttachments));
            writer.WriteInteger(statistics.CountOfUniqueAttachments);
            writer.WriteComma();

            writer.WritePropertyName(nameof(statistics.DatabaseChangeVector));
            writer.WriteString(statistics.DatabaseChangeVector);
            writer.WriteComma();

            writer.WritePropertyName(nameof(statistics.DatabaseId));
            writer.WriteString(statistics.DatabaseId);
            writer.WriteComma();

            writer.WritePropertyName(nameof(statistics.NumberOfTransactionMergerQueueOperations));
            writer.WriteInteger(statistics.NumberOfTransactionMergerQueueOperations);
            writer.WriteComma();

            writer.WritePropertyName(nameof(statistics.Is64Bit));
            writer.WriteBool(statistics.Is64Bit);
            writer.WriteComma();

            writer.WritePropertyName(nameof(statistics.Pager));
            writer.WriteString(statistics.Pager);
            writer.WriteComma();

            writer.WritePropertyName(nameof(statistics.LastDocEtag));
            if (statistics.LastDocEtag.HasValue)
                writer.WriteInteger(statistics.LastDocEtag.Value);
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(nameof(statistics.LastDatabaseEtag));
            if (statistics.LastDatabaseEtag.HasValue)
                writer.WriteInteger(statistics.LastDatabaseEtag.Value);
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(nameof(statistics.LastIndexingTime));
            if (statistics.LastIndexingTime.HasValue)
                writer.WriteDateTime(statistics.LastIndexingTime.Value, isUtc: true);
            else
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

        private static void WriteEssentialDatabaseStatisticsInternal<TIndexInformation, TWriter>(this TWriter writer, AbstractDatabaseStatistics<TIndexInformation> statistics)
            where TIndexInformation : EssentialIndexInformation
            where TWriter : IBlittableJsonTextWriter
        {
            writer.WritePropertyName(nameof(statistics.CountOfIndexes));
            writer.WriteInteger(statistics.CountOfIndexes);
            writer.WriteComma();

            writer.WritePropertyName(nameof(statistics.CountOfDocuments));
            writer.WriteInteger(statistics.CountOfDocuments);
            writer.WriteComma();

            writer.WritePropertyName(nameof(statistics.CountOfRevisionDocuments));
            writer.WriteInteger(statistics.CountOfRevisionDocuments);
            writer.WriteComma();

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

            writer.WritePropertyName(nameof(statistics.Indexes));
            writer.WriteStartArray();
            var isFirstInternal = true;
            foreach (var index in statistics.Indexes)
            {
                if (isFirstInternal == false)
                    writer.WriteComma();

                isFirstInternal = false;

                switch (index)
                {
                    case IndexInformation indexInformation:
                        WriteIndexInformation(writer, indexInformation);
                        break;
                    case EssentialIndexInformation basicIndexInformation:
                        WriteBasicIndexInformation(writer, basicIndexInformation);
                        break;
                    default:
                        throw new NotSupportedException("Unknown index information.");
                }
            }

            writer.WriteEndArray();

            void WriteIndexInformation(TWriter w, IndexInformation index)
            {
                w.WriteStartObject();

                w.WritePropertyName(nameof(index.IsStale));
                w.WriteBool(index.IsStale);
                w.WriteComma();

                w.WritePropertyName(nameof(index.State));
                w.WriteString(index.State.ToString());
                w.WriteComma();

                w.WritePropertyName(nameof(index.LastIndexingTime));
                if (index.LastIndexingTime.HasValue)
                    w.WriteDateTime(index.LastIndexingTime.Value, isUtc: true);
                else
                    w.WriteNull();

                w.WriteComma();

                WriteBasicIndexInformationInternal(w, index as TIndexInformation);

                w.WriteEndObject();
            }

            void WriteBasicIndexInformation(TWriter w, EssentialIndexInformation index)
            {
                w.WriteStartObject();

                w.WritePropertyName(nameof(index.Name));
                w.WriteString(index.Name);
                w.WriteComma();

                w.WritePropertyName(nameof(index.LockMode));
                w.WriteString(index.LockMode.ToString());
                w.WriteComma();

                w.WritePropertyName(nameof(index.Priority));
                w.WriteString(index.Priority.ToString());
                w.WriteComma();

                w.WritePropertyName(nameof(index.Type));
                w.WriteString(index.Type.ToString());
                w.WriteComma();

                w.WritePropertyName(nameof(index.SourceType));
                w.WriteString(index.SourceType.ToString());
                w.WriteComma();

                WriteBasicIndexInformationInternal(w, index as TIndexInformation);

                w.WriteEndObject();
            }

            static void WriteBasicIndexInformationInternal(TWriter w, TIndexInformation index)
            {
                w.WritePropertyName(nameof(index.Name));
                w.WriteString(index.Name);
                w.WriteComma();

                w.WritePropertyName(nameof(index.LockMode));
                w.WriteString(index.LockMode.ToString());
                w.WriteComma();

                w.WritePropertyName(nameof(index.Priority));
                w.WriteString(index.Priority.ToString());
                w.WriteComma();

                w.WritePropertyName(nameof(index.Type));
                w.WriteString(index.Type.ToString());
                w.WriteComma();

                w.WritePropertyName(nameof(index.SourceType));
                w.WriteString(index.SourceType.ToString());
            }
        }

        public static void WriteIndexDefinition(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, IndexDefinition indexDefinition, long? indexVersion = null)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(indexDefinition.Name));
            writer.WriteString(indexDefinition.Name);
            writer.WriteComma();

            if (indexVersion != null)
            {
                writer.WritePropertyName(nameof(IndexDefinitionBaseServerSide.Version));
                writer.WriteInteger(indexVersion.Value);
                writer.WriteComma();
            }

            writer.WritePropertyName(nameof(indexDefinition.SourceType));
            writer.WriteString(indexDefinition.SourceType.ToString());
            writer.WriteComma();

            writer.WritePropertyName(nameof(indexDefinition.Type));
            writer.WriteString(indexDefinition.Type.ToString());
            writer.WriteComma();

            writer.WritePropertyName(nameof(indexDefinition.LockMode));
            if (indexDefinition.LockMode.HasValue)
                writer.WriteString(indexDefinition.LockMode.ToString());
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(nameof(indexDefinition.ArchivedDataProcessingBehavior));
            if (indexDefinition.ArchivedDataProcessingBehavior.HasValue)
                writer.WriteString(indexDefinition.ArchivedDataProcessingBehavior?.ToString());
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(nameof(indexDefinition.Priority));
            if (indexDefinition.Priority.HasValue)
                writer.WriteString(indexDefinition.Priority.ToString());
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(nameof(indexDefinition.State));
            if (indexDefinition.State.HasValue)
                writer.WriteString(indexDefinition.State.ToString());
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(nameof(indexDefinition.OutputReduceToCollection));
            writer.WriteString(indexDefinition.OutputReduceToCollection);
            writer.WriteComma();

            writer.WritePropertyName(nameof(indexDefinition.DeploymentMode));
            if (indexDefinition.DeploymentMode.HasValue)
                writer.WriteString(indexDefinition.DeploymentMode.Value.ToString());
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(nameof(indexDefinition.ReduceOutputIndex));

            if (indexDefinition.ReduceOutputIndex.HasValue)
                writer.WriteInteger(indexDefinition.ReduceOutputIndex.Value);
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(nameof(indexDefinition.PatternForOutputReduceToCollectionReferences));
            writer.WriteString(indexDefinition.PatternForOutputReduceToCollectionReferences);
            writer.WriteComma();

            writer.WritePropertyName(nameof(indexDefinition.PatternReferencesCollectionName));
            writer.WriteString(indexDefinition.PatternReferencesCollectionName);
            writer.WriteComma();

            writer.WritePropertyName(nameof(indexDefinition.Configuration));
            writer.WriteStartObject();
            var isFirstInternal = true;
            foreach (var kvp in indexDefinition.Configuration)
            {
                if (isFirstInternal == false)
                    writer.WriteComma();

                isFirstInternal = false;

                writer.WritePropertyName(kvp.Key);
                writer.WriteString(kvp.Value);
            }
            writer.WriteEndObject();
            writer.WriteComma();

            if (indexDefinition.CompoundFields is { Count: > 0 })
            {
                writer.WritePropertyName(nameof(indexDefinition.CompoundFields));
                writer.WriteStartArray();
                var first = true;
                foreach (var fields in indexDefinition.CompoundFields)
                {
                    if (first == false)
                    {
                        writer.WriteComma();
                    }
                    first = false;
                    writer.WriteArrayValue(fields);
                }
                writer.WriteEndArray();
                writer.WriteComma();
            }

            writer.WritePropertyName(nameof(indexDefinition.AdditionalSources));
            writer.WriteStartObject();
            isFirstInternal = true;
            foreach (var kvp in indexDefinition.AdditionalSources)
            {
                if (isFirstInternal == false)
                    writer.WriteComma();

                isFirstInternal = false;

                writer.WritePropertyName(kvp.Key);
                writer.WriteString(kvp.Value);
            }
            writer.WriteEndObject();
            writer.WriteComma();

            writer.WriteArray(context, nameof(indexDefinition.AdditionalAssemblies), indexDefinition.AdditionalAssemblies, (w, c, a) => a.WriteTo(w));
            writer.WriteComma();

#if FEATURE_TEST_INDEX
            writer.WritePropertyName(nameof(indexDefinition.IsTestIndex));
            writer.WriteBool(indexDefinition.IsTestIndex);
            writer.WriteComma();
#endif

            writer.WritePropertyName(nameof(indexDefinition.Reduce));
            if (string.IsNullOrWhiteSpace(indexDefinition.Reduce) == false)
                writer.WriteString(indexDefinition.Reduce);
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(nameof(indexDefinition.Maps));
            writer.WriteStartArray();
            isFirstInternal = true;
            foreach (var map in indexDefinition.Maps)
            {
                if (isFirstInternal == false)
                    writer.WriteComma();

                isFirstInternal = false;
                writer.WriteString(map);
            }
            writer.WriteEndArray();
            writer.WriteComma();

            writer.WritePropertyName(nameof(indexDefinition.Fields));
            writer.WriteStartObject();
            isFirstInternal = true;
            foreach (var kvp in indexDefinition.Fields)
            {
                if (isFirstInternal == false)
                    writer.WriteComma();

                isFirstInternal = false;
                writer.WritePropertyName(kvp.Key);
                if (kvp.Value != null)
                    writer.WriteIndexFieldOptions(context, kvp.Value, removeAnalyzers: false);
                else
                    writer.WriteNull();
            }
            writer.WriteEndObject();

            writer.WriteEndObject();
        }

        public static void WriteIndexProgress(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, IndexProgress progress)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(progress.IsStale));
            writer.WriteBool(progress.IsStale);
            writer.WriteComma();

            writer.WritePropertyName(nameof(progress.IndexRunningStatus));
            writer.WriteString(progress.IndexRunningStatus.ToString());
            writer.WriteComma();

            writer.WritePropertyName(nameof(progress.ProcessedPerSecond));
            writer.WriteDouble(progress.ProcessedPerSecond);
            writer.WriteComma();

            writer.WritePropertyName(nameof(progress.Collections));
            if (progress.Collections == null)
            {
                writer.WriteNull();
            }
            else
            {
                writer.WriteStartObject();
                var isFirst = true;
                foreach (var kvp in progress.Collections)
                {
                    if (isFirst == false)
                        writer.WriteComma();

                    isFirst = false;

                    writer.WritePropertyName(kvp.Key);

                    writer.WriteStartObject();

                    writer.WritePropertyName(nameof(kvp.Value.LastProcessedItemEtag));
                    writer.WriteInteger(kvp.Value.LastProcessedItemEtag);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(kvp.Value.LastProcessedTombstoneEtag));
                    writer.WriteInteger(kvp.Value.LastProcessedTombstoneEtag);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(kvp.Value.LastProcessedTimeSeriesDeletedRangeEtag));
                    writer.WriteInteger(kvp.Value.LastProcessedTimeSeriesDeletedRangeEtag);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(kvp.Value.NumberOfItemsToProcess));
                    writer.WriteInteger(kvp.Value.NumberOfItemsToProcess);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(kvp.Value.NumberOfTombstonesToProcess));
                    writer.WriteInteger(kvp.Value.NumberOfTombstonesToProcess);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(kvp.Value.NumberOfTimeSeriesDeletedRangesToProcess));
                    writer.WriteInteger(kvp.Value.NumberOfTimeSeriesDeletedRangesToProcess);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(kvp.Value.TotalNumberOfItems));
                    writer.WriteInteger(kvp.Value.TotalNumberOfItems);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(kvp.Value.TotalNumberOfTombstones));
                    writer.WriteInteger(kvp.Value.TotalNumberOfTombstones);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(kvp.Value.TotalNumberOfTimeSeriesDeletedRanges));
                    writer.WriteInteger(kvp.Value.TotalNumberOfTimeSeriesDeletedRanges);

                    writer.WriteEndObject();
                }
                writer.WriteEndObject();
            }

            writer.WriteComma();

            writer.WritePropertyName(nameof(progress.Name));
            writer.WriteString(progress.Name);
            writer.WriteComma();

            writer.WritePropertyName(nameof(progress.Type));
            writer.WriteString(progress.Type.ToString());
            writer.WriteComma();

            writer.WritePropertyName(nameof(progress.SourceType));
            writer.WriteString(progress.SourceType.ToString());

            if (progress.IndexRollingStatus != null)
            {
                writer.WriteComma();
                writer.WritePropertyName(nameof(progress.IndexRollingStatus));
                writer.WriteObject(context.ReadObject(progress.IndexRollingStatus.ToJson(), "rollingProgress"));
            }

            writer.WriteEndObject();
        }

        public static void WriteIndexesStats<TWriter>(this TWriter writer, JsonOperationContext context, IndexStats[] indexesStats)
            where TWriter : IBlittableJsonTextWriter
        {
            writer.WriteStartObject();

            writer.WriteArray(context, "Results", indexesStats, (w, c, stats) =>
            {
                var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(stats);
                writer.WriteObject(context.ReadObject(djv, "index/stats"));
            });

            writer.WriteEndObject();
        }

        public static void WriteIndexErrorCounts(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, IEnumerable<GetIndexErrorsCountCommand.IndexErrorsCount> indexErrorCounts)
        {
            writer.WriteStartObject();
            writer.WriteArray(context, "Results", indexErrorCounts, (w, c, index) =>
            {
                w.WriteStartObject();

                w.WritePropertyName(nameof(index.Name));
                w.WriteString(index.Name);
                w.WriteComma();

                w.WriteArray(c, nameof(index.Errors), index.Errors, (ew, ec, error) =>
                {
                    ew.WriteStartObject();

                    ew.WritePropertyName(nameof(error.Action));
                    ew.WriteString(error.Action);
                    ew.WriteComma();

                    ew.WritePropertyName(nameof(error.NumberOfErrors));
                    ew.WriteInteger(error.NumberOfErrors);
                    ew.WriteEndObject();
                });

                w.WriteEndObject();
            });
            writer.WriteEndObject();
        }

        public static void WriteIndexErrors<TWriter>(this TWriter writer, JsonOperationContext context, IEnumerable<IndexErrors> indexErrors)
            where TWriter : IBlittableJsonTextWriter
        {
            writer.WriteStartObject();
            writer.WriteArray(context, "Results", indexErrors, (w, c, index) =>
            {
                w.WriteStartObject();
                w.WritePropertyName("Name");
                w.WriteString(index.Name);
                w.WriteComma();
                w.WriteArray(c, "Errors", index.Errors, (ew, ec, error) =>
                {
                    ew.WriteStartObject();
                    ew.WritePropertyName(nameof(error.Timestamp));
                    ew.WriteDateTime(error.Timestamp, isUtc: true);
                    ew.WriteComma();

                    ew.WritePropertyName(nameof(error.Document));
                    ew.WriteString(error.Document);
                    ew.WriteComma();

                    ew.WritePropertyName(nameof(error.Action));
                    ew.WriteString(error.Action);
                    ew.WriteComma();

                    ew.WritePropertyName(nameof(error.Error));
                    ew.WriteString(error.Error);
                    ew.WriteEndObject();
                });
                w.WriteEndObject();
            });
            writer.WriteEndObject();
        }

        private static void WriteIndexFieldOptions(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, IndexFieldOptions options, bool removeAnalyzers)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(options.Analyzer));
            if (string.IsNullOrWhiteSpace(options.Analyzer) == false && !removeAnalyzers)
                writer.WriteString(options.Analyzer);
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(nameof(options.Indexing));
            if (options.Indexing.HasValue)
                writer.WriteString(options.Indexing.ToString());
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(nameof(options.Storage));
            if (options.Storage.HasValue)
                writer.WriteString(options.Storage.ToString());
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(nameof(options.Suggestions));
            if (options.Suggestions.HasValue)
                writer.WriteBool(options.Suggestions.Value);
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(nameof(options.TermVector));
            if (options.TermVector.HasValue)
                writer.WriteString(options.TermVector.ToString());
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(nameof(options.Spatial));
            if (options.Spatial != null)
            {
                writer.WriteStartObject();

                writer.WritePropertyName(nameof(options.Spatial.Type));
                writer.WriteString(options.Spatial.Type.ToString());
                writer.WriteComma();

                writer.WritePropertyName(nameof(options.Spatial.MaxTreeLevel));
                writer.WriteInteger(options.Spatial.MaxTreeLevel);
                writer.WriteComma();

                writer.WritePropertyName(nameof(options.Spatial.MaxX));
                LazyStringValue lazyStringValue;
                using (lazyStringValue = context.GetLazyString(CharExtensions.ToInvariantString(options.Spatial.MaxX)))
                    writer.WriteDouble(new LazyNumberValue(lazyStringValue));
                writer.WriteComma();

                writer.WritePropertyName(nameof(options.Spatial.MaxY));
                using (lazyStringValue = context.GetLazyString(CharExtensions.ToInvariantString(options.Spatial.MaxY)))
                    writer.WriteDouble(new LazyNumberValue(lazyStringValue));
                writer.WriteComma();

                writer.WritePropertyName(nameof(options.Spatial.MinX));
                using (lazyStringValue = context.GetLazyString(CharExtensions.ToInvariantString(options.Spatial.MinX)))
                    writer.WriteDouble(new LazyNumberValue(lazyStringValue));
                writer.WriteComma();

                writer.WritePropertyName(nameof(options.Spatial.MinY));
                using (lazyStringValue = context.GetLazyString(CharExtensions.ToInvariantString(options.Spatial.MinY)))
                    writer.WriteDouble(new LazyNumberValue(lazyStringValue));
                writer.WriteComma();

                writer.WritePropertyName(nameof(options.Spatial.Strategy));
                writer.WriteString(options.Spatial.Strategy.ToString());
                writer.WriteComma();

                writer.WritePropertyName(nameof(options.Spatial.Units));
                writer.WriteString(options.Spatial.Units.ToString());

                writer.WriteEndObject();
            }
            else
                writer.WriteNull();

            writer.WriteEndObject();
        }

        public static Task<(long NumberOfResults, long TotalDocumentsSizeInBytes)> WriteDocumentsAsync(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, IEnumerable<Document> documents, bool metadataOnly, CancellationToken token)
        {
            return WriteDocumentsAsync(writer, context, documents.GetEnumerator(), metadataOnly, token);
        }

        public static async Task<(long NumberOfResults, long TotalDocumentsSizeInBytes)> WriteDocumentsAsync(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, IEnumerator<Document> documents, bool metadataOnly, CancellationToken token)
        {
            long numberOfResults = 0;
            long totalDocumentsSizeInBytes = 0;

            writer.WriteStartArray();

            var first = true;
            while (documents.MoveNext())
            {
                numberOfResults++;

                if (documents.Current != null)
                    totalDocumentsSizeInBytes += documents.Current.Data.Size;

                if (first == false)
                    writer.WriteComma();
                first = false;

                WriteDocument(writer, context, documents.Current, metadataOnly);
                await writer.MaybeFlushAsync(token)
                            .ConfigureAwait(false);
            }

            writer.WriteEndArray();
            return (numberOfResults, totalDocumentsSizeInBytes);
        }

        public static void WriteContinuationToken(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, ContinuationToken continuation)
        {
            writer.WritePropertyName(ContinuationToken.PropertyName);
            writer.WriteString(continuation.ToBase64(context));
        }

        public static async Task<(long NumberOfResults, long TotalDocumentsSizeInBytes)> WriteDocumentsAsync(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, IAsyncEnumerable<Document> documents, bool metadataOnly, CancellationToken token)
        {
            long numberOfResults = 0;
            long totalDocumentsSizeInBytes = 0;

            writer.WriteStartArray();

            var first = true;
            await foreach (var document in documents.WithCancellation(token))
            {
                numberOfResults++;

                if (document != null)
                    totalDocumentsSizeInBytes += document.Data.Size;

                if (first == false)
                    writer.WriteComma();
                first = false;

                WriteDocument(writer, context, document, metadataOnly);
                await writer.FlushAsync(token)
                            .ConfigureAwait(false); // we must flush here because we dispose the document
            }

            writer.WriteEndArray();
            return (numberOfResults, totalDocumentsSizeInBytes);
        }

        public static void WriteDocument(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, Document document, bool metadataOnly, Func<LazyStringValue, bool> filterMetadataProperty = null)
        {
            if (document == null)
            {
                writer.WriteNull();
                return;
            }

            if (document == Document.ExplicitNull)
            {
                writer.WriteNull();
                return;
            }

            if (document.Data == null && document.NonPersistentFlags.Contain(NonPersistentDocumentFlags.AllowDataAsNull))
            {
                writer.WriteNull();
                return;
            }

            // Explicitly not disposing it, a single document can be
            // used multiple times in a single query, for example, due to projections
            // so we will let the context handle it, rather than handle it directly ourselves
            //using (document.Data)
            {
                if (metadataOnly == false)
                    writer.WriteDocumentInternal(context, document, filterMetadataProperty);
                else
                    writer.WriteDocumentMetadata(context, document, filterMetadataProperty);
            }
        }

        public static async ValueTask<(long Count, long SizeInBytes)> WriteIncludesAsync(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, IEnumerable<Document> includes, CancellationToken token = default)
        {
            writer.WriteStartObject();

            long count = 0, sizeInBytes = 0;

            var first = true;
            foreach (var document in includes)
            {
                count++;
                sizeInBytes += document.Data?.Size ?? 0;

                if (first == false)
                    writer.WriteComma();
                first = false;

                if (document is IncludeDocumentsCommand.ConflictDocument conflict)
                {
                    writer.WritePropertyName(conflict.Id);
                    WriteConflict(writer, conflict);
                    await writer.MaybeFlushAsync(token)
                                .ConfigureAwait(false);
                    continue;
                }

                writer.WritePropertyName(document.Id);
                WriteDocument(writer, context, metadataOnly: false, document: document);
                await writer.MaybeFlushAsync(token)
                            .ConfigureAwait(false);
            }

            writer.WriteEndObject();

            return (count, sizeInBytes);
        }

        public static async ValueTask<(long Count, long SizeInBytes)> WriteIncludesAsync(this AsyncBlittableJsonTextWriter writer, IEnumerable<BlittableJsonReaderObject> includes, CancellationToken token = default)
        {
            writer.WriteStartObject();

            long count = 0, sizeInBytes = 0;

            var first = true;
            foreach (var includeDoc in includes)
            {
                count++;
                sizeInBytes += includeDoc.Size;

                if (first == false)
                    writer.WriteComma();
                first = false;

                var metadata = includeDoc.GetMetadata();
                writer.WritePropertyName(metadata.GetId());

                if (metadata.TryGet(Constants.Documents.Metadata.Sharding.Subscription.NonPersistentFlags, out NonPersistentDocumentFlags flag) && flag.HasFlag(NonPersistentDocumentFlags.AllowDataAsNull))
                    writer.WriteNull();
                else
                    writer.WriteObject(includeDoc);

                await writer.MaybeOuterFlushAsync()
                            .ConfigureAwait(false);
            }

            writer.WriteEndObject();

            return (count, sizeInBytes);
        }

        private static void WriteConflict(AbstractBlittableJsonTextWriter writer, IncludeDocumentsCommand.ConflictDocument conflict)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(Constants.Documents.Metadata.Key);
            writer.WriteStartObject();

            writer.WritePropertyName(Constants.Documents.Metadata.Id);
            writer.WriteString(conflict.Id);
            writer.WriteComma();

            writer.WritePropertyName(Constants.Documents.Metadata.ChangeVector);
            writer.WriteString(string.Empty);
            writer.WriteComma();

            writer.WritePropertyName(Constants.Documents.Metadata.Conflict);
            writer.WriteBool(true);

            writer.WriteEndObject();

            writer.WriteEndObject();
        }

        public static async Task<(long NumberOfResults, long TotalDocumentsSizeInBytes)> WriteObjectsAsync(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, IEnumerable<BlittableJsonReaderObject> objects, CancellationToken token)
        {
            long numberOfResults = 0;
            long totalDocumentsSizeInBytes = 0;

            writer.WriteStartArray();

            var first = true;
            foreach (var o in objects)
            {
                numberOfResults++;

                if (first == false)
                    writer.WriteComma();
                first = false;

                if (o == null)
                {
                    writer.WriteNull();
                    continue;
                }

                using (o)
                {
                    writer.WriteObject(o);

                    var writtenBytes = await writer.MaybeFlushAsync(token).ConfigureAwait(false);

                    if (o.HasParent)
                    {
                        // If blittable has a parent then its size is the parent's size
                        // Let's use the number of actually written bytes then
                        totalDocumentsSizeInBytes += writtenBytes;
                    }
                    else
                        totalDocumentsSizeInBytes += o.Size;
                }
            }

            writer.WriteEndArray();
            return (numberOfResults, totalDocumentsSizeInBytes);
        }

        public static async Task<(long NumberOfResults, long TotalDocumentsSizeInBytes)> WriteObjectsAsync(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, IAsyncEnumerable<BlittableJsonReaderObject> objects, CancellationToken token)
        {
            long numberOfResults = 0;
            long totalDocumentsSizeInBytes = 0;

            writer.WriteStartArray();

            var first = true;
            await foreach (var o in objects.WithCancellation(token))
            {
                numberOfResults++;

                if (first == false)
                    writer.WriteComma();
                first = false;

                if (o == null)
                {
                    writer.WriteNull();
                    continue;
                }

                using (o)
                {
                    writer.WriteObject(o);

                    var writtenBytes = await writer.MaybeFlushAsync(token).ConfigureAwait(false);

                    if (o.HasParent)
                    {
                        // If blittable has a parent then its size is the parent's size
                        // Let's use the number of actually written bytes then
                        totalDocumentsSizeInBytes += writtenBytes;
                    }
                    else
                        totalDocumentsSizeInBytes += o.Size;
                }
            }

            writer.WriteEndArray();
            return (numberOfResults, totalDocumentsSizeInBytes);
        }

        public static void WriteCounters(this AbstractBlittableJsonTextWriter writer, Dictionary<string, List<CounterDetail>> counters)
        {
            writer.WriteStartObject();

            var first = true;
            foreach (var kvp in counters)
            {
                if (first == false)
                    writer.WriteComma();

                first = false;

                writer.WritePropertyName(kvp.Key);

                writer.WriteCountersForDocument(kvp.Value);
            }

            writer.WriteEndObject();
        }

        private static void WriteCountersForDocument(this AbstractBlittableJsonTextWriter writer, List<CounterDetail> counters)
        {
            writer.WriteStartArray();

            var first = true;
            foreach (var counter in counters)
            {
                if (first == false)
                    writer.WriteComma();
                first = false;

                if (counter == null)
                {
                    writer.WriteNull();
                    continue;
                }

                writer.WriteStartObject();

                writer.WritePropertyName(nameof(CounterDetail.DocumentId));
                writer.WriteString(counter.DocumentId);
                writer.WriteComma();

                writer.WritePropertyName(nameof(CounterDetail.CounterName));
                writer.WriteString(counter.CounterName);
                writer.WriteComma();

                writer.WritePropertyName(nameof(CounterDetail.TotalValue));
                writer.WriteInteger(counter.TotalValue);

                writer.WriteEndObject();
            }

            writer.WriteEndArray();
        }

        public static async Task WriteCountersForDocumentAsync(this AsyncBlittableJsonTextWriter writer, List<CounterDetail> counters, CancellationToken token)
        {
            writer.WriteStartArray();

            var first = true;
            foreach (var counter in counters)
            {
                if (first == false)
                    writer.WriteComma();
                first = false;

                if (counter == null)
                {
                    writer.WriteNull();
                    await writer.MaybeFlushAsync(token).ConfigureAwait(false);
                    continue;
                }

                writer.WriteStartObject();

                writer.WritePropertyName(nameof(CounterDetail.DocumentId));
                writer.WriteString(counter.DocumentId);
                writer.WriteComma();

                writer.WritePropertyName(nameof(CounterDetail.CounterName));
                writer.WriteString(counter.CounterName);
                writer.WriteComma();

                writer.WritePropertyName(nameof(CounterDetail.TotalValue));
                writer.WriteInteger(counter.TotalValue);

                writer.WriteEndObject();

                await writer.MaybeFlushAsync(token).ConfigureAwait(false);
            }

            writer.WriteEndArray();
        }

        public static async Task WriteCompareExchangeValuesAsync(this AsyncBlittableJsonTextWriter writer, Dictionary<string, CompareExchangeValue<BlittableJsonReaderObject>> compareExchangeValues, CancellationToken token)
        {
            writer.WriteStartObject();

            var first = true;
            foreach (var kvp in compareExchangeValues)
            {
                if (first == false)
                    writer.WriteComma();

                first = false;

                writer.WritePropertyName(kvp.Key);

                writer.WriteStartObject();

                writer.WritePropertyName(nameof(kvp.Value.Key));
                writer.WriteString(kvp.Key);
                writer.WriteComma();

                writer.WritePropertyName(nameof(kvp.Value.Index));
                writer.WriteInteger(kvp.Value.Index);
                writer.WriteComma();

                writer.WritePropertyName(nameof(kvp.Value));
                writer.WriteObject(kvp.Value.Value);

                if (kvp.Value.ChangeVector != null)
                {
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(kvp.Value.ChangeVector));
                    writer.WriteString(kvp.Value.ChangeVector);
                }

                writer.WriteEndObject();

                await writer.MaybeFlushAsync(token).ConfigureAwait(false);
            }

            writer.WriteEndObject();
        }

        private static void WriteDocumentMetadata(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context,
            Document document, Func<LazyStringValue, bool> filterMetadataProperty = null)
        {
            writer.WriteStartObject();
            document.Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata);
            WriteMetadata(writer, document, metadata, filterMetadataProperty);

            writer.WriteEndObject();
        }

        public static void WriteMetadata(this AbstractBlittableJsonTextWriter writer, Document document, BlittableJsonReaderObject metadata, Func<LazyStringValue, bool> filterMetadataProperty = null)
        {
            writer.WritePropertyName(Constants.Documents.Metadata.Key);
            writer.WriteStartObject();
            bool first = true;
            if (metadata != null)
            {
                var size = metadata.Count;
                var prop = new BlittableJsonReaderObject.PropertyDetails();

                for (int i = 0; i < size; i++)
                {
                    metadata.GetPropertyByIndex(i, ref prop);

                    if (filterMetadataProperty != null && filterMetadataProperty(prop.Name))
                        continue;

                    if (first == false)
                    {
                        writer.WriteComma();
                    }
                    first = false;
                    writer.WritePropertyName(prop.Name);
                    writer.WriteValue(prop.Token & BlittableJsonReaderBase.TypesMask, prop.Value);
                }
            }

            if (first == false)
            {
                writer.WriteComma();
            }
            writer.WritePropertyName(Constants.Documents.Metadata.ChangeVector);
            writer.WriteString(document.ChangeVector);

            if (document.Flags != DocumentFlags.None)
            {
                writer.WriteComma();
                writer.WritePropertyName(Constants.Documents.Metadata.Flags);
                writer.WriteString(document.Flags.ToString());
            }
            if (document.Id != null)
            {
                writer.WriteComma();
                writer.WritePropertyName(Constants.Documents.Metadata.Id);
                writer.WriteString(document.Id);
            }
            if (document.IndexScore != null)
            {
                writer.WriteComma();
                writer.WritePropertyName(Constants.Documents.Metadata.IndexScore);
                writer.WriteDouble(document.IndexScore.Value);
            }
            if (document.Distance != null)
            {
                writer.WriteComma();
                var result = document.Distance.Value;
                writer.WritePropertyName(Constants.Documents.Metadata.SpatialResult);
                writer.WriteStartObject();
                writer.WritePropertyName(nameof(result.Distance));
                writer.WriteDouble(result.Distance);
                writer.WriteComma();
                writer.WritePropertyName(nameof(result.Latitude));
                writer.WriteDouble(result.Latitude);
                writer.WriteComma();
                writer.WritePropertyName(nameof(result.Longitude));
                writer.WriteDouble(result.Longitude);
                writer.WriteEndObject();
            }
            if (document.LastModified != DateTime.MinValue)
            {
                writer.WriteComma();
                writer.WritePropertyName(Constants.Documents.Metadata.LastModified);
                writer.WriteDateTime(document.LastModified, isUtc: true);
            }

            if (document is ShardedQueryResultDocument shardedQueryResult)
            {
                if (shardedQueryResult.OrderByFields is { Count: > 0 })
                {
                    writer.WriteComma();
                    writer.WritePropertyName(Constants.Documents.Metadata.Sharding.Querying.OrderByFields);
                    writer.WriteStartArray();

                    var firstOrderByField = true;
                    foreach (var orderByField in shardedQueryResult.OrderByFields)
                    {
                        if (firstOrderByField == false)
                            writer.WriteComma();

                        firstOrderByField = false;
                        switch (orderByField.OrderType)
                        {
                            case OrderByFieldType.Long:
                                writer.WriteInteger(orderByField.LongValue);
                                break;
                            case OrderByFieldType.Double:
                                writer.WriteDouble(orderByField.DoubleValue);
                                break;
                            default:
                                writer.WriteString(orderByField.StringValue);
                                break;
                        }
                    }

                    writer.WriteEndArray();
                }

                if (shardedQueryResult.ResultDataHash is not null)
                {
                    writer.WriteComma();
                    writer.WritePropertyName(Constants.Documents.Metadata.Sharding.Querying.ResultDataHash);
                    writer.WriteString(shardedQueryResult.ResultDataHash.Value.ToString());
                }
            }

            writer.WriteEndObject();
        }

        internal static readonly StringSegment MetadataKeySegment = new StringSegment(Constants.Documents.Metadata.Key);

        private static void WriteDocumentInternal(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, Document document, Func<LazyStringValue, bool> filterMetadataProperty = null)
        {
            writer.WriteStartObject();
            WriteDocumentProperties(writer, context, document, filterMetadataProperty);
            writer.WriteEndObject();
        }

        private static unsafe void WriteDocumentProperties(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, Document document, Func<LazyStringValue, bool> filterMetadataProperty = null)
        {
            var first = true;
            BlittableJsonReaderObject metadata = null;
            var metadataField = context.GetLazyStringForFieldWithCaching(MetadataKeySegment);

            var prop = new BlittableJsonReaderObject.PropertyDetails();
            using (var buffers = document.Data.GetPropertiesByInsertionOrder())
            {
                for (var i = 0; i < buffers.Size; i++)
                {
                    document.Data.GetPropertyByIndex(buffers.Properties[i], ref prop);
                    if (metadataField.Equals(prop.Name))
                    {
                        metadata = (BlittableJsonReaderObject)prop.Value;
                        continue;
                    }
                    if (first == false)
                    {
                        writer.WriteComma();
                    }
                    first = false;
                    writer.WritePropertyName(prop.Name);
                    writer.WriteValue(prop.Token & BlittableJsonReaderBase.TypesMask, prop.Value);
                }
            }

            if (first == false)
                writer.WriteComma();

            WriteMetadata(writer, document, metadata, filterMetadataProperty);

            if (document.TimeSeriesStream != null)
            {
                writer.WriteComma();
                writer.WriteArray(document.TimeSeriesStream.Key, document.TimeSeriesStream.TimeSeries, context);
            }
        }

        public static unsafe void WriteDocumentPropertiesWithoutMetadata(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, Document document)
        {
            var first = true;

            var prop = new BlittableJsonReaderObject.PropertyDetails();

            using (var buffers = document.Data.GetPropertiesByInsertionOrder())
            {
                for (var i = 0; i < buffers.Size; i++)
                {
                    document.Data.GetPropertyByIndex(buffers.Properties[i], ref prop);
                    if (first == false)
                    {
                        writer.WriteComma();
                    }
                    first = false;
                    writer.WritePropertyName(prop.Name);
                    writer.WriteValue(prop.Token & BlittableJsonReaderBase.TypesMask, prop.Value);
                }
            }
        }

        public static void WriteOperationIdAndNodeTag(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, long operationId, string nodeTag)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(OperationIdResult.OperationId));
            writer.WriteInteger(operationId);

            writer.WriteComma();

            writer.WritePropertyName(nameof(OperationIdResult.OperationNodeTag));
            writer.WriteString(nodeTag);

            writer.WriteEndObject();
        }

        public static void WriteArrayOfResultsAndCount(this AbstractBlittableJsonTextWriter writer, IEnumerable<string> results)
        {
            writer.WriteStartObject();
            writer.WritePropertyName("Results");
            writer.WriteStartArray();

            var first = true;
            var count = 0;

            foreach (var id in results)
            {
                if (first == false)
                    writer.WriteComma();

                writer.WriteString(id);
                count++;

                first = false;
            }

            writer.WriteEndArray();
            writer.WriteComma();

            writer.WritePropertyName("Count");
            writer.WriteInteger(count);

            writer.WriteEndObject();
        }

        public static void WriteReduceTrees(this AbstractBlittableJsonTextWriter writer, IEnumerable<ReduceTree> trees)
        {
            writer.WriteStartObject();
            writer.WritePropertyName("Results");

            writer.WriteStartArray();

            var first = true;

            foreach (var tree in trees)
            {
                if (first == false)
                    writer.WriteComma();

                writer.WriteStartObject();

                writer.WritePropertyName(nameof(ReduceTree.Name));
                writer.WriteString(tree.Name);
                writer.WriteComma();

                writer.WritePropertyName(nameof(ReduceTree.DisplayName));
                writer.WriteString(tree.DisplayName);
                writer.WriteComma();

                writer.WritePropertyName(nameof(ReduceTree.Depth));
                writer.WriteInteger(tree.Depth);
                writer.WriteComma();

                writer.WritePropertyName(nameof(ReduceTree.PageCount));
                writer.WriteInteger(tree.PageCount);
                writer.WriteComma();

                writer.WritePropertyName(nameof(ReduceTree.NumberOfEntries));
                writer.WriteInteger(tree.NumberOfEntries);
                writer.WriteComma();

                writer.WritePropertyName(nameof(ReduceTree.Root));
                writer.WriteTreePagesRecursively(new[] { tree.Root });

                writer.WriteEndObject();

                first = false;
            }

            writer.WriteEndArray();

            writer.WriteEndObject();
        }

        private static void WriteTreePagesRecursively(this AbstractBlittableJsonTextWriter writer, IEnumerable<ReduceTreePage> pages)
        {
            var first = true;

            foreach (var page in pages)
            {
                if (first == false)
                    writer.WriteComma();

                writer.WriteStartObject();

                writer.WritePropertyName(nameof(TreePage.PageNumber));
                writer.WriteInteger(page.PageNumber);
                writer.WriteComma();

                writer.WritePropertyName(nameof(ReduceTreePage.AggregationResult));
                if (page.AggregationResult != null)
                    writer.WriteObject(page.AggregationResult);
                else
                    writer.WriteNull();
                writer.WriteComma();

                writer.WritePropertyName(nameof(ReduceTreePage.Children));
                if (page.Children != null)
                {
                    writer.WriteStartArray();
                    WriteTreePagesRecursively(writer, page.Children);
                    writer.WriteEndArray();
                }
                else
                    writer.WriteNull();
                writer.WriteComma();

                writer.WritePropertyName(nameof(ReduceTreePage.Entries));
                if (page.Entries != null)
                {
                    writer.WriteStartArray();

                    var firstEntry = true;
                    foreach (var entry in page.Entries)
                    {
                        if (firstEntry == false)
                            writer.WriteComma();

                        writer.WriteStartObject();

                        writer.WritePropertyName(nameof(MapResultInLeaf.Data));
                        writer.WriteObject(entry.Data);
                        writer.WriteComma();

                        writer.WritePropertyName(nameof(MapResultInLeaf.Source));
                        writer.WriteString(entry.Source);

                        writer.WriteEndObject();

                        firstEntry = false;
                    }

                    writer.WriteEndArray();
                }
                else
                    writer.WriteNull();

                writer.WriteEndObject();
                first = false;
            }
        }

        public static DynamicJsonValue GetOrCreateMetadata(DynamicJsonValue result)
        {
            return (DynamicJsonValue)(result[Constants.Documents.Metadata.Key] ?? (result[Constants.Documents.Metadata.Key] = new DynamicJsonValue()));
        }

        public static void MergeMetadata(DynamicJsonValue result, DynamicJsonValue metadata)
        {
            var m1 = GetOrCreateMetadata(result);
            var m2 = GetOrCreateMetadata(metadata);

            foreach (var item in m2.Properties)
            {
                m1[item.Name] = item.Value;
            }
        }

        public static void WriteNextOperationIdAndNodeTag(this AbstractBlittableJsonTextWriter writer, long operationId, string nodeTag)
        {
            writer.WriteStartObject();
            writer.WritePropertyName("Id");
            writer.WriteInteger(operationId);
            writer.WriteComma();
            writer.WritePropertyName("NodeTag");
            writer.WriteString(nodeTag);
            writer.WriteEndObject();
        }
    }
}
