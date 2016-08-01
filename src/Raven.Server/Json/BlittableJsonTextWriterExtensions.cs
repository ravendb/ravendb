using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Client.Data;
using Raven.Client.Data.Indexes;
using Raven.Client.Data.Queries;
using Raven.Client.Indexing;
using Raven.Server.Documents;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Dynamic;
using Raven.Server.Documents.Replication;
using Sparrow.Json;

namespace Raven.Server.Json
{
    public static class BlittableJsonTextWriterExtensions
    {
        public static void WriteChangeVector(this BlittableJsonTextWriter writer, JsonOperationContext context,
            ChangeVectorEntry[] changeVector)
        {
            writer.WriteStartArray();
            for (int i = 0; i < changeVector.Length; i++)
            {
                var entry = changeVector[i];
                writer.WriteChangeVectorEntry(context, entry);
                writer.WriteComma();
            }
            writer.WriteEndArray();
        }

        public static void WriteChangeVectorEntry(this BlittableJsonTextWriter writer, JsonOperationContext context, ChangeVectorEntry entry)
        {
            writer.WriteStartObject();

            writer.WritePropertyName((nameof(entry.Etag)));
            writer.WriteInteger(entry.Etag);
            writer.WriteComma();

            writer.WritePropertyName((nameof(entry.DbId)));
            writer.WriteString(entry.DbId.ToString());

            writer.WriteEndObject();
        }


        public static void WriteExplanation(this BlittableJsonTextWriter writer, JsonOperationContext context, DynamicQueryToIndexMatcher.Explanation explanation)
        {
            writer.WriteStartObject();

            writer.WritePropertyName((nameof(explanation.Index)));
            writer.WriteString((explanation.Index));
            writer.WriteComma();

            writer.WritePropertyName((nameof(explanation.Reason)));
            writer.WriteString((explanation.Reason));

            writer.WriteEndObject();
        }

        public static void WriteDocumentQueryResult(this BlittableJsonTextWriter writer, JsonOperationContext context, DocumentQueryResult result, bool metadataOnly)
        {
            writer.WriteStartObject();

            writer.WritePropertyName((nameof(result.TotalResults)));
            writer.WriteInteger(result.TotalResults);
            writer.WriteComma();

            writer.WritePropertyName((nameof(result.SkippedResults)));
            writer.WriteInteger(result.SkippedResults);
            writer.WriteComma();

            writer.WriteQueryResult(context, result, metadataOnly, partial: true);

            writer.WriteEndObject();
        }

        public static void WriteQueryResult(this BlittableJsonTextWriter writer, JsonOperationContext context, QueryResultBase<Document> result, bool metadataOnly, bool partial = false)
        {
            if (partial == false)
                writer.WriteStartObject();

            writer.WritePropertyName((nameof(result.IndexName)));
            writer.WriteString((result.IndexName));
            writer.WriteComma();

            writer.WritePropertyName((nameof(result.Results)));
            writer.WriteDocuments(context, result.Results, metadataOnly);
            writer.WriteComma();

            writer.WritePropertyName(nameof(result.Includes));
            writer.WriteDocuments(context, result.Includes, metadataOnly);
            writer.WriteComma();

            writer.WritePropertyName((nameof(result.IndexTimestamp)));
            writer.WriteString((result.IndexTimestamp.ToString(Default.DateTimeFormatsToWrite)));
            writer.WriteComma();

            writer.WritePropertyName((nameof(result.LastQueryTime)));
            writer.WriteString((result.LastQueryTime.ToString(Default.DateTimeFormatsToWrite)));
            writer.WriteComma();

            writer.WritePropertyName((nameof(result.IsStale)));
            writer.WriteBool(result.IsStale);
            writer.WriteComma();

            writer.WritePropertyName((nameof(result.ResultEtag)));
            writer.WriteInteger(result.ResultEtag);

            if (partial == false)
                writer.WriteEndObject();
        }

        public static void WriteIndexingPerformanceStats(this BlittableJsonTextWriter writer, JsonOperationContext context, IndexingPerformanceStats stats)
        {
            writer.WriteStartObject();

            writer.WritePropertyName((nameof(stats.Completed)));
            writer.WriteString((stats.Completed.GetDefaultRavenFormat(isUtc: true)));
            writer.WriteComma();

            writer.WritePropertyName((nameof(stats.Started)));
            writer.WriteString((stats.Started.GetDefaultRavenFormat(isUtc: true)));
            writer.WriteComma();

            writer.WritePropertyName((nameof(stats.DurationInMilliseconds)));
            using (var lazyStringValue = context.GetLazyString(stats.DurationInMilliseconds.ToInvariantString()))
                writer.WriteDouble(new LazyDoubleValue(lazyStringValue));
            writer.WriteComma();

            writer.WritePropertyName((nameof(stats.Details)));
            writer.WriteIndexingPerformanceOperation(context, stats.Details);

            writer.WriteEndObject();
        }

        public static void WriteIndexingPerformanceOperation(this BlittableJsonTextWriter writer, JsonOperationContext context, IndexingPerformanceOperation operation)
        {
            writer.WriteStartObject();

            writer.WritePropertyName((nameof(operation.DurationInMilliseconds)));
            using (var lazyStringValue = context.GetLazyString(operation.DurationInMilliseconds.ToInvariantString()))
                writer.WriteDouble(new LazyDoubleValue(lazyStringValue));
            writer.WriteComma();

            writer.WritePropertyName((nameof(operation.Name)));
            writer.WriteString((operation.Name));
            writer.WriteComma();

            writer.WritePropertyName((nameof(operation.Operations)));
            writer.WriteStartArray();
            if (operation.Operations != null)
            {
                var isFirstInternal = true;
                foreach (var op in operation.Operations)
                {
                    if (isFirstInternal == false)
                        writer.WriteComma();

                    isFirstInternal = false;

                    writer.WriteIndexingPerformanceOperation(context, op);
                }
            }
            writer.WriteEndArray();

            writer.WriteEndObject();
        }

        public static void WriteIndexQuery(this BlittableJsonTextWriter writer, JsonOperationContext context, IndexQueryServerSide query)
        {
            writer.WriteStartObject();

            writer.WritePropertyName((nameof(query.AllowMultipleIndexEntriesForSameDocumentToResultTransformer)));
            writer.WriteBool(query.AllowMultipleIndexEntriesForSameDocumentToResultTransformer);
            writer.WriteComma();

            writer.WritePropertyName((nameof(query.CutoffEtag)));
            if (query.CutoffEtag.HasValue)
                writer.WriteInteger(query.CutoffEtag.Value);
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName((nameof(query.DebugOptionGetIndexEntries)));
            writer.WriteBool(query.DebugOptionGetIndexEntries);
            writer.WriteComma();

            writer.WritePropertyName((nameof(query.DefaultField)));
            if (query.DefaultField != null)
                writer.WriteString((query.DefaultField));
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName((nameof(query.DefaultOperator)));
            writer.WriteString((query.DefaultOperator.ToString()));
            writer.WriteComma();

            writer.WritePropertyName((nameof(query.DisableCaching)));
            writer.WriteBool(query.DisableCaching);
            writer.WriteComma();

            writer.WritePropertyName((nameof(query.ExplainScores)));
            writer.WriteBool(query.ExplainScores);
            writer.WriteComma();

            writer.WritePropertyName((nameof(query.HighlighterKeyName)));
            if (query.HighlighterKeyName != null)
                writer.WriteString((query.HighlighterKeyName));
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName((nameof(query.IsDistinct)));
            writer.WriteBool(query.IsDistinct);
            writer.WriteComma();

            writer.WritePropertyName((nameof(query.PageSize)));
            writer.WriteInteger(query.PageSize);
            writer.WriteComma();

            writer.WritePropertyName((nameof(query.PageSizeSet)));
            writer.WriteBool(query.PageSizeSet);
            writer.WriteComma();

            writer.WritePropertyName((nameof(query.Query)));
            if (query.Query != null)
                writer.WriteString((query.Query));
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName((nameof(query.Transformer)));
            if (query.Transformer != null)
                writer.WriteString((query.Transformer));
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName((nameof(query.ShowTimings)));
            writer.WriteBool(query.ShowTimings);
            writer.WriteComma();

            writer.WritePropertyName((nameof(query.SkipDuplicateChecking)));
            writer.WriteBool(query.SkipDuplicateChecking);
            writer.WriteComma();

            writer.WritePropertyName((nameof(query.Start)));
            writer.WriteInteger(query.Start);
            writer.WriteComma();

            //writer.WritePropertyName((nameof(query.TotalSize)));
            //writer.WriteInteger(query.TotalSize.Value);
            //writer.WriteComma();

            writer.WritePropertyName((nameof(query.WaitForNonStaleResults)));
            writer.WriteBool(query.WaitForNonStaleResults);
            writer.WriteComma();

            writer.WritePropertyName((nameof(query.WaitForNonStaleResultsAsOfNow)));
            writer.WriteBool(query.WaitForNonStaleResultsAsOfNow);
            writer.WriteComma();

            writer.WritePropertyName((nameof(query.WaitForNonStaleResultsTimeout)));
            if (query.WaitForNonStaleResultsTimeout.HasValue)
                writer.WriteString((query.WaitForNonStaleResultsTimeout.Value.ToString()));
            writer.WriteComma();

            writer.WritePropertyName((nameof(query.DynamicMapReduceFields)));
            writer.WriteStartArray();
            var isFirstInternal = true;
            foreach (var field in query.DynamicMapReduceFields)
            {
                if (isFirstInternal == false)
                    writer.WriteComma();

                isFirstInternal = false;

                writer.WriteStartObject();

                writer.WritePropertyName((nameof(field.Name)));
                writer.WriteString((field.Name));
                writer.WriteComma();

                writer.WritePropertyName((nameof(field.IsGroupBy)));
                writer.WriteBool(field.IsGroupBy);
                writer.WriteComma();

                writer.WritePropertyName((nameof(field.OperationType)));
                writer.WriteString((field.OperationType.ToString()));
                writer.WriteComma();

                writer.WriteEndObject();
            }
            writer.WriteEndArray();
            writer.WriteComma();

            writer.WritePropertyName((nameof(query.FieldsToFetch)));
            if (query.FieldsToFetch != null)
            {
                writer.WriteStartArray();

                isFirstInternal = true;
                foreach (var field in query.FieldsToFetch)
                {
                    if (isFirstInternal == false) writer.WriteComma();

                    isFirstInternal = false;

                    writer.WriteString((field));
                }

                writer.WriteEndArray();
            }
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName((nameof(query.HighlightedFields)));
            writer.WriteStartArray();
            if (query.HighlightedFields != null)
            {
                isFirstInternal = true;
                foreach (var field in query.HighlightedFields)
                {
                    if (isFirstInternal == false)
                        writer.WriteComma();

                    isFirstInternal = false;

                    writer.WriteStartObject();

                    writer.WritePropertyName((nameof(field.Field)));
                    writer.WriteString((field.Field));
                    writer.WriteComma();

                    writer.WritePropertyName((nameof(field.FragmentCount)));
                    writer.WriteInteger(field.FragmentCount);
                    writer.WriteComma();

                    writer.WritePropertyName((nameof(field.FragmentLength)));
                    writer.WriteInteger(field.FragmentLength);
                    writer.WriteComma();

                    writer.WritePropertyName((nameof(field.FragmentsField)));
                    writer.WriteString((field.FragmentsField));

                    writer.WriteEndObject();
                }
            }
            writer.WriteEndArray();
            writer.WriteComma();

            writer.WritePropertyName((nameof(query.HighlighterPostTags)));
            writer.WriteStartArray();
            if (query.HighlighterPostTags != null)
            {
                isFirstInternal = true;
                foreach (var tag in query.HighlighterPostTags)
                {
                    if (isFirstInternal == false)
                        writer.WriteComma();

                    isFirstInternal = false;

                    writer.WriteString((tag));
                }
            }
            writer.WriteEndArray();
            writer.WriteComma();

            writer.WritePropertyName((nameof(query.HighlighterPreTags)));
            writer.WriteStartArray();
            if (query.HighlighterPreTags != null)
            {
                isFirstInternal = true;
                foreach (var tag in query.HighlighterPreTags)
                {
                    if (isFirstInternal == false)
                        writer.WriteComma();

                    isFirstInternal = false;

                    writer.WriteString((tag));
                }
            }
            writer.WriteEndArray();
            writer.WriteComma();

            writer.WritePropertyName((nameof(query.SortedFields)));
            writer.WriteStartArray();
            if (query.SortedFields != null)
            {
                isFirstInternal = true;
                foreach (var field in query.SortedFields)
                {
                    if (isFirstInternal == false)
                        writer.WriteComma();

                    isFirstInternal = false;

                    writer.WriteStartObject();

                    writer.WritePropertyName((nameof(field.Field)));
                    writer.WriteString((field.Field));
                    writer.WriteComma();

                    writer.WritePropertyName((nameof(field.Descending)));
                    writer.WriteBool(field.Descending);
                    writer.WriteComma();

                    writer.WriteEndObject();
                }
            }
            writer.WriteEndArray();
            writer.WriteComma();

            writer.WritePropertyName((nameof(query.TransformerParameters)));
            writer.WriteStartObject();
            if (query.TransformerParameters != null)
                writer.WriteObject(query.TransformerParameters);
            writer.WriteEndObject();

            writer.WriteEndObject();
        }

        public static void WriteDatabaseStatistics(this BlittableJsonTextWriter writer, JsonOperationContext context, DatabaseStatistics statistics)
        {
            writer.WriteStartObject();

            writer.WritePropertyName((nameof(statistics.CountOfIndexes)));
            writer.WriteInteger(statistics.CountOfIndexes);
            writer.WriteComma();

            writer.WritePropertyName((nameof(statistics.ApproximateTaskCount)));
            writer.WriteInteger(statistics.ApproximateTaskCount);
            writer.WriteComma();

            writer.WritePropertyName((nameof(statistics.CountOfDocuments)));
            writer.WriteInteger(statistics.CountOfDocuments);
            writer.WriteComma();

            if (statistics.CountOfRevisionDocuments.HasValue)
            {
                writer.WritePropertyName((nameof(statistics.CountOfRevisionDocuments)));
                writer.WriteInteger(statistics.CountOfRevisionDocuments.Value);
                writer.WriteComma();
            }

            writer.WritePropertyName((nameof(statistics.CountOfTransformers)));
            writer.WriteInteger(statistics.CountOfTransformers);
            writer.WriteComma();

            writer.WritePropertyName((nameof(statistics.CurrentNumberOfItemsToIndexInSingleBatch)));
            writer.WriteInteger(statistics.CurrentNumberOfItemsToIndexInSingleBatch);
            writer.WriteComma();

            writer.WritePropertyName((nameof(statistics.CurrentNumberOfItemsToReduceInSingleBatch)));
            writer.WriteInteger(statistics.CurrentNumberOfItemsToReduceInSingleBatch);
            writer.WriteComma();

            writer.WritePropertyName((nameof(statistics.CurrentNumberOfParallelTasks)));
            writer.WriteInteger(statistics.CurrentNumberOfParallelTasks);
            writer.WriteComma();

            writer.WritePropertyName((nameof(statistics.DatabaseId)));
            writer.WriteString((statistics.DatabaseId.ToString()));
            writer.WriteComma();

            writer.WritePropertyName((nameof(statistics.Is64Bit)));
            writer.WriteBool(statistics.Is64Bit);
            writer.WriteComma();

            writer.WritePropertyName((nameof(statistics.LastDocEtag)));
            if (statistics.LastDocEtag.HasValue)
                writer.WriteInteger(statistics.LastDocEtag.Value);
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName((nameof(statistics.Indexes)));
            writer.WriteStartArray();
            var isFirstInternal = true;
            foreach (var index in statistics.Indexes)
            {
                if (isFirstInternal == false)
                    writer.WriteComma();

                isFirstInternal = false;

                writer.WriteStartObject();

                writer.WritePropertyName((nameof(index.IsStale)));
                writer.WriteBool(index.IsStale);
                writer.WriteComma();

                writer.WritePropertyName((nameof(index.Name)));
                writer.WriteString((index.Name));
                writer.WriteComma();

                writer.WritePropertyName((nameof(index.IndexId)));
                writer.WriteInteger(index.IndexId);
                writer.WriteComma();

                writer.WritePropertyName((nameof(index.LockMode)));
                writer.WriteString((index.LockMode.ToString()));
                writer.WriteComma();

                writer.WritePropertyName((nameof(index.Priority)));
                writer.WriteString((index.Priority.ToString()));

                writer.WriteEndObject();
            }
            writer.WriteEndArray();

            writer.WriteEndObject();
        }

        public static void WriteTransformerDefinition(this BlittableJsonTextWriter writer, JsonOperationContext context, TransformerDefinition transformerDefinition)
        {
            writer.WriteStartObject();

            writer.WritePropertyName((nameof(transformerDefinition.Name)));
            writer.WriteString((transformerDefinition.Name));
            writer.WriteComma();

            writer.WritePropertyName((nameof(transformerDefinition.TransformResults)));
            writer.WriteString((transformerDefinition.TransformResults));
            writer.WriteComma();

            writer.WritePropertyName((nameof(transformerDefinition.LockMode)));
            writer.WriteString((transformerDefinition.LockMode.ToString()));
            writer.WriteComma();

            writer.WritePropertyName((nameof(transformerDefinition.Temporary)));
            writer.WriteBool(transformerDefinition.Temporary);
            writer.WriteComma();

            writer.WritePropertyName((nameof(transformerDefinition.TransfomerId)));
            writer.WriteInteger(transformerDefinition.TransfomerId);
            writer.WriteComma();

            writer.WriteEndObject();
        }

        public static void WriteIndexDefinition(this BlittableJsonTextWriter writer, JsonOperationContext context, IndexDefinition indexDefinition)
        {
            writer.WriteStartObject();

            writer.WritePropertyName((nameof(indexDefinition.Name)));
            writer.WriteString((indexDefinition.Name));
            writer.WriteComma();

            writer.WritePropertyName((nameof(indexDefinition.IndexId)));
            writer.WriteInteger(indexDefinition.IndexId);
            writer.WriteComma();

            writer.WritePropertyName((nameof(indexDefinition.Type)));
            writer.WriteString((indexDefinition.Type.ToString()));
            writer.WriteComma();

            writer.WritePropertyName((nameof(indexDefinition.IsTestIndex)));
            writer.WriteBool(indexDefinition.IsTestIndex);
            writer.WriteComma();

            writer.WritePropertyName((nameof(indexDefinition.LockMode)));
            writer.WriteString((indexDefinition.LockMode.ToString()));
            writer.WriteComma();

            writer.WritePropertyName((nameof(indexDefinition.MaxIndexOutputsPerDocument)));
            if (indexDefinition.MaxIndexOutputsPerDocument.HasValue)
                writer.WriteInteger(indexDefinition.MaxIndexOutputsPerDocument.Value);
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName((nameof(indexDefinition.IndexVersion)));
            if (indexDefinition.IndexVersion.HasValue)
                writer.WriteInteger(indexDefinition.IndexVersion.Value);
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName((nameof(indexDefinition.IsSideBySideIndex)));
            writer.WriteBool(indexDefinition.IsSideBySideIndex);
            writer.WriteComma();

            writer.WritePropertyName((nameof(indexDefinition.IsTestIndex)));
            writer.WriteBool(indexDefinition.IsTestIndex);
            writer.WriteComma();

            writer.WritePropertyName((nameof(indexDefinition.Reduce)));
            if (string.IsNullOrWhiteSpace(indexDefinition.Reduce) == false)
                writer.WriteString((indexDefinition.Reduce));
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName((nameof(indexDefinition.Maps)));
            writer.WriteStartArray();
            var isFirstInternal = true;
            foreach (var map in indexDefinition.Maps)
            {
                if (isFirstInternal == false)
                    writer.WriteComma();

                isFirstInternal = false;
                writer.WriteString((map));
            }
            writer.WriteEndArray();
            writer.WriteComma();

            writer.WritePropertyName((nameof(indexDefinition.Fields)));
            writer.WriteStartObject();
            isFirstInternal = true;
            foreach (var kvp in indexDefinition.Fields)
            {
                if (isFirstInternal == false)
                    writer.WriteComma();

                isFirstInternal = false;
                writer.WritePropertyName((kvp.Key));
                if (kvp.Value != null)
                    writer.WriteIndexFieldOptions(context, kvp.Value);
                else
                    writer.WriteNull();
            }
            writer.WriteEndObject();

            writer.WriteEndObject();
        }

        public static void WriteIndexStats(this BlittableJsonTextWriter writer, JsonOperationContext context, IndexStats stats)
        {
            writer.WriteStartObject();

            writer.WritePropertyName((nameof(stats.ForCollections)));
            writer.WriteStartArray();
            var isFirst = true;
            foreach (var collection in stats.ForCollections)
            {
                if (isFirst == false)
                    writer.WriteComma();

                isFirst = false;
                writer.WriteString((collection));
            }
            writer.WriteEndArray();
            writer.WriteComma();

            writer.WritePropertyName((nameof(stats.IsInMemory)));
            writer.WriteBool(stats.IsInMemory);
            writer.WriteComma();

            writer.WritePropertyName((nameof(stats.LastIndexedEtags)));
            writer.WriteStartObject();
            isFirst = true;
            foreach (var kvp in stats.LastIndexedEtags)
            {
                if (isFirst == false)
                    writer.WriteComma();

                isFirst = false;

                writer.WritePropertyName((kvp.Key));
                writer.WriteInteger(kvp.Value);
            }
            writer.WriteEndObject();
            writer.WriteComma();

            writer.WritePropertyName((nameof(stats.LastIndexingTime)));
            if (stats.LastIndexingTime.HasValue)
                writer.WriteString((stats.LastIndexingTime.Value.GetDefaultRavenFormat(isUtc: true)));
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName((nameof(stats.LastQueryingTime)));
            if (stats.LastQueryingTime.HasValue)
                writer.WriteString((stats.LastQueryingTime.Value.GetDefaultRavenFormat(isUtc: true)));
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName((nameof(stats.LockMode)));
            writer.WriteString((stats.LockMode.ToString()));
            writer.WriteComma();

            writer.WritePropertyName((nameof(stats.Name)));
            writer.WriteString((stats.Name));
            writer.WriteComma();

            writer.WritePropertyName((nameof(stats.Priority)));
            writer.WriteString((stats.Priority.ToString()));
            writer.WriteComma();

            writer.WritePropertyName((nameof(stats.Type)));
            writer.WriteString((stats.Type.ToString()));
            writer.WriteComma();

            writer.WritePropertyName((nameof(stats.CreatedTimestamp)));
            writer.WriteString((stats.CreatedTimestamp.GetDefaultRavenFormat(isUtc: true)));
            writer.WriteComma();

            writer.WritePropertyName((nameof(stats.EntriesCount)));
            writer.WriteInteger(stats.EntriesCount);
            writer.WriteComma();

            writer.WritePropertyName((nameof(stats.Id)));
            writer.WriteInteger(stats.Id);
            writer.WriteComma();

            writer.WritePropertyName((nameof(stats.MapAttempts)));
            writer.WriteInteger(stats.MapAttempts);
            writer.WriteComma();

            writer.WritePropertyName((nameof(stats.MapErrors)));
            writer.WriteInteger(stats.MapErrors);
            writer.WriteComma();

            writer.WritePropertyName((nameof(stats.MapSuccesses)));
            writer.WriteInteger(stats.MapSuccesses);
            writer.WriteComma();

            writer.WritePropertyName((nameof(stats.ErrorsCount)));
            writer.WriteInteger(stats.ErrorsCount);
            writer.WriteComma();

            writer.WritePropertyName((nameof(stats.IsTestIndex)));
            writer.WriteBool(stats.IsTestIndex);

            writer.WriteEndObject();
        }

        private static void WriteIndexFieldOptions(this BlittableJsonTextWriter writer, JsonOperationContext context, IndexFieldOptions options)
        {
            writer.WriteStartObject();

            writer.WritePropertyName((nameof(options.Analyzer)));
            if (string.IsNullOrWhiteSpace(options.Analyzer) == false)
                writer.WriteString((options.Analyzer));
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName((nameof(options.Indexing)));
            if (options.Indexing.HasValue)
                writer.WriteString((options.Indexing.ToString()));
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName((nameof(options.Sort)));
            if (options.Sort.HasValue)
                writer.WriteString((options.Sort.ToString()));
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName((nameof(options.Storage)));
            if (options.Storage.HasValue)
                writer.WriteString((options.Storage.ToString()));
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName((nameof(options.Suggestions)));
            if (options.Suggestions.HasValue)
                writer.WriteBool(options.Suggestions.Value);
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName((nameof(options.TermVector)));
            if (options.TermVector.HasValue)
                writer.WriteString((options.TermVector.ToString()));
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName((nameof(options.Spatial)));
            if (options.Spatial != null)
            {
                writer.WriteStartObject();

                writer.WritePropertyName((nameof(options.Spatial.Type)));
                writer.WriteString((options.Spatial.Type.ToString()));
                writer.WriteComma();

                writer.WritePropertyName((nameof(options.Spatial.MaxTreeLevel)));
                writer.WriteInteger(options.Spatial.MaxTreeLevel);
                writer.WriteComma();

                writer.WritePropertyName((nameof(options.Spatial.MaxX)));
                LazyStringValue lazyStringValue;
                using (lazyStringValue = context.GetLazyString(options.Spatial.MaxX.ToInvariantString()))
                    writer.WriteDouble(new LazyDoubleValue(lazyStringValue));
                writer.WriteComma();

                writer.WritePropertyName((nameof(options.Spatial.MaxY)));
                using (lazyStringValue = context.GetLazyString(options.Spatial.MaxY.ToInvariantString()))
                    writer.WriteDouble(new LazyDoubleValue(lazyStringValue));
                writer.WriteComma();

                writer.WritePropertyName((nameof(options.Spatial.MinX)));
                using (lazyStringValue = context.GetLazyString(options.Spatial.MinX.ToInvariantString()))
                    writer.WriteDouble(new LazyDoubleValue(lazyStringValue));
                writer.WriteComma();

                writer.WritePropertyName((nameof(options.Spatial.MinY)));
                using (lazyStringValue = context.GetLazyString(options.Spatial.MinY.ToInvariantString()))
                    writer.WriteDouble(new LazyDoubleValue(lazyStringValue));
                writer.WriteComma();

                writer.WritePropertyName((nameof(options.Spatial.Strategy)));
                writer.WriteString((options.Spatial.Strategy.ToString()));
                writer.WriteComma();

                writer.WritePropertyName((nameof(options.Spatial.Units)));
                writer.WriteString((options.Spatial.Units.ToString()));

                writer.WriteEndObject();
            }
            else
                writer.WriteNull();

            writer.WriteEndObject();
        }

        public static long WriteDocuments(this BlittableJsonTextWriter writer, JsonOperationContext context, IEnumerable<Document> documents, bool metadataOnly)
        {
            writer.WriteStartArray();

            var first = true;
            Document lastDocument = null;
            foreach (var document in documents)
            {
                if (document == null)
                    continue;

                using (document.Data)
                {
                    if (first == false)
                        writer.WriteComma();
                    first = false;

                    writer.WriteDocument(context, document, metadataOnly);
                }
                lastDocument = document;
            }

            writer.WriteEndArray();
            return lastDocument?.Etag ?? 0;
        }

        public static void WriteDocuments(this BlittableJsonTextWriter writer, JsonOperationContext context, IEnumerable<Document> documents, bool metadataOnly, int start, int count)
        {
            writer.WriteStartArray();

            var first = true;
            foreach (var document in documents.Skip(start).Take(count))
            {
                if (document == null)
                    continue;

                using (document.Data)
                {
                    if (first == false)
                        writer.WriteComma();
                    first = false;

                    writer.WriteDocument(context, document, metadataOnly);
                }
            }

            writer.WriteEndArray();
        }

        public static void WriteDocument(this BlittableJsonTextWriter writer, JsonOperationContext context, Document document, bool metadataOnly)
        {
            document.EnsureMetadata();
            if (metadataOnly)
                document.RemoveAllPropertiesExceptMetadata();

            context.Write(writer, document.Data);
        }
    }
}