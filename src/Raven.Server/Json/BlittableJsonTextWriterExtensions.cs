using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Extensions;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes.Debugging;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Dynamic;
using Raven.Server.Documents.Queries.MoreLikeThis;
using Raven.Server.Documents.Queries.Suggestion;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron.Data.BTrees;

namespace Raven.Server.Json
{
    internal static class BlittableJsonTextWriterExtensions
    {
        public static void WritePerformanceStats(this BlittableJsonTextWriter writer, JsonOperationContext context, IEnumerable<IndexPerformanceStats> stats)
        {
            writer.WriteStartObject();
            writer.WriteArray(context, "Results", stats, (w, c, stat) =>
            {
                w.WriteStartObject();

                w.WritePropertyName(nameof(stat.Name));
                w.WriteString(stat.Name);
                w.WriteComma();

                w.WritePropertyName(nameof(stat.Etag));
                w.WriteInteger(stat.Etag);
                w.WriteComma();

                w.WriteArray(c, nameof(stat.Performance), stat.Performance, (wp, cp, performance) => { wp.WriteIndexingPerformanceStats(context, performance); });

                w.WriteEndObject();
            });
            writer.WriteEndObject();
        }

        public static void WriteExplanation(this BlittableJsonTextWriter writer, JsonOperationContext context, DynamicQueryToIndexMatcher.Explanation explanation)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(explanation.Index));
            writer.WriteString(explanation.Index);
            writer.WriteComma();

            writer.WritePropertyName(nameof(explanation.Reason));
            writer.WriteString(explanation.Reason);

            writer.WriteEndObject();
        }

        public static void WriteFacetedQueryResult(this BlittableJsonTextWriter writer, JsonOperationContext context, FacetedQueryResult result)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(result.IndexName));
            writer.WriteString(result.IndexName);
            writer.WriteComma();

            writer.WritePropertyName(nameof(result.Results));
            writer.WriteStartObject();
            var isFirstInternal = true;
            foreach (var kvp in result.Results)
            {
                if (isFirstInternal == false)
                    writer.WriteComma();

                isFirstInternal = false;

                writer.WritePropertyName(kvp.Key);
                writer.WriteFacetResult(context, kvp.Value);
            }
            writer.WriteEndObject();
            writer.WriteComma();

            writer.WritePropertyName(nameof(result.IndexTimestamp));
            writer.WriteString(result.IndexTimestamp.ToString(Default.DateTimeFormatsToWrite));
            writer.WriteComma();

            writer.WritePropertyName(nameof(result.LastQueryTime));
            writer.WriteString(result.LastQueryTime.ToString(Default.DateTimeFormatsToWrite));
            writer.WriteComma();

            writer.WritePropertyName(nameof(result.IsStale));
            writer.WriteBool(result.IsStale);
            writer.WriteComma();

            writer.WritePropertyName(nameof(result.ResultEtag));
            writer.WriteInteger(result.ResultEtag);

            writer.WriteEndObject();
        }

        public static void WriteFacetResult(this BlittableJsonTextWriter writer, JsonOperationContext context, FacetResult result)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(result.RemainingHits));
            writer.WriteInteger(result.RemainingHits);
            writer.WriteComma();

            writer.WritePropertyName(nameof(result.RemainingTermsCount));
            writer.WriteInteger(result.RemainingTermsCount);
            writer.WriteComma();

            writer.WritePropertyName(nameof(result.RemainingTerms));
            writer.WriteStartArray();
            var isFirstInternal = true;
            foreach (var term in result.RemainingTerms)
            {
                if (isFirstInternal == false)
                    writer.WriteComma();

                isFirstInternal = false;

                writer.WriteString(term);
            }
            writer.WriteEndArray();
            writer.WriteComma();

            writer.WritePropertyName(nameof(result.Values));
            writer.WriteStartArray();
            isFirstInternal = true;
            foreach (var value in result.Values)
            {
                if (isFirstInternal == false)
                    writer.WriteComma();

                isFirstInternal = false;

                writer.WriteStartObject();

                writer.WritePropertyName(nameof(value.Average));
                if (value.Average.HasValue)
                {
                    using (var lazyStringValue = context.GetLazyString(value.Average.ToInvariantString()))
                        writer.WriteDouble(new LazyNumberValue(lazyStringValue));
                }
                else
                    writer.WriteNull();
                writer.WriteComma();

                writer.WritePropertyName(nameof(value.Count));
                if (value.Count.HasValue)
                    writer.WriteInteger(value.Count.Value);
                else
                    writer.WriteNull();
                writer.WriteComma();

                writer.WritePropertyName(nameof(value.Hits));
                writer.WriteInteger(value.Hits);
                writer.WriteComma();

                writer.WritePropertyName(nameof(value.Max));
                if (value.Max.HasValue)
                {
                    using (var lazyStringValue = context.GetLazyString(value.Max.ToInvariantString()))
                        writer.WriteDouble(new LazyNumberValue(lazyStringValue));
                }
                else
                    writer.WriteNull();
                writer.WriteComma();

                writer.WritePropertyName(nameof(value.Min));
                if (value.Min.HasValue)
                {
                    using (var lazyStringValue = context.GetLazyString(value.Min.ToInvariantString()))
                        writer.WriteDouble(new LazyNumberValue(lazyStringValue));
                }
                else
                    writer.WriteNull();
                writer.WriteComma();

                writer.WritePropertyName(nameof(value.Range));
                writer.WriteString(value.Range);
                writer.WriteComma();

                writer.WritePropertyName(nameof(value.Sum));
                if (value.Sum.HasValue)
                {
                    using (var lazyStringValue = context.GetLazyString(value.Sum.ToInvariantString()))
                        writer.WriteDouble(new LazyNumberValue(lazyStringValue));
                }
                else
                    writer.WriteNull();

                writer.WriteEndObject();
            }
            writer.WriteEndArray();

            writer.WriteEndObject();
        }

        public static void WriteIndexEntriesQueryResult(this BlittableJsonTextWriter writer, JsonOperationContext context, IndexEntriesQueryResult result)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(result.TotalResults));
            writer.WriteInteger(result.TotalResults);
            writer.WriteComma();

            writer.WritePropertyName(nameof(result.SkippedResults));
            writer.WriteInteger(result.SkippedResults);
            writer.WriteComma();

            writer.WritePropertyName(nameof(result.DurationInMs));
            writer.WriteInteger(result.DurationInMs);
            writer.WriteComma();

            writer.WriteQueryResult(context, result, metadataOnly: false, numberOfResults: out int _, partial: true);

            writer.WriteEndObject();
        }

        public static void WriteDocumentQueryResult(this BlittableJsonTextWriter writer, JsonOperationContext context, DocumentQueryResult result, bool metadataOnly, out int numberOfResults)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(result.TotalResults));
            writer.WriteInteger(result.TotalResults);
            writer.WriteComma();

            writer.WritePropertyName(nameof(result.SkippedResults));
            writer.WriteInteger(result.SkippedResults);
            writer.WriteComma();

            writer.WritePropertyName(nameof(result.DurationInMs));
            writer.WriteInteger(result.DurationInMs);
            writer.WriteComma();


            writer.WriteArray(nameof(result.IncludedPaths),
                result.IncludedPaths);
            writer.WriteComma();

            writer.WriteQueryResult(context, result, metadataOnly, out numberOfResults, partial: true);

            writer.WriteEndObject();
        }

        public static void WriteMoreLikeThisQueryResult(this BlittableJsonTextWriter writer, JsonOperationContext context, MoreLikeThisQueryResultServerSide result, out int numberOfResults)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(result.DurationInMs));
            writer.WriteInteger(result.DurationInMs);
            writer.WriteComma();

            writer.WriteQueryResult(context, result, metadataOnly: false, numberOfResults: out numberOfResults, partial: true);

            writer.WriteEndObject();
        }

        public static void WriteSuggestionQueryResult(this BlittableJsonTextWriter writer, JsonOperationContext context, SuggestionQueryResultServerSide result)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(result.DurationInMs));
            writer.WriteInteger(result.DurationInMs);
            writer.WriteComma();

            writer.WritePropertyName(nameof(result.IndexName));
            writer.WriteString(result.IndexName);
            writer.WriteComma();

            writer.WriteArray(nameof(result.Suggestions), result.Suggestions);
            writer.WriteComma();

            writer.WritePropertyName(nameof(result.IndexTimestamp));
            writer.WriteString(result.IndexTimestamp.ToString(Default.DateTimeFormatsToWrite));
            writer.WriteComma();

            writer.WritePropertyName(nameof(result.LastQueryTime));
            writer.WriteString(result.LastQueryTime.ToString(Default.DateTimeFormatsToWrite));
            writer.WriteComma();

            writer.WritePropertyName(nameof(result.IsStale));
            writer.WriteBool(result.IsStale);
            writer.WriteComma();

            writer.WritePropertyName(nameof(result.ResultEtag));
            writer.WriteInteger(result.ResultEtag);

            writer.WriteEndObject();
        }

        public static void WriteQueryResult<T>(this BlittableJsonTextWriter writer, JsonOperationContext context, QueryResultBase<T> result, bool metadataOnly, out int numberOfResults, bool partial = false)
        {
            if (partial == false)
                writer.WriteStartObject();

            writer.WritePropertyName(nameof(result.IndexName));
            writer.WriteString(result.IndexName);
            writer.WriteComma();

            var type = typeof(T);
            if (type == typeof(List<Document>))
            {
                writer.WritePropertyName(nameof(result.Results));
                writer.WriteDocuments(context, (List<Document>)(object)result.Results, metadataOnly, out numberOfResults);
                writer.WriteComma();

                writer.WritePropertyName(nameof(result.Includes));
                writer.WriteDocuments(context, (List<Document>)(object)result.Includes, metadataOnly, out int _);
                writer.WriteComma();
            }
            else if (type == typeof(List<BlittableJsonReaderObject>))
            {
                writer.WritePropertyName(nameof(result.Results));
                writer.WriteObjects(context, (List<BlittableJsonReaderObject>)(object)result.Results, out numberOfResults);
                writer.WriteComma();

                writer.WritePropertyName(nameof(result.Includes));
                writer.WriteObjects(context, (List<BlittableJsonReaderObject>)(object)result.Includes, out int _);
                writer.WriteComma();
            }
            else
                throw new NotSupportedException($"Cannot write query result of '{type.Name}' type.");

            writer.WritePropertyName(nameof(result.IndexTimestamp));
            writer.WriteString(result.IndexTimestamp.ToString(Default.DateTimeFormatsToWrite));
            writer.WriteComma();

            writer.WritePropertyName(nameof(result.LastQueryTime));
            writer.WriteString(result.LastQueryTime.ToString(Default.DateTimeFormatsToWrite));
            writer.WriteComma();

            writer.WritePropertyName(nameof(result.IsStale));
            writer.WriteBool(result.IsStale);
            writer.WriteComma();

            writer.WritePropertyName(nameof(result.ResultEtag));
            writer.WriteInteger(result.ResultEtag);

            if (partial == false)
                writer.WriteEndObject();
        }

        public static void WriteTermsQueryResult(this BlittableJsonTextWriter writer, JsonOperationContext context, TermsQueryResultServerSide queryResult)
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

        public static void WriteIndexingPerformanceStats(this BlittableJsonTextWriter writer, JsonOperationContext context, IndexingPerformanceStats stats)
        {
            var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(stats);
            writer.WriteObject(context.ReadObject(djv, "index/performance"));
        }

        public static void WriteIndexQuery(this BlittableJsonTextWriter writer, JsonOperationContext context, IIndexQuery query)
        {
            var indexQuery = query as IndexQueryServerSide;
            if (indexQuery != null)
            {
                writer.WriteIndexQuery(context, indexQuery);
                return;
            }

            var moreLikeThisQuery = query as MoreLikeThisQueryServerSide;
            if (moreLikeThisQuery != null)
            {
                writer.WriteMoreLikeThisQuery(context, moreLikeThisQuery);
                return;
            }

            var facetQuery = query as FacetQuery;
            if (facetQuery != null)
            {
                writer.WriteFacetQuery(context, facetQuery);
                return;
            }

            throw new NotSupportedException($"Not supported query type: {query.GetType()}");
        }

        public static void WriteFacetQuery(this BlittableJsonTextWriter writer, JsonOperationContext context, FacetQuery query)
        {
            var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(query);
            var json = context.ReadObject(djv, "facet-query");

            writer.WriteObject(json);
        }

        public static void WriteMoreLikeThisQuery(this BlittableJsonTextWriter writer, JsonOperationContext context, MoreLikeThisQueryServerSide query)
        {
            var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(query);
            var json = context.ReadObject(djv, "more-like-this-query");

            writer.WriteObject(json);
        }

        public static void WriteIndexQuery(this BlittableJsonTextWriter writer, JsonOperationContext context, IndexQueryServerSide query)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(query.CutoffEtag));
            if (query.CutoffEtag.HasValue)
                writer.WriteInteger(query.CutoffEtag.Value);
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(nameof(query.DisableCaching));
            writer.WriteBool(query.DisableCaching);
            writer.WriteComma();

            writer.WritePropertyName(nameof(query.ExplainScores));
            writer.WriteBool(query.ExplainScores);
            writer.WriteComma();

            writer.WritePropertyName(nameof(query.HighlighterKeyName));
            if (query.HighlighterKeyName != null)
                writer.WriteString(query.HighlighterKeyName);
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(nameof(query.PageSize));
            writer.WriteInteger(query.PageSize);
            writer.WriteComma();

            writer.WritePropertyName(nameof(query.Query));
            if (query.Query != null)
                writer.WriteString(query.Query);
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(nameof(query.ShowTimings));
            writer.WriteBool(query.ShowTimings);
            writer.WriteComma();

            writer.WritePropertyName(nameof(query.SkipDuplicateChecking));
            writer.WriteBool(query.SkipDuplicateChecking);
            writer.WriteComma();

            writer.WritePropertyName(nameof(query.Start));
            writer.WriteInteger(query.Start);
            writer.WriteComma();

            writer.WritePropertyName(nameof(query.WaitForNonStaleResults));
            writer.WriteBool(query.WaitForNonStaleResults);
            writer.WriteComma();

            writer.WritePropertyName(nameof(query.WaitForNonStaleResultsAsOfNow));
            writer.WriteBool(query.WaitForNonStaleResultsAsOfNow);
            writer.WriteComma();

            writer.WritePropertyName(nameof(query.WaitForNonStaleResultsTimeout));
            if (query.WaitForNonStaleResultsTimeout.HasValue)
                writer.WriteString(query.WaitForNonStaleResultsTimeout.Value.ToString());
            else
                writer.WriteNull();
            writer.WriteComma();

            var isFirstInternal = true;

            writer.WritePropertyName(nameof(query.HighlightedFields));
            writer.WriteStartArray();
            if (query.HighlightedFields != null)
            {
                foreach (var field in query.HighlightedFields)
                {
                    if (isFirstInternal == false)
                        writer.WriteComma();

                    isFirstInternal = false;

                    writer.WriteStartObject();

                    writer.WritePropertyName(nameof(field.Field));
                    writer.WriteString(field.Field);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(field.FragmentCount));
                    writer.WriteInteger(field.FragmentCount);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(field.FragmentLength));
                    writer.WriteInteger(field.FragmentLength);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(field.FragmentsField));
                    writer.WriteString(field.FragmentsField);

                    writer.WriteEndObject();
                }
            }
            writer.WriteEndArray();
            writer.WriteComma();

            writer.WritePropertyName(nameof(query.HighlighterPostTags));
            writer.WriteStartArray();
            if (query.HighlighterPostTags != null)
            {
                isFirstInternal = true;
                foreach (var tag in query.HighlighterPostTags)
                {
                    if (isFirstInternal == false)
                        writer.WriteComma();

                    isFirstInternal = false;

                    writer.WriteString(tag);
                }
            }
            writer.WriteEndArray();
            writer.WriteComma();

            writer.WritePropertyName(nameof(query.HighlighterPreTags));
            writer.WriteStartArray();
            if (query.HighlighterPreTags != null)
            {
                isFirstInternal = true;
                foreach (var tag in query.HighlighterPreTags)
                {
                    if (isFirstInternal == false)
                        writer.WriteComma();

                    isFirstInternal = false;

                    writer.WriteString(tag);
                }
            }
            writer.WriteEndArray();

            writer.WriteEndObject();
        }

        public static void WriteDatabaseStatistics(this BlittableJsonTextWriter writer, JsonOperationContext context, DatabaseStatistics statistics)
        {
            writer.WriteStartObject();

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

            writer.WritePropertyName(nameof(statistics.CountOfAttachments));
            writer.WriteInteger(statistics.CountOfAttachments);
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

            writer.WritePropertyName((nameof(statistics.DatabaseChangeVector)));
            writer.WriteString(statistics.DatabaseChangeVector);
            writer.WriteComma();

            writer.WritePropertyName(nameof(statistics.LastIndexingTime));
            if (statistics.LastIndexingTime.HasValue)
                writer.WriteDateTime(statistics.LastIndexingTime.Value, isUtc: true);
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(nameof(statistics.Indexes));
            writer.WriteStartArray();
            var isFirstInternal = true;
            foreach (var index in statistics.Indexes)
            {
                if (isFirstInternal == false)
                    writer.WriteComma();

                isFirstInternal = false;

                writer.WriteStartObject();

                writer.WritePropertyName(nameof(index.IsStale));
                writer.WriteBool(index.IsStale);
                writer.WriteComma();

                writer.WritePropertyName(nameof(index.Name));
                writer.WriteString(index.Name);
                writer.WriteComma();

                writer.WritePropertyName(nameof(index.Etag));
                writer.WriteInteger(index.Etag);
                writer.WriteComma();

                writer.WritePropertyName(nameof(index.LockMode));
                writer.WriteString(index.LockMode.ToString());
                writer.WriteComma();

                writer.WritePropertyName(nameof(index.Priority));
                writer.WriteString(index.Priority.ToString());
                writer.WriteComma();

                writer.WritePropertyName(nameof(index.State));
                writer.WriteString(index.State.ToString());
                writer.WriteComma();

                writer.WritePropertyName(nameof(index.Type));
                writer.WriteString(index.Type.ToString());
                writer.WriteComma();

                writer.WritePropertyName(nameof(index.LastIndexingTime));
                if (index.LastIndexingTime.HasValue)
                    writer.WriteDateTime(index.LastIndexingTime.Value, isUtc: true);
                else
                    writer.WriteNull();

                writer.WriteEndObject();
            }
            writer.WriteEndArray();

            writer.WriteEndObject();
        }

        public static void WriteIndexDefinition(this BlittableJsonTextWriter writer, JsonOperationContext context, IndexDefinition indexDefinition, bool removeAnalyzers = false)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(indexDefinition.Name));
            writer.WriteString(indexDefinition.Name);
            writer.WriteComma();

            writer.WritePropertyName(nameof(indexDefinition.Etag));
            writer.WriteInteger(indexDefinition.Etag);
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

            writer.WritePropertyName(nameof(indexDefinition.Priority));
            if (indexDefinition.Priority.HasValue)
                writer.WriteString(indexDefinition.Priority.ToString());
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(nameof(indexDefinition.OutputReduceToCollection));
            writer.WriteString(indexDefinition.OutputReduceToCollection);
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

            writer.WritePropertyName(nameof(indexDefinition.IsTestIndex));
            writer.WriteBool(indexDefinition.IsTestIndex);
            writer.WriteComma();

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
                    writer.WriteIndexFieldOptions(context, kvp.Value, removeAnalyzers);
                else
                    writer.WriteNull();
            }
            writer.WriteEndObject();

            writer.WriteEndObject();
        }

        public static void WriteIndexProgress(this BlittableJsonTextWriter writer, JsonOperationContext context, IndexProgress progress)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(progress.IsStale));
            writer.WriteBool(progress.IsStale);
            writer.WriteComma();

            writer.WritePropertyName(nameof(progress.Collections));
            if (progress.Collections != null)
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

                    writer.WritePropertyName(nameof(kvp.Value.LastProcessedDocumentEtag));
                    writer.WriteInteger(kvp.Value.LastProcessedDocumentEtag);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(kvp.Value.LastProcessedTombstoneEtag));
                    writer.WriteInteger(kvp.Value.LastProcessedTombstoneEtag);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(kvp.Value.NumberOfDocumentsToProcess));
                    writer.WriteInteger(kvp.Value.NumberOfDocumentsToProcess);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(kvp.Value.NumberOfTombstonesToProcess));
                    writer.WriteInteger(kvp.Value.NumberOfTombstonesToProcess);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(kvp.Value.TotalNumberOfDocuments));
                    writer.WriteInteger(kvp.Value.TotalNumberOfDocuments);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(kvp.Value.TotalNumberOfTombstones));
                    writer.WriteInteger(kvp.Value.TotalNumberOfTombstones);

                    writer.WriteEndObject();
                }
                writer.WriteEndObject();
            }
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(nameof(progress.Name));
            writer.WriteString(progress.Name);
            writer.WriteComma();

            writer.WritePropertyName(nameof(progress.Type));
            writer.WriteString(progress.Type.ToString());
            writer.WriteComma();

            writer.WritePropertyName(nameof(progress.Etag));
            writer.WriteInteger(progress.Etag);

            writer.WriteEndObject();
        }

        public static void WriteIndexStats(this BlittableJsonTextWriter writer, JsonOperationContext context, IndexStats stats)
        {
            var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(stats);
            writer.WriteObject(context.ReadObject(djv, "index/stats"));
        }

        private static void WriteIndexFieldOptions(this BlittableJsonTextWriter writer, JsonOperationContext context, IndexFieldOptions options, bool removeAnalyzers)
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

        public static void WriteDocuments(this BlittableJsonTextWriter writer, JsonOperationContext context, IEnumerable<Document> documents, bool metadataOnly, out int numberOfResults)
        {
            numberOfResults = 0;

            writer.WriteStartArray();

            var first = true;
            foreach (var document in documents)
            {
                numberOfResults++;

                if (first == false)
                    writer.WriteComma();
                first = false;

                if (document == null)
                {
                    writer.WriteNull();
                    continue;
                }

                if (document == Document.ExplicitNull)
                {
                    writer.WriteNull();
                    continue;
                }

                using (document.Data)
                {
                    if (metadataOnly == false)
                        writer.WriteDocument(context, document);
                    else
                        writer.WriteDocumentMetadata(context, document);
                }
            }

            writer.WriteEndArray();
        }

        public static void WriteObjects(this BlittableJsonTextWriter writer, JsonOperationContext context, IEnumerable<BlittableJsonReaderObject> objects, out int numberOfResults)
        {
            numberOfResults = 0;

            writer.WriteStartArray();

            var first = true;
            foreach (var o in objects)
            {
                numberOfResults++;

                if (o == null)
                    continue;

                if (first == false)
                    writer.WriteComma();
                first = false;

                using (o)
                {
                    writer.WriteObject(o);
                }
            }

            writer.WriteEndArray();
        }

        [ThreadStatic]
        private static BlittableJsonReaderObject.PropertiesInsertionBuffer _buffers;

        public static void WriteDocumentMetadata(this BlittableJsonTextWriter writer, JsonOperationContext context,
            Document document)
        {
            if (_buffers == null)
                _buffers = new BlittableJsonReaderObject.PropertiesInsertionBuffer();

            writer.WriteStartObject();
            document.Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata);
            WriteMetadata(writer, document, metadata);

            writer.WriteEndObject();
        }

        public static void WriteMetadata(this BlittableJsonTextWriter writer, Document document, BlittableJsonReaderObject metadata)
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
                    if (first == false)
                    {
                        writer.WriteComma();
                    }
                    first = false;
                    metadata.GetPropertyByIndex(i, ref prop);
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
            if (document.LastModified != DateTime.MinValue)
            {
                writer.WriteComma();
                writer.WritePropertyName(Constants.Documents.Metadata.LastModified);
                writer.WriteDateTime(document.LastModified, isUtc: true);
            }
            writer.WriteEndObject();
        }

        private static readonly StringSegment MetadataKeySegment = new StringSegment(Constants.Documents.Metadata.Key);

        public static void WriteDocument(this BlittableJsonTextWriter writer, JsonOperationContext context, Document document)
        {
            writer.WriteStartObject();
            WriteDocumentProperties(writer, context, document);
            writer.WriteEndObject();
        }

        public static void WriteDocumentProperties(this BlittableJsonTextWriter writer, JsonOperationContext context, Document document)
        {
            if (_buffers == null)
                _buffers = new BlittableJsonReaderObject.PropertiesInsertionBuffer();

            var first = true;
            BlittableJsonReaderObject metadata = null;
            var metadataField = context.GetLazyStringForFieldWithCaching(MetadataKeySegment);

            var size = document.Data.GetPropertiesByInsertionOrder(_buffers);
            var prop = new BlittableJsonReaderObject.PropertyDetails();

            for (var i = 0; i < size; i++)
            {
                document.Data.GetPropertyByIndex(_buffers.Properties[i], ref prop);
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

            if (first == false)
                writer.WriteComma();
            WriteMetadata(writer, document, metadata);
        }

        public static void WriteDocumentPropertiesWithoutMetdata(this BlittableJsonTextWriter writer, JsonOperationContext context, Document document)
        {
            if (_buffers == null)
                _buffers = new BlittableJsonReaderObject.PropertiesInsertionBuffer();

            var first = true;

            var size = document.Data.GetPropertiesByInsertionOrder(_buffers);
            var prop = new BlittableJsonReaderObject.PropertyDetails();

            for (var i = 0; i < size; i++)
            {
                document.Data.GetPropertyByIndex(_buffers.Properties[i], ref prop);
                if (first == false)
                {
                    writer.WriteComma();
                }
                first = false;
                writer.WritePropertyName(prop.Name);
                writer.WriteValue(prop.Token & BlittableJsonReaderBase.TypesMask, prop.Value);
            }
        }

    public static void WriteOperationId(this BlittableJsonTextWriter writer, JsonOperationContext context, long operationId)
    {
        writer.WriteStartObject();

        writer.WritePropertyName("OperationId");
        writer.WriteInteger(operationId);

        writer.WriteEndObject();
    }

    public static void WriteArrayOfResultsAndCount(this BlittableJsonTextWriter writer, IEnumerable<string> results)
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

    public static void WriteReduceTrees(this BlittableJsonTextWriter writer, IEnumerable<ReduceTree> trees)
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

    public static void WriteTreePagesRecursively(this BlittableJsonTextWriter writer, IEnumerable<ReduceTreePage> pages)
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
}
}
