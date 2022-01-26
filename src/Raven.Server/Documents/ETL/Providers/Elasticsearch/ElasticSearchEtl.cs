using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Elasticsearch.Net;
using Nest;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Server.Documents.ETL.Providers.ElasticSearch.Enumerators;
using Raven.Server.Documents.ETL.Providers.ElasticSearch.Test;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Exceptions.ETL.ElasticSearch;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Json.Sync;

namespace Raven.Server.Documents.ETL.Providers.ElasticSearch
{
    public class ElasticSearchEtl : EtlProcess<ElasticSearchItem, ElasticSearchIndexWithRecords, ElasticSearchEtlConfiguration, ElasticSearchConnectionString, EtlStatsScope, EtlPerformanceOperation>
    {
        internal const string IndexBulkAction = @"{""index"":{""_id"":null}}";

        internal static byte[] IndexBulkActionBytes = Encoding.UTF8.GetBytes(IndexBulkAction);

        private readonly HashSet<string> _existingIndexes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        public readonly ElasticSearchEtlMetricsCountersManager ElasticSearchMetrics = new ElasticSearchEtlMetricsCountersManager();

        public ElasticSearchEtl(Transformation transformation, ElasticSearchEtlConfiguration configuration, DocumentDatabase database, ServerStore serverStore)
            : base(transformation, configuration, database, serverStore, ElasticSearchEtlTag)
        {
            Metrics = ElasticSearchMetrics;
        }

        public const string ElasticSearchEtlTag = "ElasticSearch ETL";

        public override EtlType EtlType => EtlType.ElasticSearch;

        public override bool ShouldTrackCounters() => false;

        public override bool ShouldTrackTimeSeries() => false;

        protected override bool ShouldTrackAttachmentTombstones() => false;

        private ElasticClient _client;

        protected override EtlStatsScope CreateScope(EtlRunStats stats)
        {
            return new EtlStatsScope(stats);
        }

        protected override bool ShouldFilterOutHiLoDocument() => true;

        protected override IEnumerator<ElasticSearchItem> ConvertDocsEnumerator(DocumentsOperationContext context, IEnumerator<Document> docs, string collection)
        {
            return new DocumentsToElasticSearchItems(docs, collection);
        }

        protected override IEnumerator<ElasticSearchItem> ConvertTombstonesEnumerator(DocumentsOperationContext context, IEnumerator<Tombstone> tombstones, string collection,
            bool trackAttachments)
        {
            return new TombstonesToElasticSearchItems(tombstones, collection);
        }

        protected override IEnumerator<ElasticSearchItem> ConvertAttachmentTombstonesEnumerator(DocumentsOperationContext context, IEnumerator<Tombstone> tombstones,
            List<string> collections)
        {
            throw new NotSupportedException("Attachment tombstones aren't supported by ElasticSearch ETL");
        }

        protected override IEnumerator<ElasticSearchItem> ConvertCountersEnumerator(DocumentsOperationContext context, IEnumerator<CounterGroupDetail> counters,
            string collection)
        {
            throw new NotSupportedException("Counters aren't supported by ElasticSearch ETL");
        }

        protected override IEnumerator<ElasticSearchItem> ConvertTimeSeriesEnumerator(DocumentsOperationContext context, IEnumerator<TimeSeriesSegmentEntry> timeSeries,
            string collection)
        {
            throw new NotSupportedException("Time series aren't supported by ElasticSearch ETL");
        }

        protected override IEnumerator<ElasticSearchItem> ConvertTimeSeriesDeletedRangeEnumerator(DocumentsOperationContext context,
            IEnumerator<TimeSeriesDeletedRangeItem> timeSeries, string collection)
        {
            throw new NotSupportedException("Time series aren't supported by ElasticSearch ETL");
        }

        protected override EtlTransformer<ElasticSearchItem, ElasticSearchIndexWithRecords, EtlStatsScope, EtlPerformanceOperation> GetTransformer(DocumentsOperationContext context)
        {
            return new ElasticSearchDocumentTransformer(Transformation, Database, context, Configuration);
        }

        protected override int LoadInternal(IEnumerable<ElasticSearchIndexWithRecords> records, DocumentsOperationContext context, EtlStatsScope scope)
        {
            int count = 0;

            _client ??= ElasticSearchHelper.CreateClient(Configuration.Connection);

            foreach (var index in records)
            {
                string indexName = index.IndexName.ToLower();

                EnsureIndexExistsAndValidateIfNeeded(indexName, index);

                if (index.InsertOnlyMode == false)
                    count += DeleteByQueryOnIndexIdProperty(index);

                if (index.Inserts.Count > 0)
                {
                    var streamHandler = PostData.StreamHandler(index.Inserts, (inserts, stream) =>
                    {
                        foreach (ElasticSearchItem insert in inserts)
                        {
                            if (insert.TransformationResult == null)
                                continue;

                            stream.Write(IndexBulkActionBytes);

                            using (var json = EnsureLowerCasedIndexIdProperty(context, insert.TransformationResult, index))
                            using (var writer = new BlittableJsonTextWriter(context, stream))
                            {
                                writer.WriteNewLine();
                                writer.WriteObject(json);
                                writer.WriteNewLine();
                            }

                            count++;
                        }
                    }, (i, s, token) => throw new NotSupportedException("We don't use async bulk method"));

                    
                    var bulkIndexResponse = _client.LowLevel.Bulk<BulkResponse>(indexName, streamHandler, new BulkRequestParameters { Refresh = Refresh.WaitFor });

                    if (bulkIndexResponse.IsValid == false)
                        ThrowElasticSearchLoadException($"Failed to index data to '{indexName}' index", bulkIndexResponse.ServerError, bulkIndexResponse.OriginalException,
                            bulkIndexResponse.DebugInformation);
                }
            }

            return count;
        }

        internal static BlittableJsonReaderObject EnsureLowerCasedIndexIdProperty(DocumentsOperationContext context, BlittableJsonReaderObject json,
            ElasticSearchIndexWithRecords index)
        {
            if (json.TryGet(index.DocumentIdProperty, out LazyStringValue idProperty))
            {
                using (var old = json)
                {
                    json.Modifications = new DynamicJsonValue(json) { [index.DocumentIdProperty] = LowerCaseDocumentIdProperty(idProperty) };

                    json = context.ReadObject(json, "es-etl-load");
                }
            }
            else if (json.Modifications != null)
            {
                // document id property was not added by user, so we inserted the lowercased id in ElasticSearchDocumentTransformer.LoadToFunction
#if DEBUG
                var docIdProperty = json.Modifications.Properties.First(x => x.Name == index.DocumentIdProperty);

                Debug.Assert(docIdProperty.Value.ToString() == docIdProperty.Value.ToString().ToLowerInvariant());
#endif

                json = context.ReadObject(json, "es-etl-load");
            }

            return json;
        }

        private int DeleteByQueryOnIndexIdProperty(ElasticSearchIndexWithRecords index)
        {
            string indexName = index.IndexName.ToLower();

            var idsToDelete = new List<string>();

            foreach (ElasticSearchItem delete in index.Deletes)
            {
                idsToDelete.Add(LowerCaseDocumentIdProperty(delete.DocumentId));
            }

            var deleteResponse = _client.DeleteByQuery<string>(d => d
                .Index(indexName)
                .Refresh()
                .Query(q => q
                    .Terms(p => p
                        .Field(index.DocumentIdProperty)
                        .Terms((IEnumerable<string>)idsToDelete))
                )
            );

            if (deleteResponse.IsValid == false)
                ThrowElasticSearchLoadException($"Failed to delete by query from index '{index}'. Documents IDs: {string.Join(',', idsToDelete)}",
                    deleteResponse.ServerError, deleteResponse.OriginalException, deleteResponse.DebugInformation);

            return (int)deleteResponse.Deleted;
        }

        private void EnsureIndexExistsAndValidateIfNeeded(string indexName, ElasticSearchIndexWithRecords index)
        {
            if (_existingIndexes.Contains(indexName) == false)
            {
                var indexResponse = _client.Indices.Get(new GetIndexDescriptor(Indices.Index(indexName)));

                if (indexResponse.Indices.TryGetValue(indexName, out var state))
                {
                    var mappingsProperties = state.Mappings.Properties;

                    if (mappingsProperties.TryGetValue(new PropertyName(index.DocumentIdProperty), out var propertyDefinition) == false)
                        throw new ElasticSearchLoadException(
                            $"The index '{indexName}' doesn't contain the mapping for '{index.DocumentIdProperty}' property. " +
                            "This property is meant to store RavenDB document ID so it needs to be defined as a non-analyzed field, with type 'keyword' to avoid having full-text-search on it.");

                    if (propertyDefinition.Type == null || propertyDefinition.Type.Equals("keyword", StringComparison.OrdinalIgnoreCase) == false)
                        throw new ElasticSearchLoadException(
                            $"The index '{indexName}' has invalid mapping for '{index.DocumentIdProperty}' property. " +
                            "This property is meant to store RavenDB document ID so it needs to be defined as a non-analyzed field, with type 'keyword' to avoid having full-text-search on it.");
                }
                else
                {
                    CreateDefaultIndex(indexName, index);
                }

                _existingIndexes.Add(indexName);
            }
        }

        private void CreateDefaultIndex(string indexName, ElasticSearchIndexWithRecords index)
        {
            var response = _client.Indices.Create(indexName, c => c
                .Map(m => m
                    .Properties(p => p
                        .Keyword(t => t
                            .Name(index.DocumentIdProperty)))));

            // The request made it to the server but something went wrong in ElasticSearch (query parsing exception, non-existent index, etc)
            if (response.ServerError != null)
                throw new ElasticSearchLoadException(
                    $"Failed to create '{indexName}' index. Error: {response.ServerError.Error}. Debug Information: {response.DebugInformation}");

            // ElasticSearch error occurred or a connection error (the server could not be reached, request timed out, etc)
            if (response.OriginalException != null)
                throw new ElasticSearchLoadException($"Failed to create '{indexName}' index. Debug Information: {response.DebugInformation}", response.OriginalException);
        }

        internal static string LowerCaseDocumentIdProperty(LazyStringValue id)
        {
            return id.ToLowerInvariant();
        }

        private void ThrowElasticSearchLoadException(string message, ServerError serverError, Exception originalException, string debugInformation)
        {
            if (serverError != null)
                message += $". Server error: {serverError}";

            if (string.IsNullOrEmpty(debugInformation) == false)
                message += $". Debug information: {debugInformation}";

            throw new ElasticSearchLoadException(message, originalException);
        }

        public ElasticSearchEtlTestScriptResult RunTest(IEnumerable<ElasticSearchIndexWithRecords> records, DocumentsOperationContext context)
        {
            var simulatedWriter = new ElasticSearchIndexWriterSimulator();
            var summaries = new List<IndexSummary>();
            
            foreach (var record in records)
            {
                var commands = simulatedWriter.SimulateExecuteCommandText(record, context);
                
                summaries.Add(new IndexSummary
                {
                    IndexName = record.IndexName.ToLower(),
                    Commands = commands.ToArray()
                });
            }
            
            return new ElasticSearchEtlTestScriptResult
            {
                TransformationErrors = Statistics.TransformationErrorsInCurrentBatch.Errors.ToList(),
                Summary = summaries
            };
        }
    }
}
