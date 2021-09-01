using System;
using System.Collections.Generic;
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
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.ElasticSearch
{
    public class ElasticSearchEtl : EtlProcess<ElasticSearchItem, ElasticSearchIndexWithRecords, ElasticSearchEtlConfiguration, ElasticSearchConnectionString, EtlStatsScope, EtlPerformanceOperation>
    {
        public ElasticSearchEtl(Transformation transformation, ElasticSearchEtlConfiguration configuration, DocumentDatabase database, ServerStore serverStore)
            : base(transformation, configuration, database, serverStore, ElasticSearchEtlTag)
        {
            _client = ElasticSearchHelper.CreateClient(Configuration.Connection);
        }

        public const string ElasticSearchEtlTag = "ElasticSearch ETL";

        public override EtlType EtlType => EtlType.ElasticSearch;

        public override bool ShouldTrackCounters() => false;

        public override bool ShouldTrackTimeSeries() => false;

        protected override bool ShouldTrackAttachmentTombstones() => false;

        private readonly ElasticClient _client;

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
            int statsCounter = 0;
            
            StringBuilder deleteQuery = new StringBuilder();
            
            foreach (var index in records)
            {
                deleteQuery.Clear();
                
                foreach (ElasticSearchItem delete in index.Deletes)
                {
                    deleteQuery.Append($"{delete.DocumentId},");
                }

                var deleteResponse = _client.DeleteByQuery<string>(d => d
                    .Index(index.IndexName.ToLower())
                    .Query(q => q
                        .Match(p => p
                            .Field(index.IndexIdProperty)
                            .Query(deleteQuery.ToString()))
                    )
                );
                
                if (deleteResponse.ServerError != null)
                {
                    // ElasticSearchLoadFailureException, index name, ids, deleteQuery
                    throw new Exception($"ServerError: {deleteResponse.ServerError.Error}");
                }
                
                if (deleteResponse.OriginalException != null)
                {
                    throw new Exception($"OriginalException: {deleteResponse.OriginalException.Message}");
                }

                statsCounter += (int)deleteResponse.Deleted;

                foreach (ElasticSearchItem insert in index.Inserts)
                {
                    if (insert.Property == null) continue;

                    var response = _client.LowLevel.Index<StringResponse>(
                        index: index.IndexName.ToLower(),
                        body: insert.Property.RawValue.ToString(), requestParameters: new IndexRequestParameters(){Refresh = Refresh.WaitFor});

                    if (response.Success == false)
                    {
                        throw new Exception(response.OriginalException.Message);
                    }

                    statsCounter++;
                }
            }

            return statsCounter;
        }
        
        public ElasticSearchEtlTestScriptResult RunTest(IEnumerable<ElasticSearchIndexWithRecords> records)
        {
            var simulatedWriter = new ElasticSearchIndexWriterSimulator();
            var summaries = new List<IndexSummary>();
            
            foreach (var record in records)
            {
                var commands = simulatedWriter.SimulateExecuteCommandText(record);
                
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
