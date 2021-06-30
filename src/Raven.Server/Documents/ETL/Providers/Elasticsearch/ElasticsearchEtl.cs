using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Elasticsearch.Net;
using Nest;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Elasticsearch;
using Raven.Server.Documents.ETL.Providers.Elasticsearch.Enumerators;
using Raven.Server.Documents.ETL.Providers.Elasticsearch.Test;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.Elasticsearch
{
    public class ElasticsearchEtl : EtlProcess<ElasticsearchItem, ElasticsearchIndexWithRecords, ElasticsearchEtlConfiguration, ElasticsearchConnectionString, EtlStatsScope, EtlPerformanceOperation>
    {
        public ElasticsearchEtl(Transformation transformation, ElasticsearchEtlConfiguration configuration, DocumentDatabase database, ServerStore serverStore)
            : base(transformation, configuration, database, serverStore, ElasticsearchEtlTag)
        {
        }

        public const string ElasticsearchEtlTag = "ELASTICSEARCH ETL";

        public override EtlType EtlType => EtlType.Elasticsearch;

        public override bool ShouldTrackCounters() => false;

        public override bool ShouldTrackTimeSeries() => false;

        protected override bool ShouldTrackAttachmentTombstones() => false;

        protected override EtlStatsScope CreateScope(EtlRunStats stats)
        {
            return new EtlStatsScope(stats);
        }

        protected override bool ShouldFilterOutHiLoDocument() => true;

        protected override IEnumerator<ElasticsearchItem> ConvertDocsEnumerator(DocumentsOperationContext context, IEnumerator<Document> docs, string collection)
        {
            return new DocumentsToElasticsearchItems(docs, collection);
        }

        protected override IEnumerator<ElasticsearchItem> ConvertTombstonesEnumerator(DocumentsOperationContext context, IEnumerator<Tombstone> tombstones, string collection,
            bool trackAttachments)
        {
            return new TombstonesToElasticsearchItems(tombstones, collection);
        }

        protected override IEnumerator<ElasticsearchItem> ConvertAttachmentTombstonesEnumerator(DocumentsOperationContext context, IEnumerator<Tombstone> tombstones,
            List<string> collections)
        {
            throw new NotSupportedException("Attachment tombstones aren't supported by ELASTICSEARCH ETL");
        }

        protected override IEnumerator<ElasticsearchItem> ConvertCountersEnumerator(DocumentsOperationContext context, IEnumerator<CounterGroupDetail> counters,
            string collection)
        {
            throw new NotSupportedException("Counters aren't supported by ELASTICSEARCH ETL");
        }

        protected override IEnumerator<ElasticsearchItem> ConvertTimeSeriesEnumerator(DocumentsOperationContext context, IEnumerator<TimeSeriesSegmentEntry> timeSeries,
            string collection)
        {
            throw new NotSupportedException("Time series aren't supported by ELASTICSEARCH ETL");
        }

        protected override IEnumerator<ElasticsearchItem> ConvertTimeSeriesDeletedRangeEnumerator(DocumentsOperationContext context,
            IEnumerator<TimeSeriesDeletedRangeItem> timeSeries, string collection)
        {
            throw new NotSupportedException("Time series aren't supported by ELASTICSEARCH ETL");
        }

        protected override EtlTransformer<ElasticsearchItem, ElasticsearchIndexWithRecords, EtlStatsScope, EtlPerformanceOperation> GetTransformer(DocumentsOperationContext context)
        {
            return new ElasticsearchDocumentTransformer(Transformation, Database, context, Configuration);
        }

        protected override int LoadInternal(IEnumerable<ElasticsearchIndexWithRecords> records, DocumentsOperationContext context, EtlStatsScope scope)
        {
            Uri[] nodes = Configuration.Connection.Nodes.Select(x => new Uri(x)).ToArray();
            var pool = new StaticConnectionPool(nodes);
            var settings = new ConnectionSettings(pool);
            var client = new ElasticClient(settings);
            int statsCounter = 0;
            
            foreach (var index in records)
            {
                StringBuilder deleteQuery = new StringBuilder();
                
                foreach (ElasticsearchItem delete in index.Deletes)
                {
                    deleteQuery.Append($"{delete.DocumentId},");
                }

                var deleteResponse = client.DeleteByQuery<string>(d => d
                    .Index(index.IndexName.ToLower())
                    .Query(q => q
                        .Match(p => p
                            .Field(index.IndexIdProperty)
                            .Query($"{deleteQuery}"))
                    )
                );
                
                if (deleteResponse.ServerError != null)
                {
                    throw new Exception($"ServerError: {deleteResponse.ServerError.Error}");
                }
                
                if (deleteResponse.OriginalException != null)
                {
                    throw new Exception($"OriginalException: {deleteResponse.OriginalException.Message}");
                }

                statsCounter += (int)deleteResponse.Deleted;

                foreach (ElasticsearchItem insert in index.Inserts)
                {
                    if (insert.Property == null) continue;

                    var response = client.LowLevel.Index<StringResponse>(
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
        
        public ElasticsearchEtlTestScriptResult RunTest(IEnumerable<ElasticsearchIndexWithRecords> records)
        {
            var simulatedWriter = new ElasticsearchIndexWriterSimulator();
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
            
            return new ElasticsearchEtlTestScriptResult
            {
                TransformationErrors = Statistics.TransformationErrorsInCurrentBatch.Errors.ToList(),
                Summary = summaries
            };
        }
    }
}
