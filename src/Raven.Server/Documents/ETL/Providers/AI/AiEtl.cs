#pragma warning disable SKEXP0001, SKEXP0010
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Microsoft.SemanticKernel.Connectors.OpenAI;
using Microsoft.SemanticKernel.Embeddings;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.AI;
using Raven.Client.Http;
using Raven.Client.Util;
using Raven.Server.Documents.ETL.Providers.AI.Enumerators;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Handlers.Processors.TimeSeries;
using Raven.Server.Documents.Indexes.VectorSearch;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.AI;

public sealed class AiEtl : EtlProcess<AiEtlItem, KeyValuePair<string, Dictionary<string, List<string>>>, AiEtlConfiguration, AiConnectionString, EtlStatsScope, EtlPerformanceOperation>
{
    private readonly AiEtlConfiguration _configuration;
    private readonly ServerStore _serverStore;
    private ITextEmbeddingGenerationService _service;
    
    public const string AiEtlTag = "AI ETL";
    
    public AiEtl(Transformation transformation, AiEtlConfiguration configuration, DocumentDatabase database, ServerStore serverStore) : base(transformation, configuration, database, serverStore, AiEtlTag)
    {
        _configuration = configuration;
        _serverStore = serverStore;
    }

    public override EtlType EtlType => EtlType.OpenAi;
    public override bool ShouldTrackCounters() => false;
    public override bool ShouldTrackTimeSeries() => false;

    protected override IEnumerator<AiEtlItem> ConvertDocsEnumerator(DocumentsOperationContext context, IEnumerator<Document> docs, string collection)
    {
        return new DocumentsToAiEtlItems(docs, collection);
    }

    protected override IEnumerator<AiEtlItem> ConvertTombstonesEnumerator(DocumentsOperationContext context, IEnumerator<Tombstone> tombstones, string collection, bool trackAttachments)
    {
        return new TombstonesToAiEtlItems(context, tombstones, collection, trackAttachments);
    }

    protected override IEnumerator<AiEtlItem> ConvertAttachmentTombstonesEnumerator(DocumentsOperationContext context, IEnumerator<Tombstone> tombstones, List<string> collections)
    {
        throw new System.NotImplementedException();
    }

    protected override IEnumerator<AiEtlItem> ConvertCountersEnumerator(DocumentsOperationContext context, IEnumerator<CounterGroupDetail> counters, string collection)
    {
        throw new System.NotImplementedException();
    }

    protected override IEnumerator<AiEtlItem> ConvertTimeSeriesEnumerator(DocumentsOperationContext context, IEnumerator<TimeSeriesSegmentEntry> timeSeries, string collection)
    {
        throw new System.NotImplementedException();
    }

    protected override IEnumerator<AiEtlItem> ConvertTimeSeriesDeletedRangeEnumerator(DocumentsOperationContext context, IEnumerator<TimeSeriesDeletedRangeItem> timeSeries, string collection)
    {
        throw new System.NotImplementedException();
    }

    protected override bool ShouldTrackAttachmentTombstones()
    {
        return false;
    }
    
    protected override EtlTransformer<AiEtlItem, KeyValuePair<string, Dictionary<string, List<string>>>, EtlStatsScope, EtlPerformanceOperation> GetTransformer(DocumentsOperationContext context)
    {
        return new AiEtlDocumentTransformer(Database, context, null, null, _configuration);
    }

    protected override int LoadInternal(IEnumerable<KeyValuePair<string, Dictionary<string, List<string>>>> items, DocumentsOperationContext context, EtlStatsScope scope)
    {
        //_service ??= CreateService(Configuration.Connection);

        int processed = 0;
        
        foreach (var documentData in items)
        {
            var originalDocumentId = documentData.Key;
            var newDocumentId = GetNewDocumentId(originalDocumentId);

            var documentDjv = new DynamicJsonValue { ["Id"] = newDocumentId };

            var embeddingsObjectDjv = new DynamicJsonValue();

            foreach ((string fieldName, List<string> fieldValues) in documentData.Value)
            {
                var dja = new DynamicJsonArray();
                
                foreach (var s in fieldValues)
                {
                    var embeddingDja = new DynamicJsonArray();
                    
                    var embedding = GenerateEmbeddings.FromText(context.Allocator, VectorOptions.DefaultText, s).GetEmbedding().ToArray();

                    foreach (var b in embedding)
                    {
                        embeddingDja.Add(b);
                    }
                    
                    dja.Add(embeddingDja);
                }

                embeddingsObjectDjv[fieldName] = dja;
            }

            documentDjv[_configuration.Name] = embeddingsObjectDjv;

            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                var bjro = ctx.ReadObject(documentDjv, "doc");
                
                var cmd = new MergedPutCommand(bjro, newDocumentId, null, Database);
                
                Database.TxMerger.EnqueueSync(cmd);
            }

            processed++;
        }

        return processed;
    }

    private static string GetNewDocumentId(string originalDocumentId)
    {
        return $"{originalDocumentId}/embeddings";
    }

    protected override EtlStatsScope CreateScope(EtlRunStats stats)
    {
        return new EtlStatsScope(stats);
    }

    protected override bool ShouldFilterOutHiLoDocument()
    {
        throw new System.NotImplementedException();
    }

    /*
    private ITextEmbeddingGenerationService CreateService(OpenAiConnectionString connectionString)
    {
        var service = new OpenAITextEmbeddingGenerationService(
            "text-embedding-ada-002",
            "https://{myservice}.openai.azure.com/",
            "apikey");

        return service;
    }
    */
}
#pragma warning restore SKEXP0001, SKEXP0010
