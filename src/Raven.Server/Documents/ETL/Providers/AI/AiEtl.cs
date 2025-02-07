using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.SemanticKernel.Embeddings;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.AI;
using Raven.Client.Util;
using Raven.Server.Documents.ETL.Metrics;
using Raven.Server.Documents.ETL.Providers.AI.Enumerators;
using Raven.Server.Documents.ETL.Providers.AI.Test;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Indexes.VectorSearch;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
#pragma warning disable SKEXP0001

namespace Raven.Server.Documents.ETL.Providers.AI;

public sealed class AiEtl : EtlProcess<AiEtlItem, AiEtlEmbeddingItem, AiEtlConfiguration, AiConnectionString, EtlStatsScope, EtlPerformanceOperation>
{
    private ITextEmbeddingGenerationService _service;

    private readonly MissingEmbeddingsHolder _missingEmbeddingsHolder = new();

    public const string AiEtlTag = "AI ETL";

    public AiEtl(Transformation transformation, AiEtlConfiguration configuration, DocumentDatabase database, ServerStore serverStore)
        : base(transformation, configuration, database, serverStore, AiEtlTag)
    {
        Metrics = new EtlMetricsCountersManager();
    }

    public override EtlType EtlType => EtlType.Ai;
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
        throw new NotImplementedException();
    }

    protected override IEnumerator<AiEtlItem> ConvertCountersEnumerator(DocumentsOperationContext context, IEnumerator<CounterGroupDetail> counters, string collection)
    {
        throw new NotImplementedException();
    }

    protected override IEnumerator<AiEtlItem> ConvertTimeSeriesEnumerator(DocumentsOperationContext context, IEnumerator<TimeSeriesSegmentEntry> timeSeries, string collection)
    {
        throw new NotImplementedException();
    }

    protected override IEnumerator<AiEtlItem> ConvertTimeSeriesDeletedRangeEnumerator(DocumentsOperationContext context, IEnumerator<TimeSeriesDeletedRangeItem> timeSeries, string collection)
    {
        throw new NotImplementedException();
    }

    protected override bool ShouldTrackAttachmentTombstones()
    {
        return false;
    }

    protected override EtlTransformer<AiEtlItem, AiEtlEmbeddingItem, EtlStatsScope, EtlPerformanceOperation> GetTransformer(DocumentsOperationContext context)
    {
        return new AiEtlDocumentTransformer(Database, context, Transformation, null, Configuration);
    }

    protected override int LoadInternal(IEnumerable<AiEtlEmbeddingItem> items, DocumentsOperationContext context, EtlStatsScope scope)
    {
        _service ??= AiHelper.CreateService(Configuration);

        var aiEtlScriptRun = (AiEtlScriptRun)items;

        int processed = 0;

        using (_missingEmbeddingsHolder)
        {
            foreach (var aiEtlEmbeddingItem in aiEtlScriptRun.Additions)
            {
                foreach (var kvp in aiEtlEmbeddingItem.Values)
                {
                    var values = kvp.Value;
                    
                    foreach (var value in values)
                    {
                        var valueEmbeddingsDocument = Database.AiStorage.GetValueEmbeddingsDocument(context, Configuration, value.TextualValue, out var valueEmbeddingsDocumentId);

                        value.ValueEmbeddingsDocumentId = valueEmbeddingsDocumentId;
                        value.ValueEmbeddingsAttachmentName = valueEmbeddingsDocument?.GetAttachmentNameForValue(value.TextualValue);
                        
                        if (value.ValueEmbeddingsAttachmentName == null)
                            _missingEmbeddingsHolder.Add(value.TextualValue, value);
                    }
                }

                processed++;
            }

            var embeddingsMap = _missingEmbeddingsHolder.GetEmbeddingsMap();
            var keys = embeddingsMap.Keys.ToList();
            if (embeddingsMap.Keys.Any())
            {
                var generatedValues = AsyncHelpers.RunSync(() => _service.GenerateEmbeddingsAsync(keys));

                if (generatedValues.Count != keys.Count)
                    throw new InvalidOperationException("Generated embeddings count does not match missing values count");
                
                for (var i = 0; i < keys.Count; ++i)
                {
                    var key = keys[i];
                    var embedding = generatedValues[i];
                    
                    foreach (var embeddingItem in embeddingsMap[key])
                        embeddingItem.EmbeddingValue = embedding;
                }
            }
            
            var putEmbeddingsCommand = new MergedPutEmbeddingsCommand(aiEtlScriptRun, Configuration.Name, Database);

            Database.TxMerger.EnqueueSync(putEmbeddingsCommand);
        }
        
        return processed;
    }

    protected override EtlStatsScope CreateScope(EtlRunStats stats)
    {
        return new EtlStatsScope(stats);
    }

    protected override bool ShouldFilterOutHiLoDocument()
    {
        return true;
    }

    private class MissingEmbeddingsHolder : IDisposable
    {
        // missing value -> embeddings
        private readonly Dictionary<string, List<AiEtlEmbeddingItemValue>> _embeddingsMap = new();
        
        public void Add(string value, AiEtlEmbeddingItemValue item)
        {
            if (_embeddingsMap.ContainsKey(value) == false)
                _embeddingsMap.Add(value, new List<AiEtlEmbeddingItemValue>());
                
            _embeddingsMap[value].Add(item);
        }

        public IReadOnlyDictionary<string, List<AiEtlEmbeddingItemValue>> GetEmbeddingsMap() => _embeddingsMap;

        public void Dispose()
        {
            _embeddingsMap.Clear();
        }
    }

    public AiEtlTestScriptResult RunTest(IEnumerable<AiEtlEmbeddingItem> records, DocumentsOperationContext context)
    {
        var services = AiHelper.CreateServicesForTest(
            new AiEtlConfiguration
            {
                AiConnectorType = AiConnectorType.Onnx,
                Connection = new AiConnectionString { OnnxSettings = new OnnxSettings()}
            }, out string serviceId);

        var embeddingService = services.GetRequiredKeyedService<ITextEmbeddingGenerationService>(serviceId);
        var result = new AiEtlTestScriptResult();

        foreach (var record in records)
        {
            foreach (var aiEtlEmbeddingItemValue in record.Values.SelectMany(x => x.Value))
            {
                aiEtlEmbeddingItemValue.ValueEmbeddingsDocumentId = record.DocumentId;
                aiEtlEmbeddingItemValue.EmbeddingValue = embeddingService.GenerateEmbeddingsAsync([aiEtlEmbeddingItemValue.TextualValue]).Result[0];

                result.EmbeddingItemValues.Add(aiEtlEmbeddingItemValue);
            }
        }

        result.TransformationErrors = Statistics.TransformationErrorsInCurrentBatch.Errors.ToList();
        return result;
    }
}
