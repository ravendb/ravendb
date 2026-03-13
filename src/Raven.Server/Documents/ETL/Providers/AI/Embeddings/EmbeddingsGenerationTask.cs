using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Microsoft.Extensions.AI;
using Microsoft.SemanticKernel.Embeddings;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.ETL.Metrics;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings.Stats;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings.Test;
using Raven.Server.Documents.ETL.Providers.AI.Enumerators;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Server.Utils;

#pragma warning disable SKEXP0001

namespace Raven.Server.Documents.ETL.Providers.AI.Embeddings;

public sealed class EmbeddingsGenerationTask : EtlProcess<EmbeddingsGenerationItem, EmbeddingGenerationScriptResult, EmbeddingsGenerationConfiguration, AiConnectionString,
    EmbeddingsGenerationStatsScope, EmbeddingsGenerationPerformanceOperation>
{
    public const string EmbeddingsTaskTag = "AI/Embeddings Generation";

    private int _fallbackCounter = 0;
    
    public EmbeddingsGenerationTask(Transformation transformation, EmbeddingsGenerationConfiguration configuration, DocumentDatabase database, ServerStore serverStore)
        : base(transformation, configuration, database, serverStore, EmbeddingsTaskTag)
    {
        Metrics = new EtlMetricsCountersManager();
    }

    public override EtlType EtlType => EtlType.EmbeddingsGeneration;
    public override bool ShouldTrackCounters() => false;
    public override bool ShouldTrackTimeSeries() => false;
    
    protected override bool ShouldTrackAttachmentTombstones() => false;

    protected override IEnumerator<EmbeddingsGenerationItem> ConvertDocsEnumerator(DocumentsOperationContext context, IEnumerator<Document> docs, string collection)
    {
        return new DocumentsToEmbeddingsGenerationItems(docs, collection);
    }

    protected override IEnumerator<EmbeddingsGenerationItem> ConvertTombstonesEnumerator(DocumentsOperationContext context, IEnumerator<Tombstone> tombstones, string collection,
        bool trackAttachments)
    {
        return new TombstonesToEmbeddingsGenerationItems(tombstones, collection);
    }

    protected override IEnumerator<EmbeddingsGenerationItem> ConvertAttachmentTombstonesEnumerator(DocumentsOperationContext context, IEnumerator<Tombstone> tombstones,
        List<string> collections)
    {
        throw new NotSupportedException($"{nameof(ConvertAttachmentTombstonesEnumerator)} is not supported for {nameof(EmbeddingsGenerationTask)}");
    }

    protected override IEnumerator<EmbeddingsGenerationItem> ConvertCountersEnumerator(DocumentsOperationContext context, IEnumerator<CounterGroupDetail> counters,
        string collection)
    {
        throw new NotSupportedException($"{nameof(ConvertCountersEnumerator)} is not supported for {nameof(EmbeddingsGenerationTask)}");
    }

    protected override IEnumerator<EmbeddingsGenerationItem> ConvertTimeSeriesEnumerator(DocumentsOperationContext context, IEnumerator<TimeSeriesSegmentEntry> timeSeries,
        string collection)
    {
        throw new NotSupportedException($"{nameof(ConvertTimeSeriesEnumerator)} is not supported for {nameof(EmbeddingsGenerationTask)}");
    }

    protected override IEnumerator<EmbeddingsGenerationItem> ConvertTimeSeriesDeletedRangeEnumerator(DocumentsOperationContext context,
        IEnumerator<TimeSeriesDeletedRangeItem> timeSeries, string collection)
    {
        throw new NotSupportedException($"{nameof(ConvertTimeSeriesDeletedRangeEnumerator)} is not supported for {nameof(EmbeddingsGenerationTask)}");
    }

    protected override EtlTransformer<EmbeddingsGenerationItem, EmbeddingGenerationScriptResult, EmbeddingsGenerationStatsScope, EmbeddingsGenerationPerformanceOperation>
        GetTransformer(DocumentsOperationContext context, EmbeddingsGenerationStatsScope stats)
    {
        return new EmbeddingsGenerationScriptTransformer(Database, context, Transformation, null, Configuration);
    }

    protected override string LoadFailureMessage => $"Failed to generate embeddings in '{Configuration.Name}' task. Going to do the retry in {FallbackTime} (failure #{_fallbackCounter}).";

    protected override void EnterFallbackMode(Exception e, DateTime? lastErrorTime)
    {
        // rate limits in embeddings are usually expressed as requests per minute or tokens per minute
        // and they are reset on each full minute ticks, so we'll wait until the next full minute to retry
        // at a minimum
        int secondsToWaitToNextMinute = 60 - DateTime.UtcNow.Second;

        var secondsToWait = ++_fallbackCounter switch
        {
            // first - we'll wait for the next minute each time - 5 minutes total 
            < 5 => secondsToWaitToNextMinute,
            // then - we'll wait for ~two minutes - 10 minutes total
            < 10 => secondsToWaitToNextMinute + 60,
            // then - we'll wait for three minutes - 30 minutes
            < 20 => secondsToWaitToNextMinute + 120,
            // finally - we'll use log2 minutes - so at 20+ failures, wait 5 minutes - 1 hour total
            // then after 32 failures, wait 6 minutes each time - 3.2 hours, etc...
            _ => secondsToWaitToNextMinute + ((int)Math.Log2(_fallbackCounter) * 60)
        };

        double max = Database.Configuration.Ai.EmbeddingsGenerationMaxFallbackTime.AsTimeSpan.TotalSeconds;
        FallbackTime = TimeSpan.FromSeconds(Math.Min(secondsToWait, max));
    }

    protected override bool ExtractionLimitReached(DocumentsOperationContext ctx, EmbeddingsGenerationStatsScope stats, EmbeddingsGenerationItem currentItem, int batchSize)
    {
        if (stats.NumberOfExtractedItems[EtlItemType.Document] >= Database.Configuration.Ai.EmbeddingsGenerationMaxBatchSize)
        {
            var reason = $"Stopping the batch because it has already processed max number of extracted documents : {stats.NumberOfExtractedItems[EtlItemType.Document]}";

            if (Logger.IsInfoEnabled)
                Logger.Info($"[{Name}] {reason}");

            stats.RecordBatchTransformationCompleteReason(reason);

            return true;
        }

        return false;
    }

    protected override int LoadInternal(IEnumerable<EmbeddingGenerationScriptResult> items, DocumentsOperationContext context, EmbeddingsGenerationStatsScope scope)
    {
        if (items is not EmbeddingsGenerationScriptRun embeddingsScriptRun)
        {
            Debug.Assert(items is EmbeddingGenerationScriptResult[] {Length: 0});
            
            return 0;
        }

        var taskId = new EmbeddingsGenerationTaskIdentifier(Configuration.Identifier);

        // Prevent database unloading during long-running AI operations
        using (Database.PreventFromUnloadingByIdleOperations())
        {
            var batch = Database.EmbeddingsGeneratorEtl.BatchFor(taskId);
            using (var storageScope = scope.For(EmbeddingsGenerationOperations.GenerateInAiService))
            {
                foreach (var embeddingItem in embeddingsScriptRun.Additions)
                {
                    batch.StartGenerateEmbeddingFor(context, embeddingItem.DocumentId, embeddingItem.DocumentCollectionName,
                        embeddingItem.Fields);
                }
                
                // Wait for embeddings generation and storage of embeddings cache documents 
                batch.WaitForGenerationAsync().GetAwaiter().GetResult();
                storageScope.NumberOfEmbeddingsInCache = batch.CachedEmbeddings;
                storageScope.NumberOfGeneratedEmbeddings = embeddingsScriptRun.Additions.Count;
            }

            foreach (var embeddingItem in embeddingsScriptRun.Removals)
            {
                batch.Delete(embeddingItem.DocumentId);
            }
            
            using (var storageScope = scope.For(EmbeddingsGenerationOperations.Storage))
            {
                // Start storing embedding documents and wait for them to be stored
                batch.StoreDocumentEmbeddingsAsync().GetAwaiter().GetResult();
                
                storageScope.NumberOfPutEmbeddingDocuments = embeddingsScriptRun.Additions.Count;
                storageScope.NumberOfDeletedEmbeddingDocuments = embeddingsScriptRun.Removals.Count;
            }
        }
        
        _fallbackCounter = 0;
        return embeddingsScriptRun.Additions.Count + embeddingsScriptRun.Removals.Count;
    }

    protected override EmbeddingsGenerationStatsScope CreateScope(EtlRunStats stats)
    {
        return new EmbeddingsGenerationStatsScope(stats);
    }

    protected override string StatsAggregatorTag => "Embeddings Generation";

    protected override bool ShouldFilterOutHiLoDocument()
    {
        return true;
    }

    public EmbeddingsGenerationTestScriptResult RunTest(IEnumerable<EmbeddingGenerationScriptResult> records, DocumentsOperationContext context)
    {
        (IEmbeddingGenerator<string, Embedding<float>> embeddingService, _) = AiHelper.CreateEmbeddingServicesForTest(
            new EmbeddingsGenerationConfiguration { Connection = new AiConnectionString { EmbeddedSettings = new EmbeddedSettings() } });

        var result = new EmbeddingsGenerationTestScriptResult();
        List<string> chunks = [];
        List<EmbeddingsGenerationTestScriptResult.Item> allItems = [];
        foreach (var record in records)
        {
            foreach (var (name, values) in record.Fields)
            {
                List<EmbeddingsGenerationTestScriptResult.Item> items = [];
                result.Results[name] = items;
                foreach (var (value, chunking) in values)
                {
                    foreach(var chunked in TextChunker.Chunk(value, chunking))
                    {
                        var item = new EmbeddingsGenerationTestScriptResult.Item(chunked);
                        chunks.Add(name);
                        allItems.Add(item);
                        items.Add(item);
                    }
                }
            }
        }
        var results = embeddingService.GenerateAsync(chunks, cancellationToken: CancellationToken).GetAwaiter().GetResult();
        for (int i = 0; i < results.Count; i++)
        {
            allItems[i].Embeddings = MemoryMarshalEx.Cast<float, byte>(results[i].Vector);
        }

        result.TransformationErrors = Statistics.TransformationErrorsInCurrentBatch.Errors.ToList();
        return result;
    }
}
