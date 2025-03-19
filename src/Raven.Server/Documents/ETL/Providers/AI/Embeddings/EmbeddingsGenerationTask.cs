using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.SemanticKernel.Embeddings;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.Documents.ETL.Metrics;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings.Stats;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings.Test;
using Raven.Server.Documents.ETL.Providers.AI.Enumerators;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

#pragma warning disable SKEXP0001

namespace Raven.Server.Documents.ETL.Providers.AI.Embeddings;

public sealed class EmbeddingsGenerationTask : EtlProcess<EmbeddingsGenerationItem, EmbeddingGenerationScriptResult, EmbeddingsGenerationConfiguration, AiConnectionString,
    EmbeddingsGenerationStatsScope, EmbeddingsGenerationPerformanceOperation>
{
    private const string EmbeddingsTaskTag = "AI/Embeddings Generation";

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
        GetTransformer(DocumentsOperationContext context)
    {
        return new EmbeddingsGenerationScriptTransformer(Database, context, Transformation, null, Configuration);
    }

    protected override string LoadFailureMessage => $"Failed to generate embeddings in '{Configuration.Name}' task. Going to do the retry using '{Database.Configuration.Ai.EmbeddingsGenerationTaskRetryStrategy}' backoff strategy.";

    protected override void EnterFallbackMode(Exception e, DateTime? lastErrorTime)
    {
        _fallbackCounter++;
        var taskRetryDelay = Database.Configuration.Ai.EmbeddingsGenerationTaskRetryDelay.AsTimeSpan;
        if (lastErrorTime == null)
        {
            FallbackTime = taskRetryDelay;
        }
        else
        {
            var secondsToWait = (Database.Configuration.Ai.EmbeddingsGenerationTaskRetryStrategy) switch
            {
                EmbeddingsGenerationRetryStrategy.Linear => _fallbackCounter * taskRetryDelay.TotalSeconds,
                EmbeddingsGenerationRetryStrategy.Exponential => taskRetryDelay.TotalSeconds * Math.Pow(2, _fallbackCounter),
                _ => throw new NotImplementedException($"Strategy: '{Database.Configuration.Ai.EmbeddingsGenerationTaskRetryStrategy}' is not implemented.")
                
            };
            FallbackTime = TimeSpan.FromSeconds(Math.Min(Database.Configuration.Ai.EmbeddingsGenerationTaskMaxFallbackTime.AsTimeSpan.TotalSeconds, Math.Max(taskRetryDelay.Seconds, secondsToWait)));
        }
    }

    protected override int LoadInternal(IEnumerable<EmbeddingGenerationScriptResult> items, DocumentsOperationContext context, EmbeddingsGenerationStatsScope scope)
    {
        if (items is not EmbeddingsGenerationScriptRun embeddingsScriptRun)
        {
            Debug.Assert(items != null && items!.GetType()!.FullName!.StartsWith("System.Linq.EmptyPartition"),
                $"items != null && items!.GetType()!.FullName!.StartsWith('System.Linq.EmptyPartition'): {items!.GetType()!.FullName!}");
            return 0;
        }

        var taskId = new EmbeddingsGenerationTaskIdentifier(Configuration.Identifier);

        int processed = 0;

        using (var storageScope = scope.For(EmbeddingsGenerationOperations.GenerateInAiService))
        {
            var embeddingsInCache = 0;
            List<Task> tasks = null;
            foreach (var embeddingItem in embeddingsScriptRun.Additions)
            {
                foreach (var kvp in embeddingItem.Values)
                {
                    var embeddingsGenerationItems = kvp.Value;

                    foreach (var embeddingToGenerate in embeddingsGenerationItems)
                    {
                        if (Database.EmbeddingsGeneratorEtl.GenerateEmbeddingsToCache(context, taskId, embeddingToGenerate.TextualValue, ref tasks))
                        {
                            embeddingsInCache++;
                        }
                    }
                }

                processed++;
            }

            if (tasks is not null)
            {
                Task.WaitAll(tasks.ToArray());
            }
            
            storageScope.NumberOfEmbeddingsInCache = embeddingsInCache;
            storageScope.NumberOfPutEmbeddingDocuments = embeddingsScriptRun.Additions.Count;
            storageScope.NumberOfDeletedEmbeddingDocuments = embeddingsScriptRun.Removals.Count;
        }


        return processed;
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
        (ITextEmbeddingGenerationService embeddingService, _) = AiHelper.CreateServicesForTest(
            new EmbeddingsGenerationConfiguration { Connection = new AiConnectionString { EmbeddedSettings = new EmbeddedSettings() } });

        var result = new EmbeddingsGenerationTestScriptResult();

        foreach (var record in records)
        {
            foreach (var embeddingItemValue in record.Values.SelectMany(x => x.Value))
            {
                var embedding = AiHelper.GenerateEmbedding(embeddingService, embeddingItemValue.TextualValue);

                var embeddingValue = EmbeddingsHelper.CreateEmbeddingValue(embedding, Configuration.Quantization);

                embeddingItemValue.SetEmbedding(embeddingValue, Configuration.Quantization, new AiConnectionStringIdentifier("TODO")); //TODO

                result.EmbeddingItemValues.Add(embeddingItemValue);
            }
        }

        result.TransformationErrors = Statistics.TransformationErrorsInCurrentBatch.Errors.ToList();
        return result;
    }
}
