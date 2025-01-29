using System;
using System.Collections.Generic;
using Microsoft.SemanticKernel.Embeddings;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.AI;
using Raven.Client.Util;
using Raven.Server.Documents.ETL.Providers.AI.Enumerators;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Indexes.VectorSearch;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TimeSeries;
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

    protected override EtlTransformer<AiEtlItem, AiEtlEmbeddingItem, EtlStatsScope, EtlPerformanceOperation> GetTransformer(DocumentsOperationContext context)
    {
        return new AiEtlDocumentTransformer(Database, context, null, null, Configuration);
    }

    protected override int LoadInternal(IEnumerable<AiEtlEmbeddingItem> items, DocumentsOperationContext context, EtlStatsScope scope)
    {
        _service ??= AiHelper.CreateService(Configuration);

        var aiEtlScriptRun = (AiEtlScriptRun)items;

        int processed = 0;

        using (_missingEmbeddingsHolder)
        {
            foreach (var aiEtlEmbeddingItem in aiEtlScriptRun)
            {
                foreach (var kvp in aiEtlEmbeddingItem.Values)
                {
                    var values = kvp.Value;
                    
                    foreach (var value in values)
                    {
                        var valueEmbeddingsDocument = Database.AiStorage.GetValueEmbeddingsDocument(context, Configuration, value.TextualValue, out var valueEmbeddingsDocumentId);

                        value.ValueEmbeddingsDocumentId = valueEmbeddingsDocumentId;
                        value.ValueEmbeddingsAttachmentName = valueEmbeddingsDocument.GetAttachmentNameForValue(value.TextualValue);
                        
                        if (value.ValueEmbeddingsAttachmentName == null)
                            _missingEmbeddingsHolder.Add(value.TextualValue, value);
                    }
                }
            }

            var missingValues = _missingEmbeddingsHolder.GetValuesForMissingEmbeddings();
            if (missingValues.Count > 0)
            {
                var generatedValues = AsyncHelpers.RunSync(() => _service.GenerateEmbeddingsAsync(missingValues));

                if (generatedValues.Count != missingValues.Count)
                    throw new InvalidOperationException("Generated embeddings count does not match missing values count");

                var embeddingsMap = _missingEmbeddingsHolder.GetEmbeddingsMap();

                for (var i = 0; i < embeddingsMap.Count; ++i)
                {
                    var embeddingItem = embeddingsMap[i];
                    var embedding = generatedValues[i];

                    embeddingItem.EmbeddingValue = embedding;
                }
            }

            // process the aiEtlScriptRun here via TxMerger
            var cmd = new MergedPutEmbeddingsCommand(aiEtlScriptRun.CurrentRun, Configuration.Name, Database);

            Database.TxMerger.EnqueueSync(cmd);

            return processed;
        }
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
        private const int MaxCapacity = 1024;

        private readonly List<string> _missingValues = new();

        private readonly List<AiEtlEmbeddingItemValue> _embeddingsMap = new();

        public void Add(string value, AiEtlEmbeddingItemValue item)
        {
            _missingValues.Add(value);
            _embeddingsMap.Add(item);
        }

        public List<string> GetValuesForMissingEmbeddings() => _missingValues;

        public IReadOnlyList<AiEtlEmbeddingItemValue> GetEmbeddingsMap() => _embeddingsMap;

        public void Dispose()
        {
            _missingValues.Clear();
            _embeddingsMap.Clear();

            SetCapacityIfNeeded(_missingValues);
            SetCapacityIfNeeded(_embeddingsMap);
            return;

            static void SetCapacityIfNeeded<T>(List<T> list)
            {
                if (list.Capacity > MaxCapacity)
                    list.Capacity = MaxCapacity;
            }
        }
    }
}
