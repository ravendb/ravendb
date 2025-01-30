using System;
using System.Collections.Generic;
using System.Linq;
using Jint.Native;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.AI;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.AI;

public sealed class AiEtlDocumentTransformer : EtlTransformer<AiEtlItem, AiEtlEmbeddingItem, EtlStatsScope, EtlPerformanceOperation>
{
    private readonly AiEtlConfiguration _configuration;
    private AiEtlScriptRun _currentRun;
    
    public AiEtlDocumentTransformer(DocumentDatabase database, DocumentsOperationContext context, PatchRequest mainScript, PatchRequest behaviorFunctions, AiEtlConfiguration configuration) : base(database, context, mainScript, behaviorFunctions)
    {
        _configuration = configuration;
    }

    public override void Initialize(bool debugMode)
    {
        base.Initialize(debugMode);

        if (DocumentScript == null)
            return;
    }

    protected override void AddLoadedAttachment(JsValue reference, string name, Attachment attachment)
    {
        throw new NotImplementedException();
    }

    protected override void AddLoadedCounter(JsValue reference, string name, long value)
    {
        throw new NotImplementedException();
    }

    protected override void AddLoadedTimeSeries(JsValue reference, string name, IEnumerable<SingleResult> entries)
    {
        throw new NotImplementedException();
    }

    protected override string[] LoadToDestinations { get; }
    protected override void LoadToFunction(string tableName, ScriptRunnerResult colsAsObject)
    {
        throw new NotImplementedException();
    }

    /// docId -> <fieldName, <fieldValues>>
    public override IEnumerable<AiEtlEmbeddingItem> GetTransformedResults()
    {
        return _currentRun ?? Enumerable.Empty<AiEtlEmbeddingItem>();
    }

    public override void Transform(AiEtlItem item, EtlStatsScope stats, EtlProcessState state)
    {
        Current = item;
        _currentRun ??= new AiEtlScriptRun();

        var aiEtlEmbeddingItem = new AiEtlEmbeddingItem() { DocumentId = item.DocumentId, DocumentCollectionName = item.Collection, Values = new Dictionary<string, List<AiEtlEmbeddingItemValue>>() };
        
        foreach (var fieldName in _configuration.FieldsToInclude)
        {
            if (BlittableJsonTraverserHelper.TryRead(BlittableJsonTraverser.Default, item.Document, fieldName, out var fieldValue) == false)
                continue;

            if (aiEtlEmbeddingItem.Values.TryGetValue(fieldName, out var values) == false)
                aiEtlEmbeddingItem.Values[fieldName] = values = new List<AiEtlEmbeddingItemValue>();
            
            switch (fieldValue)
            {
                case LazyStringValue lsv:
                    values.Add(new AiEtlEmbeddingItemValue() { TextualValue = lsv });
                    break;
                case LazyCompressedStringValue lcsv:
                    values.Add(new AiEtlEmbeddingItemValue() { TextualValue = lcsv });
                    break;
                case BlittableJsonReaderArray bjra:
                {
                    foreach (var textualValue in bjra)
                        values.Add(new AiEtlEmbeddingItemValue() { TextualValue = (LazyStringValue)textualValue });
                    break;
                }
                default:
                    throw new ArgumentException($"Unsupported field value type: {fieldValue.GetType()}");
            }
        }
        
        _currentRun.CurrentRun.Add(aiEtlEmbeddingItem);
    }
}
