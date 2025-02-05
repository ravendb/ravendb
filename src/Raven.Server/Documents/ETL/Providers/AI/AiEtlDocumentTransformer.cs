#pragma warning disable SKEXP0050

using System;
using System.Collections.Generic;
using System.Linq;
using Jint;
using Jint.Native;
using Jint.Runtime.Interop;
using Microsoft.SemanticKernel.Text;
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
    
    public AiEtlDocumentTransformer(DocumentDatabase database, DocumentsOperationContext context, Transformation transformation, PatchRequest behaviorFunctions, AiEtlConfiguration configuration) : base(database, context, new PatchRequest(transformation.Script, PatchRequestType.AiEtl), behaviorFunctions)
    {
        _configuration = configuration;

        LoadToDestinations = [];
    }

    public override void Initialize(bool debugMode)
    {
        base.Initialize(debugMode);

        if (DocumentScript == null)
            return;
        
        DocumentScript.ScriptEngine.SetValue("splitMarkDownLines", new ClrFunction(DocumentScript.ScriptEngine, "splitMarkDownLines", SplitMarkDownLines));
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

        if (item.IsDelete)
        {
            var deletedItem = new AiEtlEmbeddingItem() { DocumentId = Current.DocumentId, DocumentCollectionName = Current.Collection, IsDelete = true };
            
            _currentRun.Deletes.Add(deletedItem);
            
            return;
        }
        
        DocumentScript.Run(Context, Context, "execute", new object[] { Current.Document }).Dispose();

        var aiEtlEmbeddingItem = new AiEtlEmbeddingItem() { DocumentId = Current.DocumentId, DocumentCollectionName = Current.Collection, Values = new Dictionary<string, List<AiEtlEmbeddingItemValue>>() };
        
        foreach (var fieldName in _configuration.FieldsToInclude)
        {
            if (BlittableJsonTraverserHelper.TryRead(BlittableJsonTraverser.Default, Current.Document, fieldName, out var fieldValue) == false)
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
                    values.Add(new AiEtlEmbeddingItemValue() { TextualValue = fieldValue.ToString() });
                    break;
            }
        }
        
        _currentRun.CurrentRun.Add(aiEtlEmbeddingItem);
    }
    
    // todo non-default token counter
    private JsValue SplitMarkDownLines(JsValue self, JsValue[] args)
    {
        var methodSignature = "splitMarkDownLines(text, maxTokensPerLine)";
        
        if (args.Length != 2)
            ThrowInvalidScriptMethodCall($"{methodSignature} has to be called with 2 arguments");
        
        if (args[0].IsString() == false)
            ThrowInvalidScriptMethodCall($"{methodSignature} first argument must be a string");
        
        if (args[1].IsNumber() == false)
            ThrowInvalidScriptMethodCall($"{methodSignature} second argument must be a number");
        
        var chunks = TextChunker.SplitMarkDownLines(args[0].AsString(), (int)args[1].AsNumber());
        
        var jsChunks = new JsValue[chunks.Count];
        for (var i = 0; i < chunks.Count; i++)
        {
            jsChunks[i] = new JsString(chunks[i]);
        }
        
        var jsArray = new JsArray(DocumentScript.ScriptEngine, jsChunks);

        return jsArray;
    }
}
#pragma warning restore SKEXP0050
