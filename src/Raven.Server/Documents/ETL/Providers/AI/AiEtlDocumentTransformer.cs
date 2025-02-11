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

internal sealed class AiEtlDocumentTransformer : EtlTransformer<AiEtlItem, AiEtlEmbeddingItem, AiEtlStatsScope, AiEtlPerformanceOperation>
{
    private readonly AiEtlConfiguration _configuration;
    private AiEtlScriptRun _currentRun;
    private PatchRequest _mainScript;
    private AiEtlStatsScope _stats;
    
    public AiEtlDocumentTransformer(DocumentDatabase database, DocumentsOperationContext context, Transformation transformation, PatchRequest behaviorFunctions, AiEtlConfiguration configuration) : base(database, context, null, behaviorFunctions)
    {
        _configuration = configuration;
        _mainScript = new PatchRequest(transformation.Script, PatchRequestType.AiEtl);
    }

    public override void Initialize(bool debugMode)
    {
        Database.Scripts.GetScriptRunner(_mainScript, true, out DocumentScript);
        
        if (DocumentScript == null)
            return;
        
        if (debugMode)
            DocumentScript.DebugMode = true;
        
        DocumentScript.ScriptEngine.SetValue("generateEmbeddings", new ClrFunction(DocumentScript.ScriptEngine, "generateEmbeddings", EmbeddingsGenerate));
        
        DocumentScript.ScriptEngine.SetValue("splitMarkDownLines", new ClrFunction(DocumentScript.ScriptEngine, "splitMarkDownLines", SplitMarkDownLines));
        DocumentScript.ScriptEngine.SetValue("splitMarkDownParagraphs", new ClrFunction(DocumentScript.ScriptEngine, "splitMarkDownParagraphs", SplitMarkDownParagraphs));
        DocumentScript.ScriptEngine.SetValue("splitPlainTextLines", new ClrFunction(DocumentScript.ScriptEngine, "splitPlainTextLines", SplitPlainTextLines));
        DocumentScript.ScriptEngine.SetValue("splitPlainTextParagraphs", new ClrFunction(DocumentScript.ScriptEngine, "splitPlainTextParagraphs", SplitPlainTextParagraphs));
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
    
    public override IEnumerable<AiEtlEmbeddingItem> GetTransformedResults()
    {
        return _currentRun ?? Enumerable.Empty<AiEtlEmbeddingItem>();
    }

    public override void Transform(AiEtlItem item, AiEtlStatsScope stats, EtlProcessState state)
    {
        Current = item;
        _currentRun ??= new AiEtlScriptRun();

        if (item.IsDelete)
        {
            var deletedItem = new AiEtlEmbeddingItem() { DocumentId = Current.DocumentId, DocumentCollectionName = Current.Collection, IsDelete = true };
            
            _currentRun.Deletes.Add(deletedItem);
            
            return;
        }

        if (_configuration.PathsToProcess == null || _configuration.PathsToProcess.Count == 0)
        {
            DocumentScript.Run(Context, Context, "execute", new object[] { Current.Document });
            
            return;
        }

        var aiEtlEmbeddingItem = new AiEtlEmbeddingItem()
        {
            DocumentId = Current.DocumentId, DocumentCollectionName = Current.Collection, Values = new Dictionary<string, List<AiEtlEmbeddingItemValue>>()
        };
        
        foreach (var fieldName in _configuration.PathsToProcess)
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
            
        _currentRun.Additions.Add(aiEtlEmbeddingItem);
    }
    
#pragma warning disable SKEXP0050
    // todo non-default token counter
    private JsValue SplitMarkDownLines(JsValue self, JsValue[] args)
    {
        const string methodSignature = "splitMarkDownLines(text, maxTokensPerLine)";
        
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
    
    // todo optional params
    private JsValue SplitMarkDownParagraphs(JsValue self, JsValue[] args)
    {
        const string methodSignature = "splitMarkDownParagraphs(lines, maxTokensPerLine)";
        
        if (args.Length != 2)
            ThrowInvalidScriptMethodCall($"{methodSignature} has to be called with 2 arguments");
        
        if (args[0].IsArray() == false)
            ThrowInvalidScriptMethodCall($"{methodSignature} first argument must be of type {typeof(IEnumerable<string>)}");
        
        if (args[1].IsNumber() == false)
            ThrowInvalidScriptMethodCall($"{methodSignature} second argument must be a number");

        var lines = new List<string>();

        foreach (var line in args[0].AsArray())
        {
            lines.Add(line.AsString());
        }
        
        var chunks = TextChunker.SplitMarkdownParagraphs(lines, (int)args[1].AsNumber());
        
        var jsChunks = new JsValue[chunks.Count];
        for (var i = 0; i < chunks.Count; i++)
        {
            jsChunks[i] = new JsString(chunks[i]);
        }
        
        var jsArray = new JsArray(DocumentScript.ScriptEngine, jsChunks);

        return jsArray;
    }
    
    // todo non-default token counter
    private JsValue SplitPlainTextLines(JsValue self, JsValue[] args)
    {
        const string methodSignature = "splitPlainTextLines(text, maxTokensPerLine)";
        
        if (args.Length != 2)
            ThrowInvalidScriptMethodCall($"{methodSignature} has to be called with 2 arguments");
        
        if (args[0].IsString() == false)
            ThrowInvalidScriptMethodCall($"{methodSignature} first argument must be a string");
        
        if (args[1].IsNumber() == false)
            ThrowInvalidScriptMethodCall($"{methodSignature} second argument must be a number");
        
        var chunks = TextChunker.SplitPlainTextLines(args[0].AsString(), (int)args[1].AsNumber());
        
        var jsChunks = new JsValue[chunks.Count];
        for (var i = 0; i < chunks.Count; i++)
        {
            jsChunks[i] = new JsString(chunks[i]);
        }
        
        var jsArray = new JsArray(DocumentScript.ScriptEngine, jsChunks);

        return jsArray;
    }
    
    // todo optional params
    private JsValue SplitPlainTextParagraphs(JsValue self, JsValue[] args)
    {
        const string methodSignature = "splitPlainTextParagraphs(lines, maxTokensPerLine)";
        
        if (args.Length != 2)
            ThrowInvalidScriptMethodCall($"{methodSignature} has to be called with 2 arguments");
        
        if (args[0].IsArray() == false)
            ThrowInvalidScriptMethodCall($"{methodSignature} first argument must be of type {typeof(IEnumerable<string>)}");
        
        if (args[1].IsNumber() == false)
            ThrowInvalidScriptMethodCall($"{methodSignature} second argument must be a number");

        var lines = new List<string>();

        foreach (var line in args[0].AsArray())
        {
            lines.Add(line.AsString());
        }
        
        var chunks = TextChunker.SplitPlainTextParagraphs(lines, (int)args[1].AsNumber());
        
        var jsChunks = new JsValue[chunks.Count];
        for (var i = 0; i < chunks.Count; i++)
        {
            jsChunks[i] = new JsString(chunks[i]);
        }
        
        var jsArray = new JsArray(DocumentScript.ScriptEngine, jsChunks);

        return jsArray;
    }
    
    private JsValue EmbeddingsGenerate(JsValue self, JsValue[] args)
    {
        const string methodSignature = "embeddings.generate(object)";
        
        if (args.Length != 1)
            ThrowInvalidScriptMethodCall($"{methodSignature} has to be called with 1 argument");
        
        if (args[0].IsObject() == false)
            ThrowInvalidScriptMethodCall($"{methodSignature} first argument must be an object");
        
        var mainObj = args[0].AsObject();
        
        var aiEtlEmbeddingItem = new AiEtlEmbeddingItem()
        {
            DocumentId = Current.DocumentId, DocumentCollectionName = Current.Collection, Values = new Dictionary<string, List<AiEtlEmbeddingItemValue>>()
        };

        foreach (var propertyKey in mainObj.GetOwnPropertyKeys())
        {
            var propertyName = propertyKey.AsString();
            
            if (aiEtlEmbeddingItem.Values.TryGetValue(propertyName, out var values) == false)
                aiEtlEmbeddingItem.Values[propertyName] = values = new List<AiEtlEmbeddingItemValue>();
            
            mainObj.TryGetValue(propertyKey, out JsValue value);
            
            if (value.IsString())
            {
                values.Add(new AiEtlEmbeddingItemValue() { TextualValue = value.AsString() });
            }
            
            else if (value.IsArray())
            {
                var jsArray = value.AsArray();
                
                foreach (var jsValue in jsArray)
                    values.Add(new AiEtlEmbeddingItemValue() { TextualValue = jsValue.AsString() });
            }
        }
        
        _currentRun.Additions.Add(aiEtlEmbeddingItem);
        
        return JsValue.Null;
    }
}
#pragma warning restore SKEXP0050
