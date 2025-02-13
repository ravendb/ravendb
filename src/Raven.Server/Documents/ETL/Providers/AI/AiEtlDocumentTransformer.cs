using System;
using System.Buffers.Text;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax;
using Corax.Utils;
using Jint;
using Jint.Native;
using Jint.Runtime.Interop;
using Microsoft.SemanticKernel.Text;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.AI;
using Raven.Client.Exceptions.Corax;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Indexes.Persistence;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

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
            
            _currentRun.Removals.Add(deletedItem);
            
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
            if (BlittableJsonTraverserHelper.TryRead(BlittableJsonTraverser.Default, Current.Document, fieldName, out var value) == false)
                continue;

            ref var container = ref CollectionsMarshal.GetValueRefOrAddDefault(aiEtlEmbeddingItem.Values, fieldName, out _);
            container ??= new();
            WriteValueToEmbeddings(container, value);
        }
            
        _currentRun.Additions.Add(aiEtlEmbeddingItem);
    }

    private void WriteValueToEmbeddings(List<AiEtlEmbeddingItemValue> values, object value)
    {
        var valueType = ConverterBase.GetValueTypeUnlikely(value);
        switch (valueType)
        {
            case ConverterBase.ValueType.Double:
            case ConverterBase.ValueType.Numeric:
            case ConverterBase.ValueType.Enum:
            case ConverterBase.ValueType.Boolean:
            case ConverterBase.ValueType.String:
                values.Add(new AiEtlEmbeddingItemValue() { TextualValue = value.ToString() });
                break;
            
            case ConverterBase.ValueType.Char:
                if (value is char c)
                    values.Add(new AiEtlEmbeddingItemValue() { TextualValue = char.ToString(c)});
                break;

            case ConverterBase.ValueType.LazyCompressedString:
            case ConverterBase.ValueType.LazyString:
                LazyStringValue lazyStringValue = valueType == ConverterBase.ValueType.LazyCompressedString
                    ? ((LazyCompressedStringValue)value).ToLazyStringValue()
                    : (LazyStringValue)value;
                
                values.Add(new AiEtlEmbeddingItemValue() { TextualValue = lazyStringValue});
                break;
            
            case ConverterBase.ValueType.DateTime:
                var dateTime = (DateTime)value;
                var dateAsBytes = dateTime.GetDefaultRavenFormat();
                values.Add(new AiEtlEmbeddingItemValue() { TextualValue = dateAsBytes});
                break;

            case ConverterBase.ValueType.DateTimeOffset:
                var dateTimeOffset = (DateTimeOffset)value;
                var dateTimeOffsetBytes = dateTimeOffset.UtcDateTime.GetDefaultRavenFormat(isUtc: true);
                values.Add(new AiEtlEmbeddingItemValue() { TextualValue = dateTimeOffsetBytes});
                break;

            case ConverterBase.ValueType.TimeSpan:
            {
                var timeSpan = (TimeSpan)value;
                Span<byte> buffer = stackalloc byte[256];
                if (Utf8Formatter.TryFormat(timeSpan, buffer, out var bytesWritten, new('c')) == false)
                    throw new Exception($"Cannot convert {timeSpan} to a string");

                values.Add(new AiEtlEmbeddingItemValue() { TextualValue = Encodings.Utf8.GetString(buffer[..bytesWritten])});
                break;
            }
            case ConverterBase.ValueType.DateOnly:
                var dateOnly = ((DateOnly)value);
                var dateOnlyTextual = dateOnly.ToString(DefaultFormat.DateOnlyFormatToWrite, CultureInfo.InvariantCulture);
                values.Add(new AiEtlEmbeddingItemValue() { TextualValue = dateOnlyTextual});
                break;
            
            case ConverterBase.ValueType.TimeOnly:
                var timeOnly = ((TimeOnly)value);
                var timeOnlyTextual = timeOnly.ToString(DefaultFormat.TimeOnlyFormatToWrite, CultureInfo.InvariantCulture);
                values.Add(new AiEtlEmbeddingItemValue() { TextualValue = timeOnlyTextual});
                break;
            
            case ConverterBase.ValueType.Convertible:
                var iConvertible = (IConvertible)value;
                values.Add(new AiEtlEmbeddingItemValue() { TextualValue = iConvertible.ToString(CultureInfo.InvariantCulture)});
                break;
            
            case ConverterBase.ValueType.Enumerable:
                RuntimeHelpers.EnsureSufficientExecutionStack();
                var iterator = (IEnumerable)value;
                foreach (var item in iterator)
                    WriteValueToEmbeddings(values, item);
                break;

            case ConverterBase.ValueType.DynamicJsonObject:
                var valueAsJson = (DynamicBlittableJson)value;
                values.Add(new AiEtlEmbeddingItemValue() { TextualValue = valueAsJson.ToString()});
                break;

            case ConverterBase.ValueType.Dictionary:
            case ConverterBase.ValueType.ConvertToJson:
            {
                var val = TypeConverter.ToBlittableSupportedType(value);
                if (val is not DynamicJsonValue json)
                {
                    WriteValueToEmbeddings(values, val);
                    return;
                }

                using (var result = Context.ReadObject(json, "index field as json"))
                    WriteValueToEmbeddings(values, result);
                break;
            }

            case ConverterBase.ValueType.BlittableJsonObject:
                var bjo = (BlittableJsonReaderObject)value;
                values.Add(new(){TextualValue =  bjo.ToString()});
                break;
            
            case ConverterBase.ValueType.DynamicNull:
            case ConverterBase.ValueType.Null:
                values.Add(new(){TextualValue = $"null"});
                break;
            
            case ConverterBase.ValueType.EmptyString:
                values.Add(new(){TextualValue = $""});
                break;
            
            default:
                throw new NotSupportedException(valueType + " is not a supported type for AI ETL");
        }
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
