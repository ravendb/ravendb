using System;
using System.Buffers.Text;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using HtmlAgilityPack;
using Jint;
using Jint.Native;
using Jint.Native.Object;
using Jint.Runtime.Descriptors;
using Jint.Runtime.Interop;
using Microsoft.SemanticKernel.Text;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings.Stats;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Indexes.Persistence;
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

namespace Raven.Server.Documents.ETL.Providers.AI.Embeddings;

internal sealed class EmbeddingsGenerationScriptTransformer : EtlTransformer<AiIntegrationItem, EmbeddingGenerationScriptResult, EmbeddingsGenerationStatsScope, EmbeddingsGenerationPerformanceOperation>
{
    private readonly EmbeddingsGenerationConfiguration _configuration;
    private EmbeddingsGenerationScriptRun _currentRun;
    private readonly PatchRequest _mainScript;

    public EmbeddingsGenerationScriptTransformer(DocumentDatabase database, DocumentsOperationContext context, Transformation transformation, PatchRequest behaviorFunctions, EmbeddingsGenerationConfiguration configuration) : base(database, context, null, behaviorFunctions)
    {
        _configuration = configuration;
        _mainScript = new PatchRequest(transformation.Script, PatchRequestType.EmbeddingsGeneration);
    }

    public override void Initialize(bool debugMode)
    {
        Database.Scripts.GetScriptRunner(_mainScript, true, out DocumentScript);

        if (DocumentScript == null)
            return;

        if (debugMode)
            DocumentScript.DebugMode = true;

        ObjectInstance embeddingsObject = new JsObject(DocumentScript.ScriptEngine);
        embeddingsObject.FastSetProperty("generate", new PropertyDescriptor(new ClrFunction(DocumentScript.ScriptEngine, "generate", EmbeddingsGenerate), false, false, false));
        DocumentScript.ScriptEngine.SetValue("embeddings", embeddingsObject);

        ObjectInstance textObject = new JsObject(DocumentScript.ScriptEngine);
        textObject.FastSetProperty("splitLines", new PropertyDescriptor(new ClrFunction(DocumentScript.ScriptEngine, "splitLines", SplitPlainTextLines), false, false, false));
        textObject.FastSetProperty("splitParagraphs", new PropertyDescriptor(new ClrFunction(DocumentScript.ScriptEngine, "splitParagraphs", SplitPlainTextParagraphs), false, false, false));
        DocumentScript.ScriptEngine.SetValue("text", textObject);

        ObjectInstance markdownObject = new JsObject(DocumentScript.ScriptEngine);
        markdownObject.FastSetProperty("splitLines", new PropertyDescriptor(new ClrFunction(DocumentScript.ScriptEngine, "splitLines", SplitMarkDownLines), false, false, false));
        markdownObject.FastSetProperty("splitParagraphs", new PropertyDescriptor(new ClrFunction(DocumentScript.ScriptEngine, "splitParagraphs", SplitMarkDownParagraphs), false, false, false));
        DocumentScript.ScriptEngine.SetValue("markdown", markdownObject);

        ObjectInstance htmlObject = new JsObject(DocumentScript.ScriptEngine);
        htmlObject.FastSetProperty("strip", new PropertyDescriptor(new ClrFunction(DocumentScript.ScriptEngine, "strip", StripHtml), false, false, false));
        htmlObject.FastSetProperty("splitLines", new PropertyDescriptor(new ClrFunction(DocumentScript.ScriptEngine, "splitLines", SplitHtmlLines), false, false, false));
        DocumentScript.ScriptEngine.SetValue("html", htmlObject);
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

    public override IEnumerable<EmbeddingGenerationScriptResult> GetTransformedResults()
    {
        return _currentRun ?? Enumerable.Empty<EmbeddingGenerationScriptResult>();
    }

    public override void Transform(AiIntegrationItem item, EmbeddingsGenerationStatsScope stats, EtlProcessState state)
    {
        Current = item;
        _currentRun ??= new EmbeddingsGenerationScriptRun();

        if (item.IsDelete)
        {
            var deletedItem = new EmbeddingGenerationScriptResult { DocumentId = Current.DocumentId, DocumentCollectionName = Current.Collection, IsDelete = true };

            _currentRun.Removals.Add(deletedItem);

            return;
        }

        if (_configuration.EmbeddingsTransformation != null)
        {
            Debug.Assert(_configuration.EmbeddingsTransformation.Script != null, "_configuration.EmbeddingsTransformation.Script != null");

            DocumentScript.Run(Context, Context, "execute", new object[] { Current.Document });

            return;
        }

        if (_configuration.EmbeddingsPathConfigurations is { Count: > 0 })
        {
            var aiEtlEmbeddingItem = new EmbeddingGenerationScriptResult
            {
                DocumentId = Current.DocumentId,
                DocumentCollectionName = Current.Collection,
                Values = new Dictionary<string, List<EmbeddingGenerationItem>>()
            };

            foreach (var pathConfiguration in _configuration.EmbeddingsPathConfigurations)
            {
                if (BlittableJsonTraverserHelper.TryRead(BlittableJsonTraverser.Default, Current.Document, pathConfiguration.Path, out var value) == false)
                    continue;

                ref var embeddingValues = ref CollectionsMarshal.GetValueRefOrAddDefault(aiEtlEmbeddingItem.Values, pathConfiguration.Path, out _);
                embeddingValues ??= new();

                var textualValues = new List<string>();
                CollectEmbeddingValues(ref textualValues, value);
                
                var chunks = Documents.AI.TextChunker.ChunkValues(textualValues, pathConfiguration.ChunkingOptions);

                embeddingValues = chunks.Select(chunk => new EmbeddingGenerationItem(chunk)).ToList();
            }

            _currentRun.Additions.Add(aiEtlEmbeddingItem);

            return;
        }

        throw new InvalidOperationException(
            $"Cannot create embeddings because neither {nameof(_configuration.EmbeddingsTransformation)} nor {nameof(_configuration.EmbeddingsPathConfigurations)} were specified in the configuration of AI Integration task");
    }
    
     private void CollectEmbeddingValues(ref List<string> values, object value)
    {
        var valueType = ConverterBase.GetValueTypeUnlikely(value);
        switch (valueType)
        {
            case ConverterBase.ValueType.Double:
            case ConverterBase.ValueType.Numeric:
            case ConverterBase.ValueType.Enum:
            case ConverterBase.ValueType.Boolean:
            case ConverterBase.ValueType.String:
                values.Add(value.ToString());
                break;

            case ConverterBase.ValueType.Char:
                if (value is char c)
                    values.Add( char.ToString(c));
                break;

            case ConverterBase.ValueType.LazyCompressedString:
            case ConverterBase.ValueType.LazyString:
                LazyStringValue lazyStringValue = valueType == ConverterBase.ValueType.LazyCompressedString
                    ? ((LazyCompressedStringValue)value).ToLazyStringValue()
                    : (LazyStringValue)value;

                values.Add(lazyStringValue);
                break;

            case ConverterBase.ValueType.DateTime:
                var dateTime = (DateTime)value;
                var dateAsBytes = dateTime.GetDefaultRavenFormat();
                values.Add(dateAsBytes);
                break;

            case ConverterBase.ValueType.DateTimeOffset:
                var dateTimeOffset = (DateTimeOffset)value;
                var dateTimeOffsetBytes = dateTimeOffset.UtcDateTime.GetDefaultRavenFormat(isUtc: true);
                values.Add(dateTimeOffsetBytes);
                break;

            case ConverterBase.ValueType.TimeSpan:
                {
                    var timeSpan = (TimeSpan)value;
                    Span<byte> buffer = stackalloc byte[256];
                    if (Utf8Formatter.TryFormat(timeSpan, buffer, out var bytesWritten, new('c')) == false)
                        throw new Exception($"Cannot convert {timeSpan} to a string");

                    values.Add(Encodings.Utf8.GetString(buffer[..bytesWritten]));
                    break;
                }
            case ConverterBase.ValueType.DateOnly:
                var dateOnly = (DateOnly)value;
                var dateOnlyTextual = dateOnly.ToString(DefaultFormat.DateOnlyFormatToWrite, CultureInfo.InvariantCulture);
                values.Add(dateOnlyTextual);
                break;

            case ConverterBase.ValueType.TimeOnly:
                var timeOnly = (TimeOnly)value;
                var timeOnlyTextual = timeOnly.ToString(DefaultFormat.TimeOnlyFormatToWrite, CultureInfo.InvariantCulture);
                values.Add(timeOnlyTextual);
                break;

            case ConverterBase.ValueType.Convertible:
                var iConvertible = (IConvertible)value;
                values.Add(iConvertible.ToString(CultureInfo.InvariantCulture));
                break;

            case ConverterBase.ValueType.Enumerable:
                RuntimeHelpers.EnsureSufficientExecutionStack();
                var iterator = (IEnumerable)value;
                foreach (var item in iterator)
                    CollectEmbeddingValues(ref values, item);
                break;

            case ConverterBase.ValueType.DynamicJsonObject:
                var valueAsJson = (DynamicBlittableJson)value;
                values.Add(valueAsJson.ToString());
                break;

            case ConverterBase.ValueType.Dictionary:
            case ConverterBase.ValueType.ConvertToJson:
                {
                    var val = TypeConverter.ToBlittableSupportedType(value);
                    if (val is not DynamicJsonValue json)
                    {
                        CollectEmbeddingValues(ref values, val);
                        return;
                    }

                    using (var result = Context.ReadObject(json, "index field as json"))
                        CollectEmbeddingValues(ref values, result);
                    break;
                }

            case ConverterBase.ValueType.BlittableJsonObject:
                var bjo = (BlittableJsonReaderObject)value;
                values.Add(bjo.ToString());
                break;

            case ConverterBase.ValueType.DynamicNull:
            case ConverterBase.ValueType.Null:
                values.Add("null");
                break;

            case ConverterBase.ValueType.EmptyString:
                values.Add("");
                break;

            default:
                throw new NotSupportedException(valueType + " is not a supported type for AI Integration");
        }
    }

#pragma warning disable SKEXP0050
    // todo non-default token counter
    private JsValue SplitMarkDownLines(JsValue self, JsValue[] args)
    {
        const string methodSignature = "markdown.splitLines(text, maxTokensPerLine)";

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
        const string methodSignature = "markdown.splitParagraphs(lines, maxTokensPerLine)";

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
        const string methodSignature = "text.splitLines(text, maxTokensPerLine)";

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
        const string methodSignature = "text.splitParagraphs(lines, maxTokensPerLine)";

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

    private JsValue SplitHtmlLines(JsValue self, JsValue[] args)
    {
        const string methodSignature = "html.splitLines(text, maxTokensPerLine)";

        if (args.Length != 2)
            ThrowInvalidScriptMethodCall($"{methodSignature} has to be called with 2 arguments");

        if (args[0].IsString() == false)
            ThrowInvalidScriptMethodCall($"{methodSignature} first argument must be a string");

        if (args[1].IsNumber() == false)
            ThrowInvalidScriptMethodCall($"{methodSignature} second argument must be a number");

        string text = StripHtml(args[0].AsString());
        var chunks = TextChunker.SplitPlainTextLines(text, (int)args[1].AsNumber());

        var jsChunks = new JsValue[chunks.Count];
        for (var i = 0; i < chunks.Count; i++)
        {
            jsChunks[i] = new JsString(chunks[i]);
        }

        var jsArray = new JsArray(DocumentScript.ScriptEngine, jsChunks);

        return jsArray;
    }

    private JsValue StripHtml(JsValue self, JsValue[] args)
    {
        const string methodSignature = "html.strip(htmlText)";

        if (args.Length != 1)
            ThrowInvalidScriptMethodCall($"{methodSignature} has to be called with 1 argument");

        if (args[0].IsString() == false)
            ThrowInvalidScriptMethodCall($"{methodSignature} first argument must be a string");


        var text = StripHtml(args[0].AsString());

        return new JsString(text);
    }

    private string StripHtml(string input)
    {
        if (string.IsNullOrEmpty(input))
            return input;

        var htmlDoc = new HtmlDocument();
        htmlDoc.LoadHtml(input);
        return htmlDoc.DocumentNode.InnerText;
    }

    private JsValue EmbeddingsGenerate(JsValue self, JsValue[] args)
    {
        const string methodSignature = "embeddings.generate(object)";

        if (args.Length != 1)
            ThrowInvalidScriptMethodCall($"{methodSignature} has to be called with 1 argument");

        if (args[0].IsObject() == false)
            ThrowInvalidScriptMethodCall($"{methodSignature} first argument must be an object");

        var mainObj = args[0].AsObject();

        var aiEtlEmbeddingItem = new EmbeddingGenerationScriptResult()
        {
            DocumentId = Current.DocumentId,
            DocumentCollectionName = Current.Collection,
            Values = new Dictionary<string, List<EmbeddingGenerationItem>>()
        };

        foreach (var propertyKey in mainObj.GetOwnPropertyKeys())
        {
            var propertyName = propertyKey.AsString();

            if (aiEtlEmbeddingItem.Values.TryGetValue(propertyName, out var values) == false)
                aiEtlEmbeddingItem.Values[propertyName] = values = new List<EmbeddingGenerationItem>();

            mainObj.TryGetValue(propertyKey, out JsValue value);

            if (value.IsString())
            {
                values.Add(new EmbeddingGenerationItem(value.AsString()));
            }

            else if (value.IsArray())
            {
                var jsArray = value.AsArray();

                foreach (var jsValue in jsArray)
                    values.Add(new EmbeddingGenerationItem(jsValue.AsString()));
            }
        }

        _currentRun.Additions.Add(aiEtlEmbeddingItem);

        return JsValue.Null;
    }
}
#pragma warning restore SKEXP0050
