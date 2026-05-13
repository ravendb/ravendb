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
using JetBrains.Annotations;
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

internal sealed class EmbeddingsGenerationScriptTransformer : EtlTransformer<EmbeddingsGenerationItem, EmbeddingGenerationScriptResult, EmbeddingsGenerationStatsScope, EmbeddingsGenerationPerformanceOperation>
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
        embeddingsObject.SetClfFunc("generate", EmbeddingsGenerate);
        DocumentScript.ScriptEngine.SetValue("embeddings", embeddingsObject);

        ObjectInstance textObject = new JsObject(DocumentScript.ScriptEngine);
        textObject.SetClfFunc("split", SplitPlainText);
        textObject.SetClfFunc("splitLines",  SplitPlainTextLines);
        textObject.SetClfFunc("splitParagraphs", SplitPlainTextParagraphs);
        textObject.SetClfFunc("withContext", WithContext);
        DocumentScript.ScriptEngine.SetValue("text", textObject);

        ObjectInstance markdownObject = new JsObject(DocumentScript.ScriptEngine);
        markdownObject.SetClfFunc("splitLines", SplitMarkDownLines);
        markdownObject.SetClfFunc("splitParagraphs", SplitMarkDownParagraphs);
        DocumentScript.ScriptEngine.SetValue("markdown", markdownObject);

        ObjectInstance htmlObject = new JsObject(DocumentScript.ScriptEngine);
        htmlObject.SetClfFunc("strip", StripHtml);
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

    public override void Transform(EmbeddingsGenerationItem item, EmbeddingsGenerationStatsScope stats, EtlProcessState state)
    {
        Current = item;
        _currentRun ??= new EmbeddingsGenerationScriptRun();

        if (item.IsDelete)
        {
            _currentRun.Removals.Add(new EmbeddingGenerationScriptResult(Current.DocumentId, Current.Collection));

            return;
        }

        if (_configuration.EmbeddingsTransformation != null)
        {
            Debug.Assert(_configuration.EmbeddingsTransformation.Script != null, "_configuration.EmbeddingsTransformation.Script != null");

            DocumentScript.Run(Context, Context, "execute", [Current.Document]);

            return;
        }

        if (_configuration.EmbeddingsPathConfigurations is { Count: > 0 })
        {
            var aiEtlEmbeddingItem = new EmbeddingGenerationScriptResult(Current.DocumentId, Current.Collection);

            foreach (var pathConfiguration in _configuration.EmbeddingsPathConfigurations)
            {
                if (BlittableJsonTraverserHelper.TryRead(BlittableJsonTraverser.Default, Current.Document, pathConfiguration.Path, out var value) == false)
                    continue;

                ref var filedValues = ref CollectionsMarshal.GetValueRefOrAddDefault(aiEtlEmbeddingItem.Fields, pathConfiguration.Path, out _);
                filedValues ??= [];
                CollectEmbeddingValues(filedValues, pathConfiguration.ChunkingOptions, value);
            }

            _currentRun.Additions.Add(aiEtlEmbeddingItem);

            return;
        }

        throw new InvalidOperationException(
            $"Cannot create embeddings because neither {nameof(_configuration.EmbeddingsTransformation)} nor {nameof(_configuration.EmbeddingsPathConfigurations)} were specified in the configuration of Embeddings Generation task");
    }
    
     private void CollectEmbeddingValues(List<(string,ChunkingOptions)> values, ChunkingOptions chunkingOptions, object value)
    {
        var valueType = ConverterBase.GetValueType(value, properlyParseDictionaryToStoredField: true);
        switch (valueType)
        {
            case ConverterBase.ValueType.Double:
            case ConverterBase.ValueType.Numeric:
            case ConverterBase.ValueType.Enum:
            case ConverterBase.ValueType.Boolean:
            case ConverterBase.ValueType.String:
                values.Add((value.ToString(),chunkingOptions));
                break;

            case ConverterBase.ValueType.Char:
                if (value is char c)
                    values.Add( (char.ToString(c), chunkingOptions));
                break;

            case ConverterBase.ValueType.LazyCompressedString:
            case ConverterBase.ValueType.LazyString:
                LazyStringValue lazyStringValue = valueType == ConverterBase.ValueType.LazyCompressedString
                    ? ((LazyCompressedStringValue)value).ToLazyStringValue()
                    : (LazyStringValue)value;

                values.Add((lazyStringValue, chunkingOptions));
                break;

            case ConverterBase.ValueType.DateTime:
                var dateTime = (DateTime)value;
                var dateAsBytes = dateTime.GetDefaultRavenFormat();
                values.Add((dateAsBytes,chunkingOptions));
                break;

            case ConverterBase.ValueType.DateTimeOffset:
                var dateTimeOffset = (DateTimeOffset)value;
                var dateTimeOffsetBytes = dateTimeOffset.UtcDateTime.GetDefaultRavenFormat(isUtc: true);
                values.Add((dateTimeOffsetBytes,chunkingOptions));
                break;

            case ConverterBase.ValueType.TimeSpan:
                {
                    var timeSpan = (TimeSpan)value;
                    Span<byte> buffer = stackalloc byte[256];
                    if (Utf8Formatter.TryFormat(timeSpan, buffer, out var bytesWritten, new('c')) == false)
                        throw new Exception($"Cannot convert {timeSpan} to a string");

                    values.Add((Encodings.Utf8.GetString(buffer[..bytesWritten]), chunkingOptions));
                    break;
                }
            case ConverterBase.ValueType.DateOnly:
                var dateOnly = (DateOnly)value;
                var dateOnlyTextual = dateOnly.ToString(DefaultFormat.DateOnlyFormatToWrite, CultureInfo.InvariantCulture);
                values.Add((dateOnlyTextual, chunkingOptions));
                break;

            case ConverterBase.ValueType.TimeOnly:
                var timeOnly = (TimeOnly)value;
                var timeOnlyTextual = timeOnly.ToString(DefaultFormat.TimeOnlyFormatToWrite, CultureInfo.InvariantCulture);
                values.Add((timeOnlyTextual, chunkingOptions));
                break;

            case ConverterBase.ValueType.Convertible:
                var iConvertible = (IConvertible)value;
                values.Add((iConvertible.ToString(CultureInfo.InvariantCulture), chunkingOptions));
                break;

            case ConverterBase.ValueType.Enumerable:
                RuntimeHelpers.EnsureSufficientExecutionStack();
                var iterator = (IEnumerable)value;
                foreach (var item in iterator)
                    CollectEmbeddingValues(values, chunkingOptions, item);
                break;

            case ConverterBase.ValueType.DynamicJsonObject:
                var valueAsJson = (DynamicBlittableJson)value;
                values.Add((valueAsJson.ToString(), chunkingOptions));
                break;

            case ConverterBase.ValueType.Dictionary:
            case ConverterBase.ValueType.ConvertToJson:
                {
                    var val = TypeConverter.ToBlittableSupportedType(value);
                    if (val is not DynamicJsonValue json)
                    {
                        CollectEmbeddingValues(values, chunkingOptions, val);
                        return;
                    }

                    using (var result = Context.ReadObject(json, "index field as json"))
                        CollectEmbeddingValues(values, chunkingOptions, result);
                    break;
                }

            case ConverterBase.ValueType.BlittableJsonObject:
                var bjo = (BlittableJsonReaderObject)value;
                values.Add((bjo.ToString(), chunkingOptions));
                break;

            case ConverterBase.ValueType.DynamicNull:
            case ConverterBase.ValueType.Null:
                values.Add(("null",chunkingOptions));
                break;

            case ConverterBase.ValueType.EmptyString:
                values.Add((string.Empty,chunkingOptions));
                break;

            default:
                throw new NotSupportedException(valueType + " is not a supported type for Embeddings Generation");
        }
    }

#pragma warning disable SKEXP0050
    private JsValue SplitMarkDownLines(JsValue self, JsValue[] args)
    {
        const string methodSignature = "markdown.splitLines(text | [text], maxTokensPerLine, contextPrefix?)";

        if (args.Length < 2 || args.Length > 3)
            ThrowInvalidScriptMethodCall($"{methodSignature} has to be called with either 2 or 3 arguments");

        return ChunkFunc(self, args, methodSignature, ChunkingMethod.MarkDownSplitLines, supportsOverlap: false);
    }

    private JsValue SplitMarkDownParagraphs(JsValue self, JsValue[] args)
    {
        const string methodSignature = "markdown.splitParagraphs(line | [line], maxTokensPerLine, overlapTokens?, contextPrefix?)";

        if (args.Length < 2 || args.Length > 4)
            ThrowInvalidScriptMethodCall($"{methodSignature} has to be called with 2, 3 or 4 arguments");

        return ChunkFunc(self, args, methodSignature, ChunkingMethod.MarkDownSplitParagraphs, supportsOverlap: true);
    }

    private JsValue SplitPlainText(JsValue self, JsValue[] args)
    {
        const string methodSignature = "text.split(text | [text], maxTokensPerLine, contextPrefix?)";

        if (args.Length < 2 || args.Length > 3)
            ThrowInvalidScriptMethodCall($"{methodSignature} has to be called with either 2 or 3 arguments");

        return ChunkFunc(self, args, methodSignature, ChunkingMethod.PlainTextSplit, supportsOverlap: false);
    }

    private JsValue SplitPlainTextLines(JsValue self, JsValue[] args)
    {
        const string methodSignature = "text.splitLines(text | [text], maxTokensPerLine, contextPrefix?)";

        if (args.Length < 2 || args.Length > 3)
            ThrowInvalidScriptMethodCall($"{methodSignature} has to be called with either 2 or 3 arguments");

        return ChunkFunc(self, args, methodSignature, ChunkingMethod.PlainTextSplitLines, supportsOverlap: false);
    }

    private JsValue SplitPlainTextParagraphs(JsValue self, JsValue[] args)
    {
        const string methodSignature = "text.splitParagraphs(line | [line], maxTokensPerLine, overlapTokens?, contextPrefix?)";

        if (args.Length < 2 || args.Length > 4)
            ThrowInvalidScriptMethodCall($"{methodSignature} has to be called with 2, 3 or 4 arguments");

        return ChunkFunc(self, args, methodSignature, ChunkingMethod.PlainTextSplitParagraphs, supportsOverlap: true);
    }

    private JsValue StripHtml(JsValue self, JsValue[] args)
    {
        const string methodSignature = "html.strip(htmlText | [htmlText], maxTokensPerChunk, contextPrefix?)";

        if (args.Length < 2 || args.Length > 3)
            ThrowInvalidScriptMethodCall($"{methodSignature} has to be called with either 2 or 3 arguments");

        return ChunkFunc(self, args, methodSignature, ChunkingMethod.HtmlStrip, supportsOverlap: false);
    }

    private JsValue WithContext(JsValue self, JsValue[] args)
    {
        const string methodSignature = "text.withContext(value | [value], contextPrefix)";

        if (args.Length != 2)
            ThrowInvalidScriptMethodCall($"{methodSignature} has to be called with 2 arguments");

        if (args[0].IsNull() || args[0].IsUndefined())
            return JsValue.Undefined;

        if (args[0].IsString() == false && args[0].IsArray() == false)
            ThrowInvalidScriptMethodCall($"{methodSignature} first argument must be a string or a string array");

        if (args[1].IsString() == false)
            ThrowInvalidScriptMethodCall($"{methodSignature} second argument must be a string");

        var prefix = args[1].AsString();

        if (string.IsNullOrWhiteSpace(prefix))
            ThrowInvalidScriptMethodCall($"{methodSignature} second argument (contextPrefix) cannot be empty or whitespace-only");

        ChunkingOptions chunkingOptions = new()
        {
            NoChunking = true,
            ContextPrefix = prefix
        };

        var result = new ObjectForChunking(((JsObject)self).Engine);

        if (args[0].IsString())
        {
            result.Value.Add((args[0].AsString(), chunkingOptions));
            return result;
        }

        foreach (var line in args[0].AsArray())
        {
            if (line.IsNull() || line.IsUndefined())
                continue;

            if (line.IsString() == false)
                throw new NotSupportedException("Only string arrays are supported, but got an array value of '" + line + "' in " + methodSignature);

            result.Value.Add((line.AsString(), chunkingOptions));
        }

        return result;
    }

    public class ObjectForChunking([NotNull] Engine engine) : ObjectInstance(engine)
    {
        public readonly List<(string,ChunkingOptions)> Value = [];
    }

    private static JsValue ChunkFunc(JsValue self, JsValue[] args, string methodSignature, ChunkingMethod chunkingMethod, bool supportsOverlap)
    {
        if (args[0].IsNull() || args[0].IsUndefined())
            return JsValue.Undefined;

        if (args[0].IsString() == false && args[0].IsArray() == false)
            ThrowInvalidScriptMethodCall($"{methodSignature} first argument must be a string or a string array");

        ChunkingOptions chunkingOptions = BuildChunkingOptions(args, methodSignature, chunkingMethod, supportsOverlap);

        var result = new ObjectForChunking(((JsObject)self).Engine);

        if (args[0].IsString())
        {
            result.Value.Add((args[0].AsString(), chunkingOptions));
            return result;
        }

        if (args[0].IsArray())
        {
            foreach (var line in args[0].AsArray())
            {
                if (line.IsNull() || line.IsUndefined())
                    continue;

                if (line.IsString() is false)
                    throw new NotSupportedException("Only string arrays are supported, but got an array value of '" + line + "' in " + methodSignature);
                result.Value.Add((line.AsString(), chunkingOptions));
            }
            return result;
        }

        throw new NotSupportedException("Only string or string arrays are supported in " + methodSignature + " but got " + args[0]);
    }

    private static ChunkingOptions BuildChunkingOptions(JsValue[] args, string methodSignature, ChunkingMethod chunkingMethod, bool supportsOverlap)
    {
        if (args[1].IsNumber() == false)
            ThrowInvalidScriptMethodCall($"{methodSignature} second argument must be a number");

        int overlapTokens = 0;
        string contextPrefix = null;

        if (supportsOverlap)
        {
            // Layout: (value, maxTokens, overlap?, prefix?)
            if (args.Length >= 3)
            {
                if (args[2].IsNumber() == false)
                    ThrowInvalidScriptMethodCall($"{methodSignature} third argument (overlapTokens) must be a number");

                overlapTokens = (int)args[2].AsNumber();
            }

            if (args.Length == 4)
            {
                if (args[3].IsString() == false)
                    ThrowInvalidScriptMethodCall($"{methodSignature} fourth argument (contextPrefix) must be a string");

                contextPrefix = args[3].AsString();

                if (string.IsNullOrWhiteSpace(contextPrefix))
                    ThrowInvalidScriptMethodCall($"{methodSignature} fourth argument (contextPrefix) cannot be empty or whitespace-only");
            }
        }
        else
        {
            // Layout: (value, maxTokens, prefix?)
            if (args.Length == 3)
            {
                if (args[2].IsString() == false)
                    ThrowInvalidScriptMethodCall($"{methodSignature} third argument (contextPrefix) must be a string");

                contextPrefix = args[2].AsString();

                if (string.IsNullOrWhiteSpace(contextPrefix))
                    ThrowInvalidScriptMethodCall($"{methodSignature} third argument (contextPrefix) cannot be empty or whitespace-only");
            }
        }

        return new ChunkingOptions
        {
            MaxTokensPerChunk = (int)args[1].AsNumber(),
            ChunkingMethod = chunkingMethod,
            OverlapTokens = overlapTokens,
            ContextPrefix = contextPrefix
        };
    }

    private JsValue EmbeddingsGenerate(JsValue self, JsValue[] args)
    {
        const string methodSignature = "embeddings.generate(object)";

        if (args.Length != 1)
            ThrowInvalidScriptMethodCall($"{methodSignature} has to be called with 1 argument");

        if (args[0].IsObject() == false)
            ThrowInvalidScriptMethodCall($"{methodSignature} first argument must be an object");

        var mainObj = args[0].AsObject();

        EmbeddingGenerationScriptResult aiEtlEmbeddingItem;
        bool isNewInstance;
        if (_currentRun.Additions.Count > 0 && _currentRun.Additions[^1].DocumentId == Current.DocumentId)
        {
            isNewInstance = false;
            aiEtlEmbeddingItem = _currentRun.Additions[^1];
        }
        else
        {
            isNewInstance = true;
            aiEtlEmbeddingItem = new EmbeddingGenerationScriptResult(Current.DocumentId, Current.Collection);
        }

        foreach (var propertyKey in mainObj.GetOwnPropertyKeys())
        {
            var propertyName = propertyKey.AsString();
            ref var field = ref CollectionsMarshal.GetValueRefOrAddDefault(aiEtlEmbeddingItem.Fields, propertyName, out _);

            if (mainObj.TryGetValue(propertyKey, out JsValue value) is false ||
                value.IsNull() ||
                value.IsUndefined())
                continue;
            
            if (value is ObjectForChunking ofc)
            {
                field = ofc.Value;
                continue;
            }

            field ??= [];
            if (value.IsString())
            {
                field.Add((value.AsString(), _configuration.EmbeddingsTransformation.ChunkingOptions));
            }
            else if (value.IsArray())
            {
                var jsArray = value.AsArray();

                foreach (var jsValue in jsArray)
                {
                    if (jsValue is ObjectForChunking i)
                    {
                        // to handle
                        //{ Text: [html.strip(this.Body), markdown.splitLines(this.Title)] }
                        field.AddRange(i.Value);
                        continue;
                    }
                    if (jsValue.IsString() is false)
                    {
                        throw new NotSupportedException($"Only string arrays are supported, but got '{jsValue}' for '{propertyName}'");
                    }
                    field.Add((jsValue.AsString(), _configuration.EmbeddingsTransformation.ChunkingOptions));
                }
            }
            else
            {
                throw new NotSupportedException($"Only strings, string arrays and html.strip(), markdown.splitLines(), etc are supported, but got '{value}' for '{propertyName}'");
            }
        }

        if (isNewInstance)
        {
            _currentRun.Additions.Add(aiEtlEmbeddingItem);
        }

        return JsValue.Null;
    }
}
#pragma warning restore SKEXP0050
