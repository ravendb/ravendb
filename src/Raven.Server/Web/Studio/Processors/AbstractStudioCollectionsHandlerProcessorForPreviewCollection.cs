using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;
using Raven.Client;
using Raven.Server.Documents;
using Raven.Server.Json;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.Studio.Processors;

public abstract class AbstractStudioCollectionsHandlerProcessorForPreviewCollection<TRequestHandler, TResult> : IDisposable
    where TRequestHandler : RequestHandler
{
    private const int ColumnsSamplingLimit = 10;
    private const int StringLengthLimit = 255;

    protected readonly TRequestHandler RequestHandler;

    protected readonly HttpContext HttpContext;

    protected string Collection;

    protected bool IsAllDocsCollection;

    private StringValues _bindings;

    private StringValues _fullBindings;

    protected AbstractStudioCollectionsHandlerProcessorForPreviewCollection([NotNull] TRequestHandler requestHandler)
    {
        RequestHandler = requestHandler ?? throw new ArgumentNullException(nameof(requestHandler));
        HttpContext = requestHandler.HttpContext;
    }

    protected virtual ValueTask InitializeAsync()
    {
        Collection = RequestHandler.GetStringQueryString("collection", required: false);
        _bindings = RequestHandler.GetStringValuesQueryString("binding", required: false);
        _fullBindings = RequestHandler.GetStringValuesQueryString("fullBinding", required: false);

        IsAllDocsCollection = string.IsNullOrEmpty(Collection);
        return ValueTask.CompletedTask;
    }

    protected abstract JsonOperationContext GetContext();

    protected abstract ValueTask<long> GetTotalResultsAsync();

    protected abstract bool NotModified(out string etag);

    protected abstract IAsyncEnumerable<TResult> GetDocumentsAsync();

    protected abstract ValueTask<List<string>> GetAvailableColumnsAsync();

    public async Task ExecuteAsync()
    {
        await InitializeAsync();

        if (NotModified(out var etag))
        {
            RequestHandler.HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
            return;
        }

        if (etag != null)
            HttpContext.Response.Headers["ETag"] = "\"" + etag + "\"";

        var state = CreatePreviewState();
        var documents = GetDocumentsAsync();
        state.TotalResults = await GetTotalResultsAsync();
        state.AvailableColumns = await GetAvailableColumnsAsync();

        state.PropertiesPreviewToSend = IsAllDocsCollection
            ? _bindings.Count > 0 ? new HashSet<string>(_bindings) : new HashSet<string>()
            : _bindings.Count > 0 ? new HashSet<string>(_bindings) : state.AvailableColumns.Take(ColumnsSamplingLimit).Select(x => x.ToString(CultureInfo.InvariantCulture)).ToHashSet();

        state.FullPropertiesToSend = new HashSet<string>(_fullBindings);

        var context = GetContext();


        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            writer.WriteStartObject();
            await WriteResultsAsync(writer, documents, context, state);
            writer.WriteEndObject();
        }
    }

    protected virtual async ValueTask WriteResultsAsync(
        AsyncBlittableJsonTextWriter writer, 
        IAsyncEnumerable<TResult> results, 
        JsonOperationContext context, 
        PreviewState state)
    {
        writer.WritePropertyName(nameof(PreviewCollectionResult.TotalResults));
        writer.WriteInteger(state.TotalResults);
        writer.WriteComma();

        writer.WriteArray(nameof(PreviewCollectionResult.AvailableColumns), state.AvailableColumns);
        writer.WriteComma();

        writer.WritePropertyName(nameof(PreviewCollectionResult.Results));
        writer.WriteStartArray();

        var first = true;
        await foreach (var result in results)
        {
            if (first == false)
                writer.WriteComma();
            first = false;

            WriteResult(writer, context, result, state);
        }

        writer.WriteEndArray();
    }
    
    protected sealed class PreviewCollectionResult
    {
        public List<Document> Results;
        public long TotalResults;
        public List<string> AvailableColumns;
    }

    protected class PreviewState
    {
        private const string ObjectStubsKey = "$o";
        private const string ArrayStubsKey = "$a";
        private const string TrimmedValueKey = "$t";

        public HashSet<string> PropertiesPreviewToSend;
        public HashSet<string> FullPropertiesToSend;
        public long TotalResults;
        public List<string> AvailableColumns;

        public DynamicJsonValue ArrayStubsJson = new DynamicJsonValue();
        public DynamicJsonValue ObjectStubsJson = new DynamicJsonValue();
        public HashSet<LazyStringValue> TrimmedValue = new HashSet<LazyStringValue>();

        public virtual DynamicJsonValue CreateMetadata(BlittableJsonReaderObject current)
        {
            return new DynamicJsonValue(current)
            {
                [ArrayStubsKey] = ArrayStubsJson,
                [ObjectStubsKey] = ObjectStubsJson,
                [TrimmedValueKey] = new DynamicJsonArray(TrimmedValue)
            };
        }
    }

    protected virtual PreviewState CreatePreviewState() => new PreviewState();

    protected abstract void WriteResult(AsyncBlittableJsonTextWriter writer, JsonOperationContext context, TResult document, PreviewState state);

    protected static void WriteDocument(AsyncBlittableJsonTextWriter writer, JsonOperationContext context, Document document, PreviewState state)
    {
        using (document.Data)
        {
            writer.WriteStartObject();

            document.Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata);

            bool first = true;

            var prop = new BlittableJsonReaderObject.PropertyDetails();

            using (var buffers = document.Data.GetPropertiesByInsertionOrder())
            {
                for (int i = 0; i < buffers.Size; i++)
                {
                    unsafe
                    {
                        document.Data.GetPropertyByIndex(buffers.Properties[i], ref prop);
                    }

                    var sendFull = state.FullPropertiesToSend.Contains(prop.Name);
                    if (sendFull || state.PropertiesPreviewToSend.Contains(prop.Name))
                    {
                        var strategy = sendFull ? ValueWriteStrategy.Passthrough : FindWriteStrategy(prop.Token & BlittableJsonReaderBase.TypesMask, prop.Value);

                        if (strategy == ValueWriteStrategy.Passthrough || strategy == ValueWriteStrategy.Trim)
                        {
                            if (first == false)
                            {
                                writer.WriteComma();
                            }

                            first = false;
                        }

                        switch (strategy)
                        {
                            case ValueWriteStrategy.Passthrough:
                                writer.WritePropertyName(prop.Name);
                                writer.WriteValue(prop.Token & BlittableJsonReaderBase.TypesMask, prop.Value);
                                break;

                            case ValueWriteStrategy.SubstituteWithArrayStub:
                                state.ArrayStubsJson[prop.Name] = ((BlittableJsonReaderArray)prop.Value).Length;
                                break;

                            case ValueWriteStrategy.SubstituteWithObjectStub:
                                state.ObjectStubsJson[prop.Name] = ((BlittableJsonReaderObject)prop.Value).Count;
                                break;

                            case ValueWriteStrategy.Trim:
                                writer.WritePropertyName(prop.Name);
                                WriteTrimmedValue(writer, prop.Token & BlittableJsonReaderBase.TypesMask, prop.Value);
                                state.TrimmedValue.Add(prop.Name);
                                break;
                        }
                    }
                }
            }

            if (first == false)
                writer.WriteComma();

            var extraMetadataProperties = state.CreateMetadata(metadata);

            if (metadata != null)
            {
                metadata.Modifications = extraMetadataProperties;

                if (document.Flags.Contain(DocumentFlags.HasCounters) || document.Flags.Contain(DocumentFlags.HasAttachments) ||
                    document.Flags.Contain(DocumentFlags.HasTimeSeries))
                {
                    metadata.Modifications.Remove(Constants.Documents.Metadata.Counters);
                    metadata.Modifications.Remove(Constants.Documents.Metadata.Attachments);
                    metadata.Modifications.Remove(Constants.Documents.Metadata.TimeSeries);
                }

                using (var old = metadata)
                {
                    metadata = context.ReadObject(metadata, document.Id);
                }
            }
            else
            {
                metadata = context.ReadObject(extraMetadataProperties, document.Id);
            }

            writer.WriteMetadata(document, metadata);
            writer.WriteEndObject();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void WriteTrimmedValue(AsyncBlittableJsonTextWriter writer, BlittableJsonToken token, object val)
    {
        switch (token)
        {
            case BlittableJsonToken.String:
                var lazyString = (LazyStringValue)val;
                writer.WriteString(lazyString?.Substring(0,
                    Math.Min(lazyString.Length, StringLengthLimit)));
                break;

            case BlittableJsonToken.CompressedString:
                var lazyCompressedString = (LazyCompressedStringValue)val;
                string actualString = lazyCompressedString.ToString();
                writer.WriteString(actualString.Substring(0, Math.Min(actualString.Length, StringLengthLimit)));
                break;

            default:
                throw new DataMisalignedException($"Unidentified Type {token}");
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ValueWriteStrategy FindWriteStrategy(BlittableJsonToken token, object val)
    {
        switch (token)
        {
            case BlittableJsonToken.String:
                var lazyString = (LazyStringValue)val;
                return lazyString.Length > StringLengthLimit ? ValueWriteStrategy.Trim : ValueWriteStrategy.Passthrough;

            case BlittableJsonToken.Integer:
                return ValueWriteStrategy.Passthrough;

            case BlittableJsonToken.StartArray:
                return ValueWriteStrategy.SubstituteWithArrayStub;

            case BlittableJsonToken.EmbeddedBlittable:
            case BlittableJsonToken.StartObject:
                return ValueWriteStrategy.SubstituteWithObjectStub;

            case BlittableJsonToken.CompressedString:
                var lazyCompressedString = (LazyCompressedStringValue)val;
                return lazyCompressedString.UncompressedSize > StringLengthLimit ? ValueWriteStrategy.Trim : ValueWriteStrategy.Passthrough;

            case BlittableJsonToken.LazyNumber:
            case BlittableJsonToken.Boolean:
            case BlittableJsonToken.Null:
                return ValueWriteStrategy.Passthrough;

            default:
                throw new DataMisalignedException($"Unidentified Type {token}");
        }
    }

    public virtual void Dispose()
    {
        GC.SuppressFinalize(this);
    }

    private enum ValueWriteStrategy
    {
        Passthrough,
        Trim,
        SubstituteWithObjectStub,
        SubstituteWithArrayStub
    }
}
