using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Extensions;
using Raven.Server.Documents;
using Raven.Server.Documents.Operations;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.Studio
{
    public class StudioCollectionsHandler : DatabaseRequestHandler
    {
        [ThreadStatic]
        private static BlittableJsonReaderObject.PropertiesInsertionBuffer _buffers;

        private const int ColumnsSamplingLimit = 10;
        private const int StringLengthLimit = 255;

        private const string ObjectStubsKey = "$o";
        private const string ArrayStubsKey = "$a";
        private const string TrimmedValueKey = "$t";

        [RavenAction("/databases/*/studio/collections/preview", "GET")]
        public Task PreviewCollection()
        {
            var start = GetStart();
            var pageSize = GetPageSize();
            var collection = GetStringQueryString("collection", required: false);
            var bindings = GetStringValuesQueryString("binding", required: false);
            var fullBindings = GetStringValuesQueryString("fullBinding", required: false);

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                Document[] documents;
                HashSet<LazyStringValue> availableColumns;
                long totalResults;
                long etag;
                HashSet<string> propertiesPreviewToSend;
                HashSet<string> fullPropertiesToSend = new HashSet<string>(fullBindings);
                ;

                // compute etag only - maybe we can respond with NotModified?
                if (string.IsNullOrEmpty(collection))
                {
                    totalResults = Database.DocumentsStorage.GetNumberOfDocuments(context);
                    var lastEtag = DocumentsStorage.ReadLastEtag(context.Transaction.InnerTransaction);
                    etag = DocumentsStorage.ComputeEtag(lastEtag, totalResults);
                }
                else
                {
                    totalResults = Database.DocumentsStorage.GetCollection(collection, context).Count;
                    var lastCollectionEtag = Database.DocumentsStorage.GetLastDocumentEtag(context, collection);
                    etag = DocumentsStorage.ComputeEtag(lastCollectionEtag, totalResults);
                }

                if (GetLongFromHeaders("If-None-Match") == etag)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                    return Task.CompletedTask;
                }
                HttpContext.Response.Headers["ETag"] = "\"" + etag + "\"";



                if (string.IsNullOrEmpty(collection))
                {
                    documents = Database.DocumentsStorage.GetDocumentsInReverseEtagOrder(context, start, pageSize).ToArray();
                    availableColumns = ExtractColumnNames(documents, context);
                    propertiesPreviewToSend = bindings.Count > 0 ? new HashSet<string>(bindings) : new HashSet<string>();
                }
                else
                {
                    documents = Database.DocumentsStorage.GetDocumentsInReverseEtagOrder(context, collection, start, pageSize).ToArray();
                    availableColumns = ExtractColumnNames(documents, context);
                    propertiesPreviewToSend = bindings.Count > 0 ? new HashSet<string>(bindings) : availableColumns.Take(ColumnsSamplingLimit).Select(x => x.ToString()).ToHashSet();
                }

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("Results");

                    {
                        writer.WriteStartArray();

                        var first = true;
                        foreach (var document in documents)
                        {
                            if (first == false)
                                writer.WriteComma();
                            first = false;

                            using (document.Data)
                            {
                                WriteDocument(writer, context, document, propertiesPreviewToSend, fullPropertiesToSend);
                            }
                        }

                        writer.WriteEndArray();
                    }

                    writer.WriteComma();

                    writer.WritePropertyName("TotalResults");
                    writer.WriteInteger(totalResults);

                    writer.WriteComma();

                    writer.WritePropertyName("AvailableColumns");
                    writer.WriteArray(availableColumns);

                    writer.WriteEndObject();
                }

                return Task.CompletedTask;
            }
        }

        private void WriteDocument(BlittableJsonTextWriter writer, DocumentsOperationContext context, Document document, HashSet<string> propertiesPreviewToSend, HashSet<string> fullPropertiesToSend)
        {
            if (_buffers == null)
                _buffers = new BlittableJsonReaderObject.PropertiesInsertionBuffer();

            writer.WriteStartObject();

            BlittableJsonReaderObject metadata = null;
            document.Data.TryGet(Constants.Documents.Metadata.Key, out metadata);

            bool first = true;

            var objectsStubs = new HashSet<LazyStringValue>();
            var arraysStubs = new HashSet<LazyStringValue>();
            var trimmedValue = new HashSet<LazyStringValue>();

            var size = document.Data.GetPropertiesByInsertionOrder(_buffers);
            var prop = new BlittableJsonReaderObject.PropertyDetails();

            for (int i = 0; i < size; i++)
            {
                document.Data.GetPropertyByIndex(_buffers.Properties[i], ref prop);
                var sendFull = fullPropertiesToSend.Contains(prop.Name);
                if (sendFull || propertiesPreviewToSend.Contains(prop.Name))
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
                            arraysStubs.Add(prop.Name);
                            break;
                        case ValueWriteStrategy.SubstituteWithObjectStub:
                            objectsStubs.Add(prop.Name);
                            break;
                        case ValueWriteStrategy.Trim:
                            writer.WritePropertyName(prop.Name);
                            WriteTrimmedValue(writer, prop.Token & BlittableJsonReaderBase.TypesMask, prop.Value);
                            trimmedValue.Add(prop.Name);
                            break;
                    }
                }
            }
            if (first == false)
                writer.WriteComma();

            var extraMetadataProperties = new DynamicJsonValue
            {
                [ObjectStubsKey] = new DynamicJsonArray(objectsStubs),
                [ArrayStubsKey] = new DynamicJsonArray(arraysStubs),
                [TrimmedValueKey] = new DynamicJsonArray(trimmedValue)
            };

            if (metadata != null)
            {
                metadata.Modifications = extraMetadataProperties;
                metadata = context.ReadObject(metadata, document.Key);
            }
            else
            {
                metadata = context.ReadObject(extraMetadataProperties, document.Key);
            }

            writer.WriteMetadata(document, metadata);
            writer.WriteEndObject();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteTrimmedValue(BlittableJsonTextWriter writer, BlittableJsonToken token, object val)
        {
            switch (token)
            {
                case BlittableJsonToken.String:
                    var lazyString = (LazyStringValue)val;
                    writer.WriteString(lazyString.Substring(0, StringLengthLimit));
                    break;
                case BlittableJsonToken.CompressedString:
                    var lazyCompressedString = (LazyCompressedStringValue)val;
                    writer.WriteString(lazyCompressedString.Substring(0, StringLengthLimit));
                    break;

                default:
                    throw new DataMisalignedException($"Unidentified Type {token}");
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ValueWriteStrategy FindWriteStrategy(BlittableJsonToken token, object val)
        {
            switch (token)
            {
                case BlittableJsonToken.String:
                    var lazyString = (LazyStringValue) val;
                    return lazyString.Length > StringLengthLimit ? ValueWriteStrategy.Trim : ValueWriteStrategy.Passthrough;
                case BlittableJsonToken.Integer:
                    return ValueWriteStrategy.Passthrough;
                case BlittableJsonToken.StartArray:
                    return ValueWriteStrategy.SubstituteWithArrayStub;
                case BlittableJsonToken.EmbeddedBlittable:
                case BlittableJsonToken.StartObject:
                    return ValueWriteStrategy.SubstituteWithObjectStub;
                case BlittableJsonToken.CompressedString:
                    var lazyCompressedString = (LazyCompressedStringValue) val;
                    return lazyCompressedString.UncompressedSize > StringLengthLimit ? ValueWriteStrategy.Trim : ValueWriteStrategy.Passthrough;
                case BlittableJsonToken.Float:
                case BlittableJsonToken.Boolean:
                case BlittableJsonToken.Null:
                    return ValueWriteStrategy.Passthrough;
                default:
                    throw new DataMisalignedException($"Unidentified Type {token}");
            }
        }
        
        private static HashSet<LazyStringValue> ExtractColumnNames(Document[] documents, DocumentsOperationContext context)
        {
            if (_buffers == null)
                _buffers = new BlittableJsonReaderObject.PropertiesInsertionBuffer();

            var columns = new HashSet<LazyStringValue>();

            foreach (var document in documents)
            {
                var size = document.Data.GetPropertiesByInsertionOrder(_buffers);
                var prop = new BlittableJsonReaderObject.PropertyDetails();

                for (int i = 0; i < size; i++)
                {
                    document.Data.GetPropertyByIndex(_buffers.Properties[i], ref prop);
                    var propName = prop.Name;
                    if (!columns.Contains(propName))
                    {
                        columns.Add(prop.Name);
                    }
                }
            }

            var metadataField = context.GetLazyStringForFieldWithCaching(Constants.Documents.Metadata.Key);
            columns.Remove(metadataField);

            return columns;
        }

        [RavenAction("/databases/*/studio/collections/docs", "DELETE")]
        public Task Delete()
        {
            DocumentsOperationContext context;
            var returnContextToPool = ContextPool.AllocateOperationContext(out context);

            var excludeIds = new HashSet<LazyStringValue>();

            var reader = context.Read(RequestBodyStream(), "ExcludeIds");
            BlittableJsonReaderArray idsBlittable;
            if (reader.TryGet("ExcludeIds", out idsBlittable)) 
            {
                foreach (LazyStringValue item in idsBlittable)
                {
                    excludeIds.Add(item);
                }
            }

            ExecuteCollectionOperation((runner, collectionName, options, onProgress, token) => Task.Run(() => runner.ExecuteDelete(collectionName, options, context, onProgress, token)),
                context, returnContextToPool, DatabaseOperations.OperationType.DeleteByCollection, excludeIds);
            return Task.CompletedTask;
        }

       
        private void ExecuteCollectionOperation(Func<CollectionRunner, string, CollectionOperationOptions, Action<IOperationProgress>, OperationCancelToken, Task<IOperationResult>> operation, DocumentsOperationContext context, IDisposable returnContextToPool, DatabaseOperations.OperationType operationType, HashSet<LazyStringValue> excludeIds)
        {
            var collectionName = GetStringQueryString("name");

            var token = CreateTimeLimitedOperationToken();

            var collectionRunner = new StudioCollectionRunner(Database, context, excludeIds);

            var operationId = Database.Operations.GetNextOperationId();

            // use default options
            var options = new CollectionOperationOptions();

            var task = Database.Operations.AddOperation(collectionName, operationType, onProgress =>
                    operation(collectionRunner, collectionName, options, onProgress, token), operationId, token);

            task.ContinueWith(_ => returnContextToPool.Dispose());

            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteOperationId(context, operationId);
            }
        }

        private enum ValueWriteStrategy
        {
            Passthrough,
            Trim,
            SubstituteWithObjectStub,
            SubstituteWithArrayStub
        }
    }
}