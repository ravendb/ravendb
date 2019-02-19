using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.Studio
{
    public class StudioCollectionsHandler : DatabaseRequestHandler
    {
        private const int ColumnsSamplingLimit = 10;
        private const int StringLengthLimit = 255;

        private const string ObjectStubsKey = "$o";
        private const string ArrayStubsKey = "$a";
        private const string TrimmedValueKey = "$t";

        [RavenAction("/databases/*/studio/collections/preview", "GET", AuthorizationStatus.ValidUser)]
        public Task PreviewCollection()
        {
            var start = GetStart();
            var pageSize = GetPageSize();
            var collection = GetStringQueryString("collection", required: false);
            var bindings = GetStringValuesQueryString("binding", required: false);
            var fullBindings = GetStringValuesQueryString("fullBinding", required: false);

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                Document[] documents;
                HashSet<LazyStringValue> availableColumns;
                HashSet<string> propertiesPreviewToSend;
                HashSet<string> fullPropertiesToSend = new HashSet<string>(fullBindings);

                long totalResults;
                string changeVector;
                string etag = null;

                if (string.IsNullOrEmpty(collection))
                {
                    changeVector = DocumentsStorage.GetDatabaseChangeVector(context);
                    totalResults = Database.DocumentsStorage.GetNumberOfDocuments(context);
                    etag = $"{changeVector}/{totalResults}";
                }
                else
                {
                    changeVector = Database.DocumentsStorage.GetLastDocumentChangeVector(context, collection);
                    totalResults = Database.DocumentsStorage.GetCollection(collection, context).Count;

                    if (changeVector != null)
                        etag = $"{changeVector}/{totalResults}";
                }

                if (etag != null && GetStringFromHeaders("If-None-Match") == etag)
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

                    writer.WriteArray("AvailableColumns", availableColumns);

                    writer.WriteEndObject();
                }

                return Task.CompletedTask;
            }
        }

        private void WriteDocument(BlittableJsonTextWriter writer, JsonOperationContext context, Document document, HashSet<string> propertiesPreviewToSend, HashSet<string> fullPropertiesToSend)
        {
            writer.WriteStartObject();

            document.Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata);

            bool first = true;

            var objectsStubs = new HashSet<LazyStringValue>();
            var arraysStubs = new HashSet<LazyStringValue>();
            var trimmedValue = new HashSet<LazyStringValue>();

            var prop = new BlittableJsonReaderObject.PropertyDetails();

            using (var buffers = document.Data.GetPropertiesByInsertionOrder())
            {
                for (int i = 0; i < buffers.Properties.Count; i++)
                {
                    document.Data.GetPropertyByIndex(buffers.Properties.Array[i + buffers.Properties.Offset], ref prop);
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
                metadata = context.ReadObject(metadata, document.Id);
            }
            else
            {
                metadata = context.ReadObject(extraMetadataProperties, document.Id);
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
                    writer.WriteString(lazyString?.Substring(0,
                        Math.Min(lazyString.Length, StringLengthLimit)));
                    break;
                case BlittableJsonToken.CompressedString:
                    var lazyCompressedString = (LazyCompressedStringValue)val;
                    writer.WriteString(lazyCompressedString?.Substring(0,
                        Math.Min(lazyCompressedString.UncompressedSize, StringLengthLimit)));
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

        private static HashSet<LazyStringValue> ExtractColumnNames(Document[] documents, DocumentsOperationContext context)
        {
            var columns = new HashSet<LazyStringValue>();

            foreach (var document in documents)
            {
                FetchColumnNames(document.Data, columns);
            }

            RemoveMetadata(context, columns);

            return columns;
        }

        public static void FetchColumnNames(BlittableJsonReaderObject data, HashSet<LazyStringValue> columns)
        {
            using (var buffers = data.GetPropertiesByInsertionOrder())
            {
                var prop = new BlittableJsonReaderObject.PropertyDetails();

                for (var i = 0; i < buffers.Properties.Count; i++)
                {
                    data.GetPropertyByIndex(buffers.Properties.Array[i + buffers.Properties.Offset], ref prop);
                    var propName = prop.Name;
                    if (columns.Contains(propName) == false)
                    {
                        columns.Add(prop.Name);
                    }
                }
            }
        }

        public static void RemoveMetadata(DocumentsOperationContext context, HashSet<LazyStringValue> columns)
        {
            var metadataField = context.GetLazyStringForFieldWithCaching(Constants.Documents.Metadata.Key);
            columns.Remove(metadataField);
        }

        [RavenAction("/databases/*/studio/collections/docs", "DELETE", AuthorizationStatus.ValidUser)]
        public Task Delete()
        {
            var returnContextToPool = ContextPool.AllocateOperationContext(out DocumentsOperationContext context);

            var excludeIds = new HashSet<string>();

            var reader = context.Read(RequestBodyStream(), "ExcludeIds");
            if (reader.TryGet("ExcludeIds", out BlittableJsonReaderArray ids))
            {
                foreach (LazyStringValue id in ids)
                {
                    excludeIds.Add(id);
                }
            }

            ExecuteCollectionOperation((runner, collectionName, options, onProgress, token) => Task.Run(async () => await runner.ExecuteDelete(collectionName, options, onProgress, token)),
                context, returnContextToPool, Documents.Operations.Operations.OperationType.DeleteByCollection, excludeIds);
            return Task.CompletedTask;
        }


        private void ExecuteCollectionOperation(Func<CollectionRunner, string, CollectionOperationOptions, Action<IOperationProgress>, OperationCancelToken, Task<IOperationResult>> operation, DocumentsOperationContext context, IDisposable returnContextToPool, Documents.Operations.Operations.OperationType operationType, HashSet<string> excludeIds)
        {
            var collectionName = GetStringQueryString("name");

            var token = CreateTimeLimitedCollectionOperationToken();

            var collectionRunner = new StudioCollectionRunner(Database, context, excludeIds);

            var operationId = Database.Operations.GetNextOperationId();

            // use default options
            var options = new CollectionOperationOptions();

            var task = Database.Operations.AddOperation(Database, collectionName, operationType, onProgress =>
                     operation(collectionRunner, collectionName, options, onProgress, token), operationId, token: token);

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
