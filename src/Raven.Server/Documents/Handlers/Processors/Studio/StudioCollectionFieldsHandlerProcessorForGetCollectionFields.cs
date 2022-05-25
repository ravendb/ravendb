using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Studio
{
    internal class StudioCollectionFieldsHandlerProcessorForGetCollectionFields : AbstractStudioCollectionFieldsHandlerProcessorForGetCollectionFields<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public StudioCollectionFieldsHandlerProcessorForGetCollectionFields([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override ValueTask<Dictionary<LazyStringValue, FieldType>> GetFieldsAsync(DocumentsOperationContext context, string collection, string prefix)
        {
            using (context.OpenReadTransaction())
            {
                long totalResults;
                string changeVector;
                string etag = null;

                if (string.IsNullOrEmpty(collection))
                {
                    changeVector = DocumentsStorage.GetDatabaseChangeVector(context);
                    totalResults = RequestHandler.Database.DocumentsStorage.GetNumberOfDocuments(context);
                    etag = $"{changeVector}/{totalResults}";
                }
                else
                {
                    changeVector = RequestHandler.Database.DocumentsStorage.GetLastDocumentChangeVector(context.Transaction.InnerTransaction, context, collection);
                    totalResults = RequestHandler.Database.DocumentsStorage.GetCollection(collection, context).Count;

                    if (changeVector != null)
                        etag = $"{changeVector}/{totalResults}";
                }

                if (etag != null && RequestHandler.GetStringFromHeaders(Constants.Headers.IfNoneMatch) == etag)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                    return ValueTask.FromResult<Dictionary<LazyStringValue, FieldType>>(null);
                }

                HttpContext.Response.Headers["ETag"] = "\"" + etag + "\"";

                var fields = new Dictionary<LazyStringValue, FieldType>(LazyStringValueComparer.Instance);

                if (string.IsNullOrEmpty(collection))
                {
                    foreach (var collectionStats in RequestHandler.Database.DocumentsStorage.GetCollections(context))
                    {
                        FetchFieldsForCollection(context, collectionStats.Name, prefix, fields);
                    }
                }
                else
                {
                    FetchFieldsForCollection(context, collection, prefix, fields);
                }

                return ValueTask.FromResult(fields);
            }
        }

        private void FetchFieldsForCollection(DocumentsOperationContext context, string collection, string prefix,
            Dictionary<LazyStringValue, FieldType> fields)
        {
            var document = RequestHandler.Database.DocumentsStorage.GetDocumentsInReverseEtagOrder(context, collection, 0, 1).FirstOrDefault();
            if (document != null)
            {
                var data = document.Data;
                if (string.IsNullOrEmpty(prefix))
                {
                    FetchFields(data, fields);
                }
                else
                {
                    var prefixFields = prefix.Split('.', StringSplitOptions.RemoveEmptyEntries);
                    for (var i = 0; i < prefixFields.Length; i++)
                    {
                        var prefixField = prefixFields[i];
                        var index = data.GetPropertyIndex(prefixField);
                        if (index < 0)
                            break;

                        var prop = new BlittableJsonReaderObject.PropertyDetails();
                        data.GetPropertyByIndex(index, ref prop);
                        var token = prop.Token & BlittableJsonReaderBase.TypesMask;

                        if (i + 1 == prefixFields.Length)
                        {
                            switch (token)
                            {
                                case BlittableJsonToken.StartObject:
                                    FetchFields((BlittableJsonReaderObject)prop.Value, fields);
                                    break;

                                case BlittableJsonToken.StartArray:
                                    var array = (BlittableJsonReaderArray)prop.Value;
                                    for (int j = 0; j < Math.Min(array.Length, MaxArrayItemsToFetch); j++)
                                    {
                                        var item = array[i];
                                        if (item is BlittableJsonReaderObject itemObject)
                                            FetchFields(itemObject, fields);
                                    }
                                    break;
                            }
                        }
                        else
                        {
                            if (token != BlittableJsonToken.StartObject)
                                break;

                            data = (BlittableJsonReaderObject)prop.Value;
                        }
                    }
                }
            }
        }

        private unsafe void FetchFields(BlittableJsonReaderObject data, Dictionary<LazyStringValue, FieldType> fields)
        {
            var prop = new BlittableJsonReaderObject.PropertyDetails();
            using (var buffers = data.GetPropertiesByInsertionOrder())
            {
                for (var i = 0; i < buffers.Size; i++)
                {
                    data.GetPropertyByIndex(buffers.Properties[i], ref prop);
                    var type = GetFieldType(prop.Token & BlittableJsonReaderBase.TypesMask, prop.Value);
                    if (fields.TryGetValue(prop.Name, out var token))
                    {
                        if (token != type)
                        {
                            fields[prop.Name] = token | type;
                        }
                    }
                    else
                    {
                        fields[prop.Name] = type;
                    }
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private FieldType GetFieldType(BlittableJsonToken token, object value)
        {
            switch (token)
            {
                case BlittableJsonToken.String:
                case BlittableJsonToken.CompressedString:
                    return FieldType.String;

                case BlittableJsonToken.Integer:
                case BlittableJsonToken.LazyNumber:
                    return FieldType.Number;

                case BlittableJsonToken.StartArray:
                    if (value == null)
                        return FieldType.Array;

                    var array = (BlittableJsonReaderArray)value;
                    if (array.Length == 0)
                        return FieldType.Array;

                    var arrayType = GetFieldType(array.GetArrayType(), null);
                    switch (arrayType)
                    {
                        case FieldType.Object:
                            return FieldType.ArrayObject;

                        case FieldType.Array:
                            return FieldType.ArrayArray;

                        case FieldType.String:
                            return FieldType.ArrayString;

                        case FieldType.Number:
                            return FieldType.ArrayNumber;

                        case FieldType.Boolean:
                            return FieldType.ArrayBoolean;
                    }

                    return FieldType.Array;

                case BlittableJsonToken.EmbeddedBlittable:
                case BlittableJsonToken.StartObject:
                    return FieldType.Object;

                case BlittableJsonToken.Boolean:
                    return FieldType.Boolean;

                case BlittableJsonToken.Null:
                    return FieldType.Null;

                default:
                    throw new DataMisalignedException($"Unidentified Type {token}");
            }
        }
    }
}
