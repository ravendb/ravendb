using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Lucene.Net.Documents;
using Raven.Abstractions.Data;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.ServerWide.Context;

using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Queries.Results
{
    public class MapQueryResultRetriever : IQueryResultRetriever
    {
        private readonly DocumentsStorage _documentsStorage;

        private readonly DocumentsOperationContext _context;

        private readonly FieldsToFetch _fieldsToFetch;

        public MapQueryResultRetriever(DocumentsStorage documentsStorage, DocumentsOperationContext context, FieldsToFetch fieldsToFetch)
        {
            _documentsStorage = documentsStorage;
            _context = context;
            _fieldsToFetch = fieldsToFetch;
        }

        public Document Get(Lucene.Net.Documents.Document input)
        {
            string id;
            if (TryGetKey(input, out id) == false)
                throw new InvalidOperationException($"Could not extract '{Constants.DocumentIdFieldName}' from index.");

            if (_fieldsToFetch.IsProjection || _fieldsToFetch.IsTransformation)
                return GetProjection(input, id);

            return DirectGet(id);
        }

        public bool TryGetKey(Lucene.Net.Documents.Document input, out string key)
        {
            key = input.Get(Constants.DocumentIdFieldName);
            return key != null;
        }

        private Document DirectGet(string id)
        {
            var doc = _documentsStorage.Get(_context, id);
            if (doc == null)
                return null;

            doc.EnsureMetadata();
            return doc;
        }

        private Document GetProjection(Lucene.Net.Documents.Document input, string id)
        {
            Document doc = null;
            if (_fieldsToFetch.AnyExtractableFromIndex == false)
            {
                doc = DirectGet(id);
                if (doc == null)
                    return null;

                return GetProjectionFromDocument(doc, _fieldsToFetch, _context);
            }

            var documentLoaded = false;

            var result = new DynamicJsonValue();

            if (_fieldsToFetch.IsDistinct == false)
                result[Constants.DocumentIdFieldName] = id;

            Dictionary<string, FieldsToFetch.FieldToFetch> fields;
            if (_fieldsToFetch.ExtractAllFromIndexAndDocument)
            {
                fields = input.GetFields()
                    .Where(x => x.Name != Constants.DocumentIdFieldName)
                    .ToDictionary(x => x.Name, x => new FieldsToFetch.FieldToFetch(x.Name, x.IsStored));

                doc = _documentsStorage.Get(_context, id);
                documentLoaded = true;

                if (doc != null)
                {
                    foreach (var name in doc.Data.GetPropertyNames())
                    {
                        if (fields.ContainsKey(name))
                            continue;

                        fields[name] = new FieldsToFetch.FieldToFetch(name, canExtractFromIndex: false);
                    }
                }
            }
            else
            {
                fields = _fieldsToFetch.Fields;
            }

            foreach (var fieldToFetch in fields.Values)
            {
                if (TryExtractValueFromIndex(fieldToFetch, input, result))
                    continue;

                if (documentLoaded == false)
                {
                    doc = _documentsStorage.Get(_context, id);
                    documentLoaded = true;
                }

                if (doc == null)
                    continue;

                TryExtractValueFromDocument(fieldToFetch, doc, result);
            }

            if (doc == null)
            {
                doc = new Document
                {
                    Key = _context.GetLazyString(id)
                };
            }

            return ReturnProjection(result, doc, _context);
        }

        public static Document GetProjectionFromDocument(Document doc, FieldsToFetch fieldsToFetch, JsonOperationContext context)
        {
            var result = new DynamicJsonValue();

            if (fieldsToFetch.IsDistinct == false)
                result[Constants.DocumentIdFieldName] = doc.Key;

            foreach (var fieldToFetch in fieldsToFetch.Fields.Values)
                TryExtractValueFromDocument(fieldToFetch, doc, result);

            return ReturnProjection(result, doc, context);
        }

        private static Document ReturnProjection(DynamicJsonValue result, Document doc, JsonOperationContext context)
        {
            var newData = context.ReadObject(result, doc.Key);

            try
            {
                doc.Data?.Dispose();
            }
            catch (Exception)
            {
                newData.Dispose();
                throw;
            }

            doc.Data = newData;
            doc.EnsureMetadata();

            return doc;
        }

        private bool TryExtractValueFromIndex(FieldsToFetch.FieldToFetch fieldToFetch, Lucene.Net.Documents.Document indexDocument, DynamicJsonValue toFill)
        {
            if (fieldToFetch.CanExtractFromIndex == false)
                return false;

            var name = fieldToFetch.Name.Value;

            DynamicJsonArray array = null;
            FieldType fieldType = null;
            var anyExtracted = false;
            foreach (var field in indexDocument.GetFields(fieldToFetch.Name))
            {
                if (fieldType == null)
                    fieldType = GetFieldType(field, indexDocument);

                var fieldValue = ConvertType(indexDocument, field, fieldType);

                if (fieldType.IsArray)
                {
                    if (array == null)
                    {
                        array = new DynamicJsonArray();
                        toFill[name] = array;
                    }

                    array.Add(fieldValue);
                    anyExtracted = true;
                    continue;
                }

                toFill[name] = fieldValue;
                anyExtracted = true;
            }

            return anyExtracted;
        }

        private static FieldType GetFieldType(IFieldable field, Lucene.Net.Documents.Document indexDocument)
        {
            return new FieldType
            {
                IsArray = indexDocument.GetField(field.Name + LuceneDocumentConverterBase.IsArrayFieldSuffix) != null,
                IsJson = indexDocument.GetField(field.Name + LuceneDocumentConverterBase.ConvertToJsonSuffix) != null,
            };
        }

        private class FieldType
        {
            public bool IsArray;
            public bool IsJson;
        }

        private object ConvertType(Lucene.Net.Documents.Document indexDocument, IFieldable field, FieldType fieldType)
        {
            if (field.IsBinary)
                throw new NotImplementedException("Support for binary values");

            var stringValue = field.StringValue;
            if (stringValue == Constants.NullValue || stringValue == null)
                return null;
            if (stringValue == Constants.EmptyString || stringValue == string.Empty)
                return string.Empty;

            if (fieldType.IsJson == false)
                return stringValue;

            var bytes = _context.Encoding.GetBytes(stringValue);
            var ms = new MemoryStream(bytes);
            return _context.ReadForMemory(ms, field.Name);
        }

        private static bool TryExtractValueFromDocument(FieldsToFetch.FieldToFetch fieldToFetch, Document document, DynamicJsonValue toFill)
        {
            object value;
            if (BlittableJsonTraverser.Default.TryRead(document.Data, fieldToFetch.Name, out value) == false)
                return false;

            toFill[fieldToFetch.Name.Value] = value;
            return true;
        }
    }
}