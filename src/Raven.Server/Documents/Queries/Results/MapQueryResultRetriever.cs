using System;
using System.IO;
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

        private readonly BlittableJsonTraverser _traverser;

        public MapQueryResultRetriever(DocumentsStorage documentsStorage, DocumentsOperationContext context, FieldsToFetch fieldsToFetch)
        {
            _documentsStorage = documentsStorage;
            _context = context;
            _fieldsToFetch = fieldsToFetch;
            _traverser = _fieldsToFetch.IsProjection ? new BlittableJsonTraverser() : null;
        }

        public Document Get(Lucene.Net.Documents.Document input)
        {
            var id = input.Get(Constants.DocumentIdFieldName);

            if (_fieldsToFetch.IsProjection)
                return GetProjection(input, id);

            var doc = _documentsStorage.Get(_context, id);
            if (doc == null)
                return null;

            doc.EnsureMetadata();
            return doc;
        }

        private Document GetProjection(Lucene.Net.Documents.Document input, string id)
        {
            if (_fieldsToFetch.AnyExtractableFromIndex == false)
                return GetProjectionFromDocument(id);

            Document doc = null;
            var documentLoaded = false;

            var result = new DynamicJsonValue();

            if (_fieldsToFetch.IsDistinct == false)
                result[Constants.DocumentIdFieldName] = id;

            foreach (var fieldToFetch in _fieldsToFetch.Fields.Values)
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

            return ReturnProjection(result, doc);
        }

        private Document GetProjectionFromDocument(string id)
        {
            var doc = _documentsStorage.Get(_context, id);
            if (doc == null)
                return null;

            var result = new DynamicJsonValue();

            if (_fieldsToFetch.IsDistinct == false)
                result[Constants.DocumentIdFieldName] = doc.Key;

            foreach (var fieldToFetch in _fieldsToFetch.Fields.Values)
                TryExtractValueFromDocument(fieldToFetch, doc, result);

            return ReturnProjection(result, doc);
        }

        private Document ReturnProjection(DynamicJsonValue result, Document doc)
        {
            var newData = _context.ReadObject(result, doc.Key);

            try
            {
                doc.Data.Dispose();
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

            var name = _traverser.GetNameFromPath(fieldToFetch.Name.Value);

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

        private bool TryExtractValueFromDocument(FieldsToFetch.FieldToFetch fieldToFetch, Document document, DynamicJsonValue toFill)
        {
            object value;
            if (_traverser.TryRead(document.Data, fieldToFetch.Name, out value) == false)
                return false;

            toFill[_traverser.GetNameFromPath(fieldToFetch.Name.Value)] = value;
            return true;
        }
    }
}