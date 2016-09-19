using System;
using System.Linq;
using Lucene.Net.Documents;
using Sparrow.Json.Parsing;
using Sparrow.Json;
using Raven.Abstractions.Data;
using System.Collections.Generic;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Json;
using System.IO;

namespace Raven.Server.Documents.Queries.Results
{
    public abstract class QueryResultRetrieverBase : IQueryResultRetriever
    {
        protected readonly FieldsToFetch _fieldsToFetch;
        JsonOperationContext _context;

        public QueryResultRetrieverBase(FieldsToFetch fieldsToFetch, JsonOperationContext context)
        {
            _context = context;
            _fieldsToFetch = fieldsToFetch;
        }

        public abstract Document Get(Lucene.Net.Documents.Document input, float score);

        public abstract bool TryGetKey(Lucene.Net.Documents.Document document, out string key);

        protected abstract Document DirectGet(Lucene.Net.Documents.Document input, string id);

        protected Document GetProjection(Lucene.Net.Documents.Document input, float score, string id)
        {
            Document doc = null;
            if (_fieldsToFetch.AnyExtractableFromIndex == false)
            {
                doc = DirectGet(input, id);

                if (doc == null)
                    return null;

                return GetProjectionFromDocument(doc, score, _fieldsToFetch, _context);
            }

            var documentLoaded = false;

            var result = new DynamicJsonValue();

            if (_fieldsToFetch.IsDistinct == false && string.IsNullOrEmpty(id) == false)
                result[Constants.Indexing.Fields.DocumentIdFieldName] = id;

            Dictionary<string, FieldsToFetch.FieldToFetch> fields;
            if (_fieldsToFetch.ExtractAllFromIndexAndDocument)
            {
                fields = input.GetFields()
                    .Where(x => x.Name != Constants.Indexing.Fields.DocumentIdFieldName && 
                                x.Name != Constants.Indexing.Fields.ReduceKeyFieldName &&
                                x.Name != Constants.Indexing.Fields.ReduceValueFieldName)
                    .Distinct(UniqueFieldNames.Instance)
                    .ToDictionary(x => x.Name, x => new FieldsToFetch.FieldToFetch(x.Name, x.IsStored));

                doc = DirectGet(input, id);
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
                    doc = DirectGet(input, id);
                    documentLoaded = true;
                }

                if (doc == null)
                    continue;

                MaybeExtractValueFromDocument(fieldToFetch, doc, result);
            }

            if (doc == null)
            {
                doc = new Document
                {
                    Key = _context.GetLazyString(id)
                };
            }

            return ReturnProjection(result, doc, score, _context);
        }

        public static Document GetProjectionFromDocument(Document doc, float score, FieldsToFetch fieldsToFetch, JsonOperationContext context)
        {
            var result = new DynamicJsonValue();

            if (fieldsToFetch.IsDistinct == false && doc.Key != null)
                result[Constants.Indexing.Fields.DocumentIdFieldName] = doc.Key;

            foreach (var fieldToFetch in fieldsToFetch.Fields.Values)
                MaybeExtractValueFromDocument(fieldToFetch, doc, result);

            return ReturnProjection(result, doc, score, context);
        }

        private static Document ReturnProjection(DynamicJsonValue result, Document doc, float score, JsonOperationContext context)
        {
            var newData = context.ReadObject(result, "projection result");

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
            doc.EnsureMetadata(score);

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

        private static void MaybeExtractValueFromDocument(FieldsToFetch.FieldToFetch fieldToFetch, Document document, DynamicJsonValue toFill)
        {
            object value;
            if (BlittableJsonTraverserHelper.TryRead(BlittableJsonTraverser.Default, document, fieldToFetch.Name, out value) == false)
                return;

            toFill[fieldToFetch.Name.Value] = value;
        }

        private class UniqueFieldNames : IEqualityComparer<IFieldable>
        {
            public static UniqueFieldNames Instance = new UniqueFieldNames();

            public bool Equals(IFieldable x, IFieldable y)
            {
                return x.Name.Equals(y.Name, StringComparison.OrdinalIgnoreCase);
            }

            public int GetHashCode(IFieldable obj)
            {
                return obj.Name.GetHashCode();
            }
        }
    }
}
