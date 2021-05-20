using System;
using System.Collections.Generic;
using System.Linq;
using Jint.Native;
using Jint.Native.Object;
using Jint.Runtime;
using Lucene.Net.Documents;
using Lucene.Net.Store;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Results.TimeSeries;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Json.Sync;

namespace Raven.Server.Documents.Queries.Results
{
    public abstract class QueryResultRetrieverBase : IQueryResultRetriever
    {
        public static readonly Lucene.Net.Search.ScoreDoc ZeroScore = new Lucene.Net.Search.ScoreDoc(-1, 0f);

        public static readonly Lucene.Net.Search.ScoreDoc OneScore = new Lucene.Net.Search.ScoreDoc(-1, 1f);

        private readonly DocumentDatabase _database;
        protected readonly IndexQueryServerSide _query;
        private readonly JsonOperationContext _context;
        private readonly IncludeDocumentsCommand _includeDocumentsCommand;
        private readonly IncludeCompareExchangeValuesCommand _includeCompareExchangeValuesCommand;
        private readonly BlittableJsonTraverser _blittableTraverser;
        private Dictionary<string, Document> _loadedDocuments;
        private Dictionary<string, Document> _loadedDocumentsByAliasName;
        private HashSet<string> _loadedDocumentIds;

        protected readonly DocumentFields DocumentFields;

        protected readonly DocumentsStorage DocumentsStorage;

        protected readonly FieldsToFetch FieldsToFetch;

        protected readonly QueryTimingsScope RetrieverScope;

        private QueryTimingsScope _projectionScope;
        private QueryTimingsScope _projectionStorageScope;
        private QueryTimingsScope _functionScope;
        private QueryTimingsScope _loadScope;

        private TimeSeriesRetriever _timeSeriesRetriever;

        protected QueryResultRetrieverBase(DocumentDatabase database, IndexQueryServerSide query, QueryTimingsScope queryTimings, FieldsToFetch fieldsToFetch, DocumentsStorage documentsStorage, JsonOperationContext context, bool reduceResults, IncludeDocumentsCommand includeDocumentsCommand, IncludeCompareExchangeValuesCommand includeCompareExchangeValuesCommand)
        {
            _database = database;
            _query = query;
            _context = context;
            _includeDocumentsCommand = includeDocumentsCommand;
            _includeCompareExchangeValuesCommand = includeCompareExchangeValuesCommand;

            ValidateFieldsToFetch(fieldsToFetch);
            FieldsToFetch = fieldsToFetch;

            DocumentsStorage = documentsStorage;

            RetrieverScope = queryTimings?.For(nameof(QueryTimingsScope.Names.Retriever), start: false);

            DocumentFields = query?.DocumentFields ?? DocumentFields.All;

            _blittableTraverser = reduceResults ? BlittableJsonTraverser.FlatMapReduceResults : BlittableJsonTraverser.Default;
        }

        protected virtual void ValidateFieldsToFetch(FieldsToFetch fieldsToFetch)
        {
            if (fieldsToFetch == null)
                throw new ArgumentNullException(nameof(fieldsToFetch));
        }

        protected void FinishDocumentSetup(Document doc, Lucene.Net.Search.ScoreDoc scoreDoc)
        {
            if (doc == null || scoreDoc == null)
                return;

            doc.IndexScore = scoreDoc.Score;
            if (_query?.Distances != null)
            {
                doc.Distance = _query.Distances.Get(scoreDoc.Doc);
            }
        }

        public abstract Document Get(Lucene.Net.Documents.Document input, Lucene.Net.Search.ScoreDoc scoreDoc, IState state);

        public abstract bool TryGetKey(Lucene.Net.Documents.Document document, IState state, out string key);

        protected abstract Document DirectGet(Lucene.Net.Documents.Document input, string id, DocumentFields fields, IState state);

        protected abstract Document LoadDocument(string id);

        protected abstract long? GetCounter(string docId, string name);

        protected abstract DynamicJsonValue GetCounterRaw(string docId, string name);

        protected Document GetProjection(Lucene.Net.Documents.Document input, Lucene.Net.Search.ScoreDoc scoreDoc, string lowerId, IState state)
        {
            using (_projectionScope = _projectionScope?.Start() ?? RetrieverScope?.For(nameof(QueryTimingsScope.Names.Projection)))
            {
                Document doc = null;
                if (FieldsToFetch.AnyExtractableFromIndex == false)
                {
                    using (_projectionStorageScope = _projectionStorageScope?.Start() ?? _projectionScope?.For(nameof(QueryTimingsScope.Names.Storage)))
                        doc = DirectGet(input, lowerId, DocumentFields.All, state);

                    if (doc == null)
                    {
                        if (FieldsToFetch.Projection.MustExtractFromDocument)
                        {
                            if (FieldsToFetch.Projection.MustExtractOrThrow)
                                FieldsToFetch.Projection.ThrowCouldNotExtractProjectionOnDocumentBecauseDocumentDoesNotExistException(lowerId);
                        }

                        return null;
                    }

                    return GetProjectionFromDocumentInternal(doc, input, scoreDoc, FieldsToFetch, _context, state);
                }

                var documentLoaded = false;

                var result = new DynamicJsonValue();

                Dictionary<string, FieldsToFetch.FieldToFetch> fields = null;
                if (FieldsToFetch.ExtractAllFromIndex)
                {
                    fields = input.GetFields()
                        .Where(x => x.Name != Constants.Documents.Indexing.Fields.DocumentIdFieldName
                                    && x.Name != Constants.Documents.Indexing.Fields.SourceDocumentIdFieldName
                                    && x.Name != Constants.Documents.Indexing.Fields.ReduceKeyHashFieldName
                                    && x.Name != Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName
                                    && x.Name != Constants.Documents.Indexing.Fields.ValueFieldName
                                    && FieldUtil.GetRangeTypeFromFieldName(x.Name) == RangeType.None)
                        .Distinct(UniqueFieldNames.Instance)
                        .ToDictionary(x => x.Name, x => new FieldsToFetch.FieldToFetch(x.Name, null, null, x.IsStored, isDocumentId: false, isTimeSeries: false));
                }

                if (fields == null)
                    fields = FieldsToFetch.Fields;
                else if (FieldsToFetch.Fields != null && FieldsToFetch.Fields.Count > 0)
                {
                    foreach (var kvp in FieldsToFetch.Fields)
                    {
                        if (fields.ContainsKey(kvp.Key))
                            continue;

                        fields[kvp.Key] = kvp.Value;
                    }
                }

                foreach (var fieldToFetch in fields.Values)
                {
                    if (TryExtractValueFromIndex(fieldToFetch, input, result, state))
                        continue;

                    if (FieldsToFetch.Projection.MustExtractFromIndex)
                    {
                        if (FieldsToFetch.Projection.MustExtractOrThrow)
                            FieldsToFetch.Projection.ThrowCouldNotExtractFieldFromIndexBecauseIndexDoesNotContainSuchFieldOrFieldValueIsNotStored(fieldToFetch.Name.Value);

                        continue;
                    }

                    if (documentLoaded == false)
                    {
                        using (_projectionStorageScope = _projectionStorageScope?.Start() ?? _projectionScope?.For(nameof(QueryTimingsScope.Names.Storage)))
                            doc = DirectGet(input, lowerId, DocumentFields.All, state);

                        documentLoaded = true;
                    }

                    if (doc == null)
                    {
                        if (FieldsToFetch.Projection.MustExtractFromDocument)
                        {
                            if (FieldsToFetch.Projection.MustExtractOrThrow)
                                FieldsToFetch.Projection.ThrowCouldNotExtractFieldFromDocumentBecauseDocumentDoesNotExistException(lowerId, fieldToFetch.Name.Value);

                            break;
                        }

                        continue;
                    }

                    if (TryGetValue(fieldToFetch, doc, input, state, FieldsToFetch.IndexFields, FieldsToFetch.AnyDynamicIndexFields, out var key, out var fieldVal))
                    {
                        if (FieldsToFetch.SingleBodyOrMethodWithNoAlias)
                        {
                            if (fieldVal is BlittableJsonReaderObject nested)
                                doc.Data = nested;
                            else if (fieldVal is Document d)
                                doc = d;
                            else
                                ThrowInvalidQueryBodyResponse(fieldVal);
                            FinishDocumentSetup(doc, scoreDoc);
                            return doc;
                        }

                        if (fieldVal is List<object> list)
                            fieldVal = new DynamicJsonArray(list);

                        if (fieldVal is Document d2)
                            fieldVal = d2.Data;

                        result[key] = fieldVal;
                    }
                    else
                    {
                        if (FieldsToFetch.Projection.MustExtractFromDocument)
                        {
                            if (FieldsToFetch.Projection.MustExtractOrThrow)
                                FieldsToFetch.Projection.ThrowCouldNotExtractFieldFromDocumentBecauseDocumentDoesNotContainSuchField(lowerId, fieldToFetch.Name.Value);
                        }
                    }
                }

                if (doc == null)
                {
                    doc = new Document
                    {
                        Id = _context.GetLazyString(lowerId)
                    };
                }

                return ReturnProjection(result, doc, scoreDoc, _context);
            }
        }

        public Document GetProjectionFromDocument(Document doc, Lucene.Net.Documents.Document luceneDoc, Lucene.Net.Search.ScoreDoc scoreDoc, FieldsToFetch fieldsToFetch, JsonOperationContext context, IState state)
        {
            using (RetrieverScope?.Start())
            using (_projectionScope = _projectionScope?.Start() ?? RetrieverScope?.For(nameof(QueryTimingsScope.Names.Projection)))
            {
                return GetProjectionFromDocumentInternal(doc, luceneDoc, scoreDoc, fieldsToFetch, context, state);
            }
        }

        private Document GetProjectionFromDocumentInternal(Document doc, Lucene.Net.Documents.Document luceneDoc, Lucene.Net.Search.ScoreDoc scoreDoc, FieldsToFetch fieldsToFetch, JsonOperationContext context, IState state)
        {
            using (doc)
            {
                var result = new DynamicJsonValue();

                foreach (var fieldToFetch in fieldsToFetch.Fields.Values)
                {
                    if (TryGetValue(fieldToFetch, doc, luceneDoc, state, fieldsToFetch.IndexFields, fieldsToFetch.AnyDynamicIndexFields, out var key, out var fieldVal) == false)
                    {
                        if (FieldsToFetch.Projection.MustExtractFromDocument)
                        {
                            if (FieldsToFetch.Projection.MustExtractOrThrow)
                                throw new InvalidQueryException($"Could not extract field '{fieldToFetch.Name.Value}' from document, because document does not contain such a field.");
                        }

                        if (fieldToFetch.QueryField != null && fieldToFetch.QueryField.HasSourceAlias)
                            continue;
                    }

                    var immediateResult = AddProjectionToResult(doc, scoreDoc, fieldsToFetch, result, key, fieldVal);

                    if (immediateResult != null)
                        return immediateResult;
                }

                return ReturnProjection(result, doc.Clone(context), scoreDoc, context);
            }
        }

        protected Document AddProjectionToResult(Document doc, Lucene.Net.Search.ScoreDoc scoreDoc, FieldsToFetch fieldsToFetch,
            DynamicJsonValue result, string key, object fieldVal)
        {
            if (_query.IsStream &&
                key.StartsWith(Constants.TimeSeries.QueryFunction))
            {
                doc.TimeSeriesStream ??= new TimeSeriesStream();
                var value = (TimeSeriesRetriever.TimeSeriesRetrieverResult)fieldVal;
                doc.TimeSeriesStream.TimeSeries = value.Stream;
                doc.TimeSeriesStream.Key = key;
                Json.BlittableJsonTextWriterExtensions.MergeMetadata(result, value.Metadata);
                return null;
            }

            if (fieldsToFetch.SingleBodyOrMethodWithNoAlias)
            {
                var newDoc = CreateNewDocument(doc, key, fieldVal);
                FinishDocumentSetup(newDoc, scoreDoc);
                return newDoc;
            }

            AddProjectionToResult(result, key, fieldVal);
            return null;
        }

        private Document CreateNewDocument(Document doc, string key, object fieldVal)
        {
            switch (fieldVal)
            {
                case BlittableJsonReaderObject nested:
                    return new Document
                    {
                        Id = _context.GetLazyString(doc.Id),
                        ChangeVector = doc.ChangeVector,
                        Data = nested,
                        Etag = doc.Etag,
                        Flags = doc.Flags,
                        LastModified = doc.LastModified,
                        LowerId = _context.GetLazyString(doc.LowerId),
                        NonPersistentFlags = doc.NonPersistentFlags,
                        StorageId = doc.StorageId,
                        TransactionMarker = doc.TransactionMarker
                    };

                case Document d:
                    return d.Clone(_context);

                case TimeSeriesRetriever.TimeSeriesRetrieverResult ts:
                    return new Document
                    {
                        Id = _context.GetLazyString(doc.Id),
                        ChangeVector = doc.ChangeVector,
                        Data = _context.ReadObject(ts.Metadata, "time-series-metadata"),
                        Etag = doc.Etag,
                        Flags = doc.Flags,
                        LastModified = doc.LastModified,
                        LowerId = _context.GetLazyString(doc.LowerId),
                        NonPersistentFlags = doc.NonPersistentFlags,
                        StorageId = doc.StorageId,
                        TransactionMarker = doc.TransactionMarker,
                        TimeSeriesStream = new TimeSeriesStream { TimeSeries = ts.Stream, Key = key }
                    };
                default:
                    ThrowInvalidQueryBodyResponse(fieldVal);
                    break;
            }

            return null;
        }

        protected static void AddProjectionToResult(DynamicJsonValue result, string key, object fieldVal)
        {
            if (fieldVal is List<object> list)
            {
                var array = new DynamicJsonArray();
                for (int i = 0; i < list.Count; i++)
                {
                    if (list[i] is Document d)
                        array.Add(d.Data);
                    else
                        array.Add(list[i]);
                }
                fieldVal = array;
            }
            if (fieldVal is Document d2)
                fieldVal = d2.Data;

            result[key] = fieldVal;
        }

        private static void ThrowInvalidQueryBodyResponse(object fieldVal)
        {
            throw new InvalidOperationException("Query returning a single function call result must return an object, but got: " + (fieldVal ?? "null"));
        }

        protected Document ReturnProjection(DynamicJsonValue result, Document doc, Lucene.Net.Search.ScoreDoc scoreDoc, JsonOperationContext context)
        {
            var metadata = Json.BlittableJsonTextWriterExtensions.GetOrCreateMetadata(result);
            metadata[Constants.Documents.Metadata.Projection] = true;

            var newData = context.ReadObject(result, "projection result");

            try
            {
                if (ReferenceEquals(newData, doc.Data) == false)
                    doc.Data?.Dispose();
            }
            catch (Exception)
            {
                newData.Dispose();
                throw;
            }

            doc.Data = newData;
            FinishDocumentSetup(doc, scoreDoc);

            return doc;
        }

        private bool TryExtractValueFromIndex(FieldsToFetch.FieldToFetch fieldToFetch, Lucene.Net.Documents.Document indexDocument, DynamicJsonValue toFill, IState state)
        {
            if (fieldToFetch.CanExtractFromIndex == false)
                return false;

            var name = fieldToFetch.ProjectedName ?? fieldToFetch.Name.Value;

            DynamicJsonArray array = null;
            FieldType fieldType = null;
            var anyExtracted = false;
            foreach (var field in indexDocument.GetFields(fieldToFetch.Name.Value))
            {
                if (fieldType == null)
                    fieldType = GetFieldType(field.Name, indexDocument);

                var fieldValue = ConvertType(_context, field, fieldType, state);

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

        internal static FieldType GetFieldType(string field, Lucene.Net.Documents.Document indexDocument)
        {
            var isArray = false;
            var isJson = false;
            var isNumeric = false;

            var arrayFieldName = field + LuceneDocumentConverterBase.IsArrayFieldSuffix;
            var jsonConvertFieldName = field + LuceneDocumentConverterBase.ConvertToJsonSuffix;
            var numericFieldName = field + Constants.Documents.Indexing.Fields.RangeFieldSuffixDouble;

            foreach (var f in indexDocument.GetFields())
            {
                if (f.Name == arrayFieldName)
                {
                    isArray = true;
                    continue;
                }

                if (f.Name == jsonConvertFieldName)
                {
                    isJson = true;
                    break;
                }

                if (f.Name == numericFieldName)
                {
                    isNumeric = true;
                }
            }

            return new FieldType
            {
                IsArray = isArray,
                IsJson = isJson,
                IsNumeric = isNumeric
            };
        }

        internal class FieldType
        {
            public bool IsArray;
            public bool IsJson;
            public bool IsNumeric;
        }

        private static object ConvertType(JsonOperationContext context, IFieldable field, FieldType fieldType, IState state)
        {
            if (field.IsBinary)
                ThrowBinaryValuesNotSupported();

            var stringValue = field.StringValue(state);

            if (stringValue == null)
                return null;

            if (stringValue == string.Empty)
                return string.Empty;

            if (field.IsTokenized == false)
            {
                // NULL_VALUE and EMPTY_STRING fields aren't tokenized
                // this will prevent converting fields with a "NULL_VALUE" string to null
                switch (stringValue)
                {
                    case Constants.Documents.Indexing.Fields.NullValue:
                        return null;

                    case Constants.Documents.Indexing.Fields.EmptyString:
                        return string.Empty;
                }
            }

            if (fieldType.IsJson == false)
                return stringValue;

            return context.Sync.ReadForMemory(stringValue, field.Name);
        }

        private static void ThrowBinaryValuesNotSupported()
        {
            throw new NotSupportedException("Cannot convert binary values");
        }

        protected bool TryGetValue(FieldsToFetch.FieldToFetch fieldToFetch, Document document, Lucene.Net.Documents.Document luceneDoc, IState state, Dictionary<string, IndexField> indexFields, bool? anyDynamicIndexFields, out string key, out object value)
        {
            key = fieldToFetch.ProjectedName ?? fieldToFetch.Name.Value;

            if (fieldToFetch.QueryField == null)
            {
                return TryGetFieldValueFromDocument(document, fieldToFetch, out value);
            }

            if (fieldToFetch.QueryField.Function != null)
            {
                var args = new object[fieldToFetch.QueryField.FunctionArgs.Length + 1];
                for (int i = 0; i < fieldToFetch.FunctionArgs.Length; i++)
                {
                    TryGetValue(fieldToFetch.FunctionArgs[i], document, luceneDoc, state, indexFields, anyDynamicIndexFields, out _, out args[i]);
                    if (ReferenceEquals(args[i], document))
                    {
                        args[i] = Tuple.Create(document, luceneDoc, state, indexFields, anyDynamicIndexFields, FieldsToFetch.Projection);
                    }
                }
                value = GetFunctionValue(fieldToFetch, document.Id, args);
                return true;
            }

            if (fieldToFetch.QueryField.IsCounter)
            {
                string name;
                string id = document.Id;
                if (fieldToFetch.QueryField.IsParameter)
                {
                    if (_query.QueryParameters == null)
                        throw new InvalidQueryException("The query is parametrized but the actual values of parameters were not provided", _query.Query, null);

                    if (_query.QueryParameters.TryGetMember(fieldToFetch.QueryField.Name, out var nameObject) == false)
                        throw new InvalidQueryException($"Value of parameter '{fieldToFetch.QueryField.Name}' was not provided", _query.Query, _query.QueryParameters);

                    name = nameObject.ToString();
                    key = fieldToFetch.QueryField.Alias ?? name;
                }
                else
                {
                    name = fieldToFetch.Name.Value;
                }

                if (fieldToFetch.QueryField.SourceAlias != null
                    && BlittableJsonTraverser.Default.TryRead(document.Data, fieldToFetch.QueryField.SourceAlias, out var sourceId, out _))
                {
                    id = sourceId.ToString();
                }

                if (fieldToFetch.QueryField.FunctionArgs != null)
                {
                    value = GetCounterRaw(id, name);
                }
                else
                {
                    value = GetCounter(id, name);
                }

                return true;
            }

            if (fieldToFetch.QueryField.ValueTokenType != null)
            {
                var val = fieldToFetch.QueryField.Value;
                if (fieldToFetch.QueryField.ValueTokenType.Value == ValueTokenType.Parameter)
                {
                    if (_query == null)
                    {
                        value = null;
                        return false; // only happens for debug endpoints and more like this
                    }
                    _query.QueryParameters.TryGet((string)val, out val);
                }
                value = val;
                return true;
            }

            if (fieldToFetch.QueryField.HasSourceAlias == false)
            {
                return TryGetFieldValueFromDocument(document, fieldToFetch, out value);
            }
            if (_loadedDocumentIds == null)
            {
                _loadedDocumentIds = new HashSet<string>();
                _loadedDocuments = new Dictionary<string, Document>();
                _loadedDocumentsByAliasName = new Dictionary<string, Document>();
            }
            _loadedDocumentIds.Clear();

            //_loadedDocuments.Clear(); - explicitly not clearing this, we want to cache this for the duration of the query

            _loadedDocuments[document.Id ?? string.Empty] = document;
            if (fieldToFetch.QueryField.SourceAlias != null)
            {
                if (fieldToFetch.QueryField.IsQuoted)
                {
                    _loadedDocumentIds.Add(fieldToFetch.QueryField.SourceAlias);
                }
                else if (fieldToFetch.QueryField.IsParameter)
                {
                    if (_query.QueryParameters == null)
                        throw new InvalidQueryException("The query is parametrized but the actual values of parameters were not provided", _query.Query, (BlittableJsonReaderObject)null);

                    if (_query.QueryParameters.TryGetMember(fieldToFetch.QueryField.SourceAlias, out var id) == false)
                        throw new InvalidQueryException($"Value of parameter '{fieldToFetch.QueryField.SourceAlias}' was not provided", _query.Query, _query.QueryParameters);

                    _loadedDocumentIds.Add(id.ToString());
                }
                else if (fieldToFetch.QueryField.LoadFromAlias != null)
                {
                    if (_loadedDocumentsByAliasName.TryGetValue(fieldToFetch.QueryField.LoadFromAlias, out var loadedDoc))
                    {
                        IncludeUtil.GetDocIdFromInclude(loadedDoc.Data, fieldToFetch.QueryField.SourceAlias, _loadedDocumentIds, _database.IdentityPartsSeparator);
                    }
                }
                else if (fieldToFetch.CanExtractFromIndex)
                {
                    if (luceneDoc != null)
                    {
                        var field = luceneDoc.GetField(fieldToFetch.QueryField.SourceAlias);
                        if (field != null)
                        {
                            var fieldValue = ConvertType(_context, field, GetFieldType(field.Name, luceneDoc), state);
                            _loadedDocumentIds.Add(fieldValue.ToString());
                        }
                    }
                }
                else
                {
                    IncludeUtil.GetDocIdFromInclude(document.Data, fieldToFetch.QueryField.SourceAlias, _loadedDocumentIds, _database.IdentityPartsSeparator);
                }
            }
            else
            {
                _loadedDocumentIds.Add(document.Id ?? string.Empty); // null source alias is the root doc
                _loadedDocumentsByAliasName.Clear();
            }

            if (_loadedDocumentIds.Count == 0)
            {
                if (fieldToFetch.QueryField.SourceIsArray)
                {
                    value = new List<object>();
                    return true;
                }
                value = null;
                return false;
            }

            var buffer = new List<object>();

            foreach (var docId in _loadedDocumentIds)
            {
                if (docId == null)
                    continue;

                if (_loadedDocuments.TryGetValue(docId, out var doc) == false)
                {
                    using (_loadScope = _loadScope?.Start() ?? _projectionScope?.For(nameof(QueryTimingsScope.Names.Load)))
                    {
                        _loadedDocuments[docId] = doc = LoadDocument(docId);
                        if (doc != null)
                            doc.IgnoreDispose = true;
                    }
                }
                if (doc == null)
                    continue;

                if (fieldToFetch.QueryField.Alias != null)
                {
                    _loadedDocumentsByAliasName[fieldToFetch.QueryField.Alias] = doc;
                }

                if (string.IsNullOrEmpty(fieldToFetch.Name.Value)) // we need the whole document here
                {
                    buffer.Add(doc);
                    continue;
                }
                if (TryGetFieldValueFromDocument(doc, fieldToFetch, out var val))
                {
                    if (val is string == false && val is LazyStringValue == false && val is System.Collections.IEnumerable items)
                    {
                        // we flatten arrays in projections
                        foreach (var item in items)
                        {
                            buffer.Add(item);
                        }

                        fieldToFetch.QueryField.SourceIsArray = true;
                    }
                    else
                    {
                        buffer.Add(val);
                    }
                }
            }

            if (fieldToFetch.QueryField.SourceIsArray)
            {
                value = buffer;
                return true;
            }
            if (buffer.Count > 0)
            {
                if (buffer.Count > 1)
                {
                    ThrowOnlyArrayFieldCanHaveMultipleValues(fieldToFetch);
                }
                value = buffer[0];
                return true;
            }
            value = null;
            return false;
        }

        protected object GetFunctionValue(FieldsToFetch.FieldToFetch fieldToFetch, string documentId, object[] args)
        {
            using (_functionScope = _functionScope?.Start() ?? _projectionScope?.For(nameof(QueryTimingsScope.Names.JavaScript)))
            {
                args[args.Length - 1] = _query.QueryParameters;
                var value = InvokeFunction(
                    fieldToFetch.QueryField.Name,
                    _query.Metadata.Query,
                    documentId,
                    args,
                    _functionScope);

                return value;
            }
        }

        private class QueryKey : ScriptRunnerCache.Key
        {
            private readonly Dictionary<string, DeclaredFunction> _functions;

            private bool Equals(QueryKey other)
            {
                if (_functions?.Count != other._functions?.Count)
                    return false;

                foreach (var function in _functions ?? Enumerable.Empty<KeyValuePair<string, DeclaredFunction>>())
                {
                    if (other._functions != null &&
                        (other._functions.TryGetValue(function.Key, out var otherVal) == false ||
                         function.Value.FunctionText != otherVal.FunctionText))
                        return false;
                }

                return true;
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj))
                    return false;
                if (ReferenceEquals(this, obj))
                    return true;
                if (obj.GetType() != GetType())
                    return false;
                return Equals((QueryKey)obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    int hashCode = 0;
                    foreach (var function in _functions ?? Enumerable.Empty<KeyValuePair<string, DeclaredFunction>>())
                    {
                        hashCode = (hashCode * 397) ^ (function.Value.FunctionText.GetHashCode());
                    }

                    return hashCode;
                }
            }

            public QueryKey(Dictionary<string, DeclaredFunction> functions)
            {
                _functions = functions;
            }

            public override void GenerateScript(ScriptRunner runner)
            {
                foreach (var kvp in _functions ?? Enumerable.Empty<KeyValuePair<string, DeclaredFunction>>())
                {
                    if (kvp.Value.Type == DeclaredFunction.FunctionType.TimeSeries)
                    {
                        runner.AddTimeSeriesDeclaration(kvp.Value);
                        continue;
                    }

                    if (kvp.Value.Type != DeclaredFunction.FunctionType.JavaScript)
                        continue;

                    runner.AddScript(kvp.Value.FunctionText);
                }
            }
        }

        private object InvokeFunction(string methodName, Query query, string documentId, object[] args, QueryTimingsScope timings)
        {
            if (TryGetTimeSeriesFunction(methodName, query, out var func))
            {
                _timeSeriesRetriever ??= new TimeSeriesRetriever(_includeDocumentsCommand.Context, _query.QueryParameters, _loadedDocuments);
                var result = _timeSeriesRetriever.InvokeTimeSeriesFunction(func, documentId, args, out var type);
                if (_query.IsStream)
                    return _timeSeriesRetriever.PrepareForStreaming(result, FieldsToFetch.SingleBodyOrMethodWithNoAlias, _query.AddTimeSeriesNames);
                return _timeSeriesRetriever.MaterializeResults(result, type, FieldsToFetch.SingleBodyOrMethodWithNoAlias, _query.AddTimeSeriesNames);
            }

            var key = new QueryKey(query.DeclaredFunctions);
            using (_database.Scripts.GetScriptRunner(key, readOnly: true, patchRun: out var run))
            using (var result = run.Run(_context, _context as DocumentsOperationContext, methodName, args, timings))
            {
                _includeDocumentsCommand?.AddRange(run.Includes, documentId);
                _includeCompareExchangeValuesCommand?.AddRange(run.CompareExchangeValueIncludes);

                if (result.IsNull)
                    return null;

                return run.Translate(result, _context, QueryResultModifier.Instance);
            }
        }

        private static bool TryGetTimeSeriesFunction(string methodName, Query query, out DeclaredFunction func)
        {
            func = default;

            return query.DeclaredFunctions != null &&
                   query.DeclaredFunctions.TryGetValue(methodName, out func) &&
                   func.Type == DeclaredFunction.FunctionType.TimeSeries;
        }

        private bool TryGetFieldValueFromDocument(Document document, FieldsToFetch.FieldToFetch field, out object value)
        {
            if (field.IsDocumentId)
            {
                value = GetIdFromDocument(document);
            }
            else if (field.IsCompositeField == false)
            {
                if (BlittableJsonTraverserHelper.TryRead(_blittableTraverser, document, field.Name, out value) == false)
                {
                    if (field.ProjectedName == null)
                        return false;
                    if (BlittableJsonTraverserHelper.TryRead(_blittableTraverser, document, field.ProjectedName, out value) == false)
                        return false;
                }
            }
            else
            {
                var component = new DynamicJsonValue();

                foreach (var componentField in field.Components)
                {
                    if (BlittableJsonTraverserHelper.TryRead(_blittableTraverser, document, componentField, out var componentValue))
                        component[componentField] = componentValue;
                }

                value = component;
            }
            return true;
        }

        private static string GetIdFromDocument(Document document)
        {
            if (document.Id != null)
            {
                return document.Id;
            }

            if (document.Data == null)
            {
                return null;
            }

            if (document.Data.TryGet(Constants.Documents.Metadata.IdProperty, out string docId))
            {
                return docId;
            }

            if (document.TryGetMetadata(out var md) &&
                (md.TryGet(Constants.Documents.Metadata.Id, out docId) ||
                 md.TryGet(Constants.Documents.Metadata.IdProperty, out docId)))
            {
                return docId;
            }

            return null;
        }

        private static void ThrowOnlyArrayFieldCanHaveMultipleValues(FieldsToFetch.FieldToFetch fieldToFetch)
        {
            throw new NotSupportedException(
                $"Attempted to read multiple values in field {fieldToFetch.ProjectedName ?? fieldToFetch.Name.Value}, but it isn't an array and should have only a single value, did you forget '[]' ?");
        }

        private class UniqueFieldNames : IEqualityComparer<IFieldable>
        {
            public static readonly UniqueFieldNames Instance = new UniqueFieldNames();

            public bool Equals(IFieldable x, IFieldable y)
            {
                return x.Name.Equals(y.Name, StringComparison.OrdinalIgnoreCase);
            }

            public int GetHashCode(IFieldable obj)
            {
                return obj.Name.GetHashCode();
            }
        }

        private class QueryResultModifier : JsBlittableBridge.IResultModifier
        {
            public static readonly QueryResultModifier Instance = new QueryResultModifier();

            private QueryResultModifier()
            {
            }

            public void Modify(ObjectInstance json)
            {
                ObjectInstance metadata;
                var value = json.Get(Constants.Documents.Metadata.Key);
                if (value.Type == Types.Object)
                    metadata = value.AsObject();
                else
                {
                    metadata = json.Engine.Object.Construct(Array.Empty<JsValue>());
                    json.Set(Constants.Documents.Metadata.Key, metadata, false);
                }

                metadata.Set(Constants.Documents.Metadata.Projection, JsBoolean.True, false);
            }
        }
    }
}
