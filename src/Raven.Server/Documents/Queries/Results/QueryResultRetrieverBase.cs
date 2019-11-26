using System;
using System.Linq;
using Lucene.Net.Documents;
using Sparrow.Json.Parsing;
using Sparrow.Json;
using System.Collections.Generic;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Json;
using Jint.Native;
using Jint.Native.Object;
using Jint.Runtime;
using Lucene.Net.Store;
using Raven.Client;
using Raven.Client.Exceptions;
using Raven.Client.Extensions;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using static Raven.Server.Documents.TimeSeries.TimeSeriesStorage.Reader;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.TimeSeries;
using Sparrow;
using Sparrow.Extensions;
using BinaryExpression = Raven.Server.Documents.Queries.AST.BinaryExpression;

namespace Raven.Server.Documents.Queries.Results
{
    public abstract class QueryResultRetrieverBase : IQueryResultRetriever
    {
        public static readonly Lucene.Net.Search.ScoreDoc ZeroScore = new Lucene.Net.Search.ScoreDoc(-1, 0f);

        public static readonly Lucene.Net.Search.ScoreDoc OneScore = new Lucene.Net.Search.ScoreDoc(-1, 1f);

        private readonly DocumentDatabase _database;
        private readonly IndexQueryServerSide _query;
        private readonly JsonOperationContext _context;
        private readonly IncludeDocumentsCommand _includeDocumentsCommand;
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

        private Dictionary<ValueExpression, object> _valuesDictionary;

        private Dictionary<FieldExpression, object> _argumentValuesDictionary;


        protected QueryResultRetrieverBase(DocumentDatabase database, IndexQueryServerSide query, QueryTimingsScope queryTimings, FieldsToFetch fieldsToFetch, DocumentsStorage documentsStorage, JsonOperationContext context, bool reduceResults, IncludeDocumentsCommand includeDocumentsCommand)
        {
            _database = database;
            _query = query;
            _context = context;
            _includeDocumentsCommand = includeDocumentsCommand;

            DocumentsStorage = documentsStorage;
            RetrieverScope = queryTimings?.For(nameof(QueryTimingsScope.Names.Retriever), start: false);
            FieldsToFetch = fieldsToFetch;

            DocumentFields = query?.DocumentFields ?? DocumentFields.All;

            _blittableTraverser = reduceResults ? BlittableJsonTraverser.FlatMapReduceResults : BlittableJsonTraverser.Default;
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
                        return null;
                    return GetProjectionFromDocument(doc, input, scoreDoc, FieldsToFetch, _context, state);
                }

                var documentLoaded = false;

                var result = new DynamicJsonValue();

                Dictionary<string, FieldsToFetch.FieldToFetch> fields = null;
                if (FieldsToFetch.ExtractAllFromIndex)
                {
                    fields = input.GetFields()
                        .Where(x => x.Name != Constants.Documents.Indexing.Fields.DocumentIdFieldName
                                    && x.Name != Constants.Documents.Indexing.Fields.ReduceKeyHashFieldName
                                    && x.Name != Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName
                                    && FieldUtil.GetRangeTypeFromFieldName(x.Name) == RangeType.None)
                        .Distinct(UniqueFieldNames.Instance)
                        .ToDictionary(x => x.Name, x => new FieldsToFetch.FieldToFetch(x.Name, null, null, x.IsStored, isDocumentId: false));
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

                    if (documentLoaded == false)
                    {
                        using (_projectionStorageScope = _projectionStorageScope?.Start() ?? _projectionScope?.For(nameof(QueryTimingsScope.Names.Storage)))
                            doc = DirectGet(input, lowerId, DocumentFields.All, state);

                        documentLoaded = true;
                    }

                    if (doc == null)
                        continue;

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
            var result = new DynamicJsonValue();

            foreach (var fieldToFetch in fieldsToFetch.Fields.Values)
            {
                if (TryGetValue(fieldToFetch, doc, luceneDoc, state, fieldsToFetch.IndexFields, fieldsToFetch.AnyDynamicIndexFields, out var key, out var fieldVal) == false &&
                    fieldToFetch.QueryField != null && fieldToFetch.QueryField.HasSourceAlias)
                    continue;

                var immediateResult = AddProjectionToResult(doc, scoreDoc, fieldsToFetch, result, key, fieldVal);

                if (immediateResult != null)
                    return immediateResult;
            }

            return ReturnProjection(result, doc, scoreDoc, context);
        }

        protected Document AddProjectionToResult(Document doc, Lucene.Net.Search.ScoreDoc scoreDoc, FieldsToFetch fieldsToFetch, DynamicJsonValue result, string key, object fieldVal)
        {
            if (fieldsToFetch.SingleBodyOrMethodWithNoAlias)
            {
                Document newDoc = null;
                if (fieldVal is BlittableJsonReaderObject nested)
                {
                    newDoc = new Document
                    {
                        Id = doc.Id,
                        ChangeVector = doc.ChangeVector,
                        Data = nested,
                        Etag = doc.Etag,
                        Flags = doc.Flags,
                        LastModified = doc.LastModified,
                        LowerId = doc.LowerId,
                        NonPersistentFlags = doc.NonPersistentFlags,
                        StorageId = doc.StorageId,
                        TransactionMarker = doc.TransactionMarker
                    };
                }
                else if (fieldVal is Document d)
                {
                    newDoc = d;
                }
                else
                    ThrowInvalidQueryBodyResponse(fieldVal);

                FinishDocumentSetup(newDoc, scoreDoc);
                return newDoc;
            }

            AddProjectionToResult(result, key, fieldVal);
            return null;
        }

        protected static void AddProjectionToResult(DynamicJsonValue result, string key, object fieldVal)
        {
            if (fieldVal is List<object> list)
            {
                var array = new DynamicJsonArray();
                for (int i = 0; i < list.Count; i++)
                {
                    if (list[i] is Document d3)
                        array.Add(d3.Data);
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
            result[Constants.Documents.Metadata.Key] = new DynamicJsonValue
            {
                [Constants.Documents.Metadata.Projection] = true
            };

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

            return context.ReadForMemory(stringValue, field.Name);
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
                        args[i] = Tuple.Create(document, luceneDoc, state, indexFields, anyDynamicIndexFields);
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
                        IncludeUtil.GetDocIdFromInclude(loadedDoc.Data, fieldToFetch.QueryField.SourceAlias, _loadedDocumentIds);
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
                    IncludeUtil.GetDocIdFromInclude(document.Data, fieldToFetch.QueryField.SourceAlias, _loadedDocumentIds);
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
                    _loadedDocuments[docId] = doc = LoadDocument(docId);
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
                    args);

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
                    if (function.Value.Type != DeclaredFunction.FunctionType.JavaScript)
                        continue;

                    if (other._functions != null && (other._functions.TryGetValue(function.Key, out var otherVal) == false
                                                     || function.Value.FunctionText != otherVal.FunctionText))
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
                        if (function.Value.Type != DeclaredFunction.FunctionType.JavaScript)
                            continue;

                        hashCode = (hashCode * 397) ^ (function.Value.GetHashCode());
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
                    if (kvp.Value.Type != DeclaredFunction.FunctionType.JavaScript)
                        continue;

                    runner.AddScript(kvp.Value.FunctionText);
                }
            }
        }

        public object InvokeFunction(string methodName, Query query, string documentId, object[] args)
        {
            if (query.DeclaredFunctions != null && 
                query.DeclaredFunctions.TryGetValue(methodName, out var func) && 
                func.Type == DeclaredFunction.FunctionType.TimeSeries)
                return InvokeTimeSeriesFunction(func, documentId, args);

            var key = new QueryKey(query.DeclaredFunctions);
            using (_database.Scripts.GetScriptRunner(key, readOnly: true, patchRun: out var run))
            using (var result = run.Run(_context, _context as DocumentsOperationContext, methodName, args))
            {
                _includeDocumentsCommand?.AddRange(run.Includes, documentId);

                if (result.IsNull)
                    return null;

                return run.Translate(result, _context, QueryResultModifier.Instance);
            }
        }

        private BlittableJsonReaderObject InvokeTimeSeriesFunction(DeclaredFunction declaredFunction, string documentId, object[] args)
        {
            if (_valuesDictionary == null)
                _valuesDictionary = new Dictionary<ValueExpression, object>();

            if (_argumentValuesDictionary == null)
                _argumentValuesDictionary = new Dictionary<FieldExpression, object>();

            var tss = _database.DocumentsStorage.TimeSeriesStorage;
            var timeSeriesFunction = declaredFunction.TimeSeries;
            var source = GetSourceAndId();

            var min = GetDateValue(timeSeriesFunction.Between.MinExpression, declaredFunction, args) ?? DateTime.MinValue;
            var max = GetDateValue(timeSeriesFunction.Between.MaxExpression, declaredFunction, args) ?? DateTime.MaxValue;

            long count = 0;
            var array = new DynamicJsonArray();
            TimeSeriesStorage.Reader reader;

            if (timeSeriesFunction.GroupBy == null)
                return GetRawValues();
            
            var groupBy = timeSeriesFunction.GroupBy.GetValue(_query.QueryParameters)?.ToString();
            if (groupBy == null)
                throw new ArgumentException("Unable to parse group by value, expected range specification, but got a null");

            var rangeSpec = TimeSeriesFunction.ParseRangeFromString(groupBy);

            var aggStates = new TimeSeriesAggregation[timeSeriesFunction.Select.Count];
            InitializeAggregationStates(timeSeriesFunction, aggStates);

            DateTime start = default, next = default;
            return GetAggregatedValues();

            void AggregateIndividualItems(IEnumerable<SingleResult> items)
            {
                foreach (var cur in items)
                {
                    MaybeMoveToNextRange(cur.TimeStamp);

                    if (ShouldFilter(timeSeriesFunction.Where, cur))
                        continue;
                    
                    count++;
                    for (int i = 0; i < aggStates.Length; i++)
                    {
                        aggStates[i].Step(cur.Values.Span);
                    }
                }
            }

            void MaybeMoveToNextRange(DateTime ts)
            {
                if (ts <= next)
                    return;

                if (aggStates[0].Any)
                {
                    array.Add(AddTimeSeriesResult(timeSeriesFunction, aggStates, start, next));
                }

                start = rangeSpec.GetRangeStart(ts);
                next = rangeSpec.GetNextRangeStart(start);

                for (int i = 0; i < aggStates.Length; i++)
                {
                    aggStates[i].Init();
                }
            }

            BlittableJsonReaderObject GetRawValues()
            {
                reader = tss.GetReader(_includeDocumentsCommand.Context, documentId, source, min, max);

                foreach (var singleResult in reader.AllValues())
                {
                    if (ShouldFilter(timeSeriesFunction.Where, singleResult))
                        continue;

                    var vals = new DynamicJsonArray();
                    for (var index = 0; index < singleResult.Values.Span.Length; index++)
                    {
                        vals.Add(singleResult.Values.Span[index]);
                    }

                    array.Add(new DynamicJsonValue
                    {
                        ["Tag"] = singleResult.Tag.ToString(),
                        ["Timestamp"] = singleResult.TimeStamp,
                        ["Values"] = vals
                    });

                    count++;
                }

                _argumentValuesDictionary?.Clear();

                return _context.ReadObject(new DynamicJsonValue
                {
                    ["Count"] = count,
                    ["Results"] = array
                }, "timeseries/value");
            }

            BlittableJsonReaderObject GetAggregatedValues()
            {
                reader = tss.GetReader(_includeDocumentsCommand.Context, documentId, source, min, max);

                foreach (var it in reader.SegmentsOrValues())
                {
                    if (it.IndividualValues != null)
                    {
                        AggregateIndividualItems(it.IndividualValues);
                    }
                    else
                    {
                        //We might need to close the old aggregation range and start a new one
                        MaybeMoveToNextRange(it.Segment.Start);

                        // now we need to see if we can consume the whole segment, or 
                        // if the range it cover needs to be broken up to multiple ranges.
                        // For example, if the segment covers 3 days, but we have group by 1 hour,
                        // we still have to deal with the individual values
                        if (it.Segment.End > next || timeSeriesFunction.Where != null)
                        {
                            AggregateIndividualItems(it.Segment.Values);
                        }
                        else
                        {
                            var span = it.Segment.Summary.Span;
                            for (int i = 0; i < aggStates.Length; i++)
                            {
                                aggStates[i].Segment(span);
                            }

                            count += span[0].Count;
                        }
                    }
                }

                if (aggStates[0].Any)
                {
                    array.Add(AddTimeSeriesResult(timeSeriesFunction, aggStates, start, next));
                }

                _argumentValuesDictionary?.Clear();

                return _context.ReadObject(new DynamicJsonValue
                {
                    ["Count"] = count,
                    ["Results"] = array
                }, "timeseries/value");
            }

            bool ShouldFilter(QueryExpression filter, SingleResult singleResult)
            {
                if (filter == null)
                    return false;

                if (filter is BinaryExpression be)
                {
                    switch (be.Operator)
                    {
                        case OperatorType.And:
                            return ShouldFilter(be.Left, singleResult) ||
                                   ShouldFilter(be.Right, singleResult);
                        case OperatorType.Or:
                            return ShouldFilter(be.Left, singleResult) &&
                                   ShouldFilter(be.Right, singleResult);
                    }

                    var left = GetValue(be.Left, singleResult);
                    var right = GetValue(be.Right, singleResult);
                    bool result;

                    switch (left)
                    {
                        case DateTime dt:
                            result = CompareDateTimes(dt, right);
                            break;
                        case LazyNumberValue lnv:
                            result = CompareNumbers(lnv, right);
                            break;
                        case LazyStringValue lsv:
                            result = CompareStrings(lsv, right);
                            break;
                        default:
                            result = CompareDynamic(left, right);
                            break;
                    }

                    return result == false;
                }

                if (filter is NegatedExpression ne)
                {
                    return ShouldFilter(ne.Expression, singleResult) == false;
                }

                if (filter is InExpression inExpression)
                {
                    var result = EvaluateInExpression();
                    return result == false;
                }

                if (filter is TimeSeriesBetweenExpression betweenExpression)
                {
                    var result = EvaluateBetweenExpression();
                    return result == false;
                }

                throw new InvalidQueryException($"Unsupported expression '{filter}' inside WHERE clause of TimeSeries function '{declaredFunction.Name}'. " +
                                                "Supported expressions are : Binary Expressions (=, !=, <, >, <=, >=, AND, OR, NOT), IN expressions, BETWEEN expressions.");

                bool CompareNumbers(LazyNumberValue lnv, object right)
                {
                    switch (be.Operator)
                    {
                        case OperatorType.Equal:
                            return lnv.Equals(right);
                        case OperatorType.NotEqual:
                            return lnv.Equals(right) == false;
                        case OperatorType.LessThan:
                            return lnv.CompareTo(right) < 0;
                        case OperatorType.GreaterThan:
                            return lnv.CompareTo(right) > 0;
                        case OperatorType.LessThanEqual:
                            return lnv.CompareTo(right) <= 0;
                        case OperatorType.GreaterThanEqual:
                            return lnv.CompareTo(right) >= 0;
                        default:
                            throw new InvalidQueryException($"Invalid binary expression '{be}' inside WHERE clause of time series function '{declaredFunction.Name}'." +
                                                                $"Operator '{be.Operator}' is not supported.");
                    }
                }

                bool CompareDateTimes(DateTime? dateTime, object right)
                {
                    DateTime? rightAsDt;

                    if (right is DateTime dt)
                        rightAsDt = dt;
                    else
                        rightAsDt = ParseDateTime(right?.ToString());

                    switch (be.Operator)
                    {
                        case OperatorType.Equal:
                            return dateTime.Equals(rightAsDt);
                        case OperatorType.NotEqual:
                            return dateTime.Equals(rightAsDt) == false;
                        case OperatorType.LessThan:
                            return dateTime < rightAsDt;
                        case OperatorType.GreaterThan:
                            return dateTime > rightAsDt;
                        case OperatorType.LessThanEqual:
                            return dateTime <= rightAsDt;
                        case OperatorType.GreaterThanEqual:
                            return dateTime >= rightAsDt;
                        default:
                            throw new InvalidQueryException($"Invalid binary expression '{be}' inside WHERE clause of time series function '{declaredFunction.Name}'." +
                                                                $"Operator '{be.Operator}' is not supported.");
                    }
                }

                bool CompareStrings(LazyStringValue lsv, object right)
                {
                    if (right is DateTime)
                    {
                        var leftAsDt = ParseDateTime(lsv);
                        return CompareDateTimes(leftAsDt, right);
                    }

                    switch (be.Operator)
                    {
                        case OperatorType.Equal:
                            return lsv.Equals(right);
                        case OperatorType.NotEqual:
                            return lsv.Equals(right) == false;
                        case OperatorType.LessThan:
                            return lsv.CompareTo(right) < 0;
                        case OperatorType.GreaterThan:
                            return lsv.CompareTo(right) > 0;
                        case OperatorType.LessThanEqual:
                            return lsv.CompareTo(right) <= 0;
                        case OperatorType.GreaterThanEqual:
                            return lsv.CompareTo(right) >= 0;
                        default:
                            throw new InvalidQueryException($"Invalid binary expression '{be}' inside WHERE clause of time series function '{declaredFunction.Name}'." +
                                                                $"Operator '{be.Operator}' is not supported.");
                    }
                }

                bool CompareDynamic(dynamic left, dynamic right)
                {
                    bool result;
                    switch (be.Operator)
                    {
                        case OperatorType.Equal:
                            result = Equals(left, right);
                            break;
                        case OperatorType.NotEqual:
                            result = Equals(left, right) == false;
                            break;
                        case OperatorType.LessThan:
                            result = left < right;
                            break;
                        case OperatorType.GreaterThan:
                            result = left > right;
                            break;
                        case OperatorType.LessThanEqual:
                            result = left <= right;
                            break;
                        case OperatorType.GreaterThanEqual:
                            result = left >= right;
                            break;
                        default:
                            throw new InvalidQueryException($"Invalid binary expression '{be}' inside WHERE clause of time series function '{declaredFunction.Name}'." +
                                                            $"Operator '{be.Operator}' is not supported.");
                    }

                    return result;
                }

                bool EvaluateInExpression()
                {
                    dynamic src = GetValue(inExpression.Source, singleResult);
                    bool result = false;
                    dynamic val;

                    if (inExpression.All)
                        throw new InvalidQueryException($"Invalid InExpression '{inExpression}' inside WHERE clause of time series function '{declaredFunction.Name}'." +
                                                        "Operator 'ALL IN' is not supported in time series functions");

                    if (src is LazyNumberValue lnv)
                    {
                        for (int i = 0; i < inExpression.Values.Count; i++)
                        {
                            val = GetValue(inExpression.Values[i], singleResult);

                            if (lnv.CompareTo(val) == 0)
                            {
                                result = true;
                                break;
                            }
                        }
                    }
                    else
                    {
                        for (int i = 0; i < inExpression.Values.Count; i++)
                        {
                            val = GetValue(inExpression.Values[i], singleResult);
                            if (src == val)
                            {
                                result = true;
                                break;
                            }
                        }
                    }

                    return result;
                }

                bool EvaluateBetweenExpression()
                {
                    var result = false;

                    dynamic src = GetValue(betweenExpression.Source, singleResult);
                    dynamic value = GetValue(betweenExpression.MinExpression, singleResult);

                    if (src is LazyNumberValue lnv)
                    {
                        if (lnv.CompareTo(value) >= 0)
                        {
                            value = GetValue(betweenExpression.MaxExpression, singleResult);
                            result = lnv.CompareTo(value) <= 0;
                        }
                    }

                    else if (src >= value)
                    {
                        value = GetValue(betweenExpression.MaxExpression, singleResult);
                        result = src <= value;
                    }

                    return result;
                }
            }


            object GetValue(QueryExpression expression, SingleResult singleResult)
            {
                if (expression is FieldExpression fe)
                {
                    switch (fe.Compound[0].Value.ToLower())
                    {
                        case "tag":
                            if (fe.Compound.Count > 1)
                                throw new InvalidQueryException($"Failed to evaluate expression '{fe}'");
                            return singleResult.Tag.ToString();
                        case "values":
                            if (fe.Compound.Count == 1)
                                return singleResult.Values;

                            if (fe.Compound.Count > 2)
                                throw new InvalidQueryException($"Failed to evaluate expression '{fe}'");

                            if (int.TryParse(fe.Compound[1].Value, out var index) == false)
                                throw new InvalidQueryException($"Failed to evaluate expression '{fe}'");

                            if (index >= singleResult.Values.Length)
                                return null;

                            return singleResult.Values.Span[index];
                        case "timestamp":
                            if (fe.Compound.Count > 1)
                                throw new InvalidQueryException($"Failed to evaluate expression '{fe}'");
                            return singleResult.TimeStamp;
                        default:
                            if (fe.Compound[0].Value == timeSeriesFunction.LoadTagAs?.Value)
                                return GetValueFromLoadedTag(fe, singleResult);
                            
                            if (_argumentValuesDictionary.TryGetValue(fe, out var val) == false)
                                _argumentValuesDictionary[fe] = val = GetValueFromArgument(declaredFunction, args, fe);
                            
                            return val;
                    }
                }

                if (expression is ValueExpression ve)
                {
                    if (_valuesDictionary.TryGetValue(ve, out var val) == false)
                    {
                        _valuesDictionary[ve] = val = ve.Value == ValueTokenType.String
                            ? ve.Token.Value
                            : ve.GetValue(_query.QueryParameters);
                    }

                    return val;
                }

                // shouldn't happen - query parser should have caught this 
                throw new InvalidQueryException($"Failed to invoke time series function '{declaredFunction.Name}'. Unable to get the value of expression '{expression}'. " +
                                                    $"Unsupported expression type : '{expression.Type}'");
            }

            string GetSourceAndId()
            {
                var compound = ((FieldExpression)timeSeriesFunction.Between.Source).Compound;

                if (compound.Count == 1)
                    return ((FieldExpression)timeSeriesFunction.Between.Source).FieldValue;
                
                if (args == null)
                    throw new InvalidQueryException($"Unable to parse TimeSeries name from expression '{(FieldExpression)timeSeriesFunction.Between.Source}'. " +
                                                        $"'{compound[0]}' is unknown, and no arguments were provided to time series function '{declaredFunction.Name}'.");
                
                if (args.Length < declaredFunction.Parameters.Count)
                    throw new InvalidQueryException($"Incorrect number of arguments passed to time series function '{declaredFunction.Name}'." +
                                                        $"Expected '{declaredFunction.Parameters.Count}' arguments, but got '{args.Length}'");
                
                var index = GetParameterIndex(declaredFunction, compound[0]);
                if (index == 0)
                {
                    if (args[0] is Document document)
                        documentId = document.Id;
                }
                else
                {
                    if (index == declaredFunction.Parameters.Count) // not found
                        throw new InvalidQueryException($"Unable to parse TimeSeries name from expression '{(FieldExpression)timeSeriesFunction.Between.Source}'. " +
                                                            $"'{compound[0]}' is unknown, and no matching argument was provided to time series function '{declaredFunction.Name}'.");
                    
                    if (!(args[index] is Document document))
                        throw new InvalidQueryException($"Unable to parse TimeSeries name from expression '{(FieldExpression)timeSeriesFunction.Between.Source}'. " +
                                                            $"Expected argument '{compound[0]}' to be a Document instance, but got '{args[index].GetType()}'");
                    
                    documentId = document.Id;
                }

                return ((FieldExpression)timeSeriesFunction.Between.Source).FieldValueWithoutAlias;
            }
        }

        private static object GetValueFromArgument(DeclaredFunction declaredFunction, object[] args, FieldExpression fe)
        {
            if (args == null)
                throw new InvalidQueryException($"Unable to get the value of '{fe}'. '{fe.Compound[0]}' is unknown, and no arguments were provided to time series function '{declaredFunction.Name}'.");
            
            if (args.Length < declaredFunction.Parameters.Count)
                throw new InvalidQueryException($"Incorrect number of arguments passed to time series function '{declaredFunction.Name}'. " +
                                                    $"Expected '{declaredFunction.Parameters.Count}', but got '{args.Length}.'");
            
            var index = GetParameterIndex(declaredFunction, fe.Compound[0]);

            if (index == declaredFunction.Parameters.Count) // not found
                throw new InvalidQueryException($"Unable to get the value of '{fe}'. '{fe.Compound[0]}' is unknown, " +
                                                    $"and no matching argument was provided to time series function '{declaredFunction.Name}'.");

            if (args[index] == null)
                return null;
            
            if (!(args[index] is Document doc))
            {
                if (index == 0 && args[0] is Tuple<Document, Lucene.Net.Documents.Document, IState, Dictionary<string, IndexField>, bool?> tuple)
                    doc = tuple.Item1;
                else
                    return args[index];
            }

            if (fe.Compound.Count == 1)
                return doc;
            
            return GetFieldFromDocument(fe, doc);
        }

        private static int GetParameterIndex(DeclaredFunction declaredFunction, StringSegment str)
        {
            for (int i = 0; i < declaredFunction.Parameters.Count; i++)
            {
                var parameter = declaredFunction.Parameters[i];

                if (str == ((FieldExpression)parameter).FieldValue)
                    return i;
            }

            return declaredFunction.Parameters.Count;
        }

        private static object GetFieldFromDocument(FieldExpression fe, Document document)
        {
            if (document == null)
                return null;

            var currentPart = 1;
            object val = null;
            var data = document.Data;

            while (currentPart < fe.Compound.Count)
            {
                if (data.TryGetMember(fe.Compound[currentPart], out val) == false)
                {
                    var id = currentPart == 1
                        ? document.Id
                        : data.TryGetId(out var nestedId) 
                            ? nestedId : null;

                    throw new InvalidQueryException($"Unable to get the value of '{fe.FieldValueWithoutAlias}' from document '{document.Id}'. " +
                                                        $"Document '{id}' does not have a property named '{fe.Compound[currentPart]}'.");
                }

                if (!(val is BlittableJsonReaderObject nested))
                    break;

                data = nested;
                currentPart++;
            }

            return val;
        }

        private object GetValueFromLoadedTag(FieldExpression fe, SingleResult singleResult)
        {
            if (_loadedDocuments == null)
                _loadedDocuments = new Dictionary<string, Document>();

            var tag = singleResult.Tag?.ToString();
            if (tag == null)
                throw new InvalidQueryException("Unable to load document from 'Tag' property of time series entry. 'Tag' is null. " +
                                                    $"Timestamp: '{singleResult.TimeStamp.GetDefaultRavenFormat()}'");

            if (_loadedDocuments.TryGetValue(tag, out var document) == false)
                _loadedDocuments[tag] = document = _database.DocumentsStorage.Get(_includeDocumentsCommand.Context, tag);

            if (fe.Compound.Count == 1)
                return document;

            if (document == null)
                throw new InvalidQueryException($"Unable to load document '{tag}' from 'Tag' property of time series entry. " +
                                                    $"Document '{tag}' does not exist. Timestamp: '{singleResult.TimeStamp.GetDefaultRavenFormat()}'");

            return GetFieldFromDocument(fe, document);
        }

        private static void InitializeAggregationStates(TimeSeriesFunction timeSeriesFunction, TimeSeriesAggregation[] aggStates)
        {
            for (int i = 0; i < timeSeriesFunction.Select.Count; i++)
            {
                if (timeSeriesFunction.Select[i].Item1 is MethodExpression me)
                {
                    if (Enum.TryParse(me.Name.Value, ignoreCase: true, out TimeSeriesAggregation.Type type))
                    {
                        aggStates[i] = new TimeSeriesAggregation(type);
                        continue;
                    }

                    throw new ArgumentException("Unknown method in timeseries query: " + me);
                }

                throw new ArgumentException("Unknown method in timeseries query: " + timeSeriesFunction.Select[i].Item1);
            }
        }

        private static DynamicJsonValue AddTimeSeriesResult(TimeSeriesFunction func, TimeSeriesAggregation[] aggStates, DateTime start, DateTime next)
        {
            var result = new DynamicJsonValue
            {
                ["From"] = start,
                ["To"] = next,
                ["Count"] = new DynamicJsonArray(aggStates[0].Count)
            };
            for (int i = 0; i < aggStates.Length; i++)
            {
                var name = func.Select[i].Item2?.ToString() ?? aggStates[i].Aggregation.ToString();
                result[name] = new DynamicJsonArray(aggStates[i].GetFinalValues());
            }
            return result;
        }

        private DateTime? GetDateValue(QueryExpression qe, DeclaredFunction func, object[] args)
        {
            if (qe == null)
                return null;

            if (qe is ValueExpression ve)
            {
                if (_valuesDictionary.TryGetValue(ve, out var value))
                    return (DateTime)value;

                var val = ve.GetValue(_query.QueryParameters);
                if (val == null)
                    throw new ArgumentException("Unable to parse timeseries from/to values. Got a null instead of a value"); 

                DateTime? result;
                _valuesDictionary[ve] = result = ParseDateTime(val.ToString());

                return result;
            }

            if (qe is FieldExpression fe)
            {
                var val = GetValueFromArgument(func, args, fe);
                return ParseDateTime(val.ToString());
            }

            throw new ArgumentException("Unable to parse timeseries from/to values. Got: " + qe);
        }

        private static unsafe DateTime? ParseDateTime(string valueAsStr)
        {
            fixed (char* c = valueAsStr)
            {
                var result = LazyStringParser.TryParseDateTime(c, valueAsStr.Length, out var dt, out _);
                if (result != LazyStringParser.Result.DateTime)
                    throw new ArgumentException("Unable to parse timeseries from/to values. Got: " + valueAsStr);
                return dt;
            }
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
                    json.Put(Constants.Documents.Metadata.Key, metadata, false);
                }

                metadata.Put(Constants.Documents.Metadata.Projection, JsBoolean.True, false);
            }
        }
    }
}
