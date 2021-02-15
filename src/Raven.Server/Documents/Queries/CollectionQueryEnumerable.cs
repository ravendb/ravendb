using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Server;
using Voron;
using Constants = Raven.Client.Constants;
using Query = Raven.Server.Documents.Queries.AST.Query;

namespace Raven.Server.Documents.Queries
{
    public class CollectionQueryEnumerable : IEnumerable<Document>
    {
        private readonly DocumentDatabase _database;
        private readonly DocumentsStorage _documents;
        private readonly FieldsToFetch _fieldsToFetch;
        private readonly DocumentsOperationContext _context;
        private readonly IncludeDocumentsCommand _includeDocumentsCommand;
        private readonly IncludeCompareExchangeValuesCommand _includeCompareExchangeValuesCommand;
        private readonly Reference<int> _totalResults;
        private readonly string _collection;
        private readonly IndexQueryServerSide _query;
        private readonly QueryTimingsScope _queryTimings;
        private readonly bool _isAllDocsCollection;

        public CollectionQueryEnumerable(DocumentDatabase database, DocumentsStorage documents, FieldsToFetch fieldsToFetch, string collection,
            IndexQueryServerSide query, QueryTimingsScope queryTimings, DocumentsOperationContext context, IncludeDocumentsCommand includeDocumentsCommand, IncludeCompareExchangeValuesCommand includeCompareExchangeValuesCommand, Reference<int> totalResults)
        {
            _database = database;
            _documents = documents;
            _fieldsToFetch = fieldsToFetch;
            _collection = collection;
            _isAllDocsCollection = collection == Constants.Documents.Collections.AllDocumentsCollection;
            _query = query;
            _queryTimings = queryTimings;
            _context = context;
            _includeDocumentsCommand = includeDocumentsCommand;
            _includeCompareExchangeValuesCommand = includeCompareExchangeValuesCommand;
            _totalResults = totalResults;
        }

        public string StartAfterId { get; set; }

        public Reference<long> AlreadySeenIdsCount { get; set; }

        public DocumentFields Fields { get; set; } = DocumentFields.All;

        public IEnumerator<Document> GetEnumerator()
        {
            return new Enumerator(_database, _documents, _fieldsToFetch, _collection, _isAllDocsCollection, _query, _queryTimings, _context, _includeDocumentsCommand, _includeCompareExchangeValuesCommand, _totalResults, StartAfterId, AlreadySeenIdsCount, Fields);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        private class Enumerator : IEnumerator<Document>
        {
            private readonly DocumentsStorage _documents;
            private readonly FieldsToFetch _fieldsToFetch;
            private readonly DocumentsOperationContext _context;
            private readonly Reference<int> _totalResults;
            private readonly string _collection;
            private readonly bool _isAllDocsCollection;
            private readonly IndexQueryServerSide _query;
            private readonly string _startAfterId;
            private readonly Reference<long> _alreadySeenIdsCount;
            private readonly DocumentFields _fields;

            private bool _initialized;

            private int _returnedResults;

            private readonly HashSet<ulong> _alreadySeenProjections;
            private long _start;
            private IEnumerator<Document> _inner;
            private int _innerCount;
            private readonly List<Slice> _ids;
            private readonly MapQueryResultRetriever _resultsRetriever;
            private readonly string _startsWith;

            public Enumerator(DocumentDatabase database, DocumentsStorage documents, FieldsToFetch fieldsToFetch, string collection, bool isAllDocsCollection,
                IndexQueryServerSide query, QueryTimingsScope queryTimings, DocumentsOperationContext context, IncludeDocumentsCommand includeDocumentsCommand,
                IncludeCompareExchangeValuesCommand includeCompareExchangeValuesCommand, Reference<int> totalResults, 
                string startAfterId, Reference<long> alreadySeenIdsCount, DocumentFields fields)
            {
                _documents = documents;
                _fieldsToFetch = fieldsToFetch;
                _collection = collection;
                _isAllDocsCollection = isAllDocsCollection;
                _query = query;
                _context = context;
                _totalResults = totalResults;
                _totalResults.Value = 0;
                _startAfterId = startAfterId;
                _alreadySeenIdsCount = alreadySeenIdsCount;
                _fields = fields;

                if (_fieldsToFetch.IsDistinct)
                    _alreadySeenProjections = new HashSet<ulong>();

                _resultsRetriever = new MapQueryResultRetriever(database, query, queryTimings, documents, context, fieldsToFetch, includeDocumentsCommand, includeCompareExchangeValuesCommand);

                (_ids, _startsWith) = ExtractIdsFromQuery(query, context);
            }

            private (List<Slice>, string) ExtractIdsFromQuery(IndexQueryServerSide query, DocumentsOperationContext context)
            {
                if (query.Metadata.Query.Where == null)
                    return (null, null);

                if (query.Metadata.IndexFieldNames.Contains(QueryFieldName.DocumentId) == false)
                    return (null, null);

                IDisposable releaseServerContext = null;
                IDisposable closeServerTransaction = null;
                TransactionOperationContext serverContext = null;

                try
                {
                    if (query.Metadata.HasCmpXchg)
                    {
                        releaseServerContext = context.DocumentDatabase.ServerStore.ContextPool.AllocateOperationContext(out serverContext);
                        closeServerTransaction = serverContext.OpenReadTransaction();
                    }

                    using (closeServerTransaction)
                    {
                        var idsRetriever = new RetrieveDocumentIdsVisitor(serverContext, context, query.Metadata, _context.Allocator);

                        idsRetriever.Visit(query.Metadata.Query.Where, query.QueryParameters);

                        return (idsRetriever.Ids?.OrderBy(x => x, SliceComparer.Instance).ToList(), idsRetriever.StartsWith);
                    }
                }
                finally
                {
                    releaseServerContext?.Dispose();
                }
            }

            public bool MoveNext()
            {
                if (_initialized == false)
                    _start = Initialize();

                while (true)
                {
                    if (_inner == null)
                    {
                        _inner = GetDocuments().GetEnumerator();
                        _innerCount = 0;
                    }

                    if (_inner.MoveNext() == false)
                    {
                        Current = null;

                        if (_returnedResults >= _query.PageSize)
                            return false;

                        if (_innerCount < _query.PageSize)
                            return false;

                        _start += _query.PageSize;
                        _inner = null;
                        continue;
                    }

                    _innerCount++;

                    var doc = _fieldsToFetch.IsProjection
                        ? _resultsRetriever.GetProjectionFromDocument(_inner.Current, null, QueryResultRetrieverBase.ZeroScore, _fieldsToFetch, _context, null)
                        : _inner.Current;

                    if (_query.SkipDuplicateChecking || _fieldsToFetch.IsDistinct == false)
                    {
                        _returnedResults++;
                        Current = doc;
                        return true;
                    }

                    if (_alreadySeenProjections.Add(doc.DataHash))
                    {
                        _returnedResults++;
                        Current = doc;
                        return true;
                    }

                    if (_returnedResults >= _query.PageSize)
                        return false;
                }
            }

            private IEnumerable<Document> GetDocuments()
            {
                IEnumerable<Document> documents;

                if (_startsWith != null)
                {
                    var countQuery = false;

                    if (_query.PageSize == 0)
                    {
                        countQuery = true;
                        _query.PageSize = int.MaxValue;
                    }

                    documents = _isAllDocsCollection
                        ? _documents.GetDocumentsStartingWith(_context, _startsWith, null, null, _startAfterId, _start, _query.PageSize, fields: _fields) 
                        : _documents.GetDocumentsStartingWith(_context, _startsWith, null, null, _startAfterId, _start, _query.PageSize, _collection, _fields);

                    if (countQuery)
                    {
                        foreach (var document in documents)
                        {
                            using (document.Data)
                                _totalResults.Value++;
                        }

                        documents = Enumerable.Empty<Document>();

                        _query.PageSize = 0;
                    }
                }
                else if (_ids != null)
                {
                    if (_ids.Count == 0)
                    {
                        documents = Enumerable.Empty<Document>();
                    }
                    else if (_alreadySeenIdsCount != null)
                    {
                        var idsLeft = _ids.Count - _alreadySeenIdsCount.Value;
                        if (idsLeft == 0)
                        {
                            documents = Enumerable.Empty<Document>();
                        }
                        else
                        {
                            var count = idsLeft >= _query.PageSize ? _query.PageSize : idsLeft;
                            var ids = _ids.Skip((int)_alreadySeenIdsCount.Value).Take((int)count);
                            _alreadySeenIdsCount.Value += count;

                            documents = _isAllDocsCollection
                                ? _documents.GetDocuments(_context, ids, 0, _query.PageSize, _totalResults)
                                : _documents.GetDocuments(_context, ids, _collection, 0, _query.PageSize, _totalResults);
                        }
                    }
                    else
                    {
                        documents = _isAllDocsCollection
                            ? _documents.GetDocuments(_context, _ids, _start, _query.PageSize, _totalResults)
                            : _documents.GetDocuments(_context, _ids, _collection, _start, _query.PageSize, _totalResults);
                    }
                }
                else if (_isAllDocsCollection)
                {
                    documents = _documents.GetDocumentsFrom(_context, 0, _start, _query.PageSize);
                    _totalResults.Value = (int)_documents.GetNumberOfDocuments(_context);
                }
                else
                {
                    documents = _documents.GetDocumentsFrom(_context, _collection, 0, _start, _query.PageSize);
                    _totalResults.Value = (int)_documents.GetCollection(_collection, _context).Count;
                }

                return documents;
            }

            private long Initialize()
            {
                _initialized = true;

                if (_query.Start == 0)
                    return 0;

                if (_query.SkipDuplicateChecking)
                    return _query.Start;

                if (_fieldsToFetch.IsDistinct == false)
                    return _query.Start;

                var start = 0;
                while (true)
                {
                    var count = 0;
                    foreach (var document in _documents.GetDocumentsFrom(_context, _collection, 0, start, _query.PageSize))
                    {
                        count++;

                        var doc = _fieldsToFetch.IsProjection
                            ? _resultsRetriever.GetProjectionFromDocument(document, null, QueryResultRetrieverBase.ZeroScore, _fieldsToFetch, _context, null)
                            : _inner.Current;

                        if (doc.Data.Count <= 0)
                            continue;

                        if (_alreadySeenProjections.Add(doc.DataHash) && _alreadySeenProjections.Count == _query.Start)
                            break;
                    }

                    if (_alreadySeenProjections.Count == _query.Start)
                        break;

                    if (count < _query.PageSize)
                        break;

                    start += count;
                }

                return start;
            }

            public void Reset()
            {
                throw new NotSupportedException();
            }

            public Document Current { get; private set; }

            object IEnumerator.Current => Current;

            public void Dispose()
            {
                if (_ids != null)
                {
                    foreach (var id in _ids)
                    {
                        id.Release(_context.Allocator);
                    }
                }
            }

            private class RetrieveDocumentIdsVisitor : WhereExpressionVisitor
            {
                private readonly Query _query;
                private readonly TransactionOperationContext _serverContext;
                private readonly DocumentsOperationContext _context;
                private readonly QueryMetadata _metadata;
                private readonly ByteStringContext _allocator;
                public string StartsWith;

                public HashSet<Slice> Ids { get; private set; }

                public RetrieveDocumentIdsVisitor(TransactionOperationContext serverContext, DocumentsOperationContext context, QueryMetadata metadata, ByteStringContext allocator) : base(metadata.Query.QueryText)
                {
                    _query = metadata.Query;
                    _serverContext = serverContext;
                    _context = context;
                    _metadata = metadata;
                    _allocator = allocator;
                }

                public override void VisitBooleanMethod(QueryExpression leftSide, QueryExpression rightSide, OperatorType operatorType, BlittableJsonReaderObject parameters)
                {
                    VisitFieldToken(leftSide, rightSide, parameters, operatorType);
                }

                public override void VisitFieldToken(QueryExpression fieldName, QueryExpression value, BlittableJsonReaderObject parameters, OperatorType? operatorType)
                {
                    if (fieldName is MethodExpression me)
                    {
                        var methodType = QueryMethod.GetMethodType(me.Name.Value);
                        switch (methodType)
                        {
                            case MethodType.Id:
                                if (value is ValueExpression ve)
                                {
                                    var id = QueryBuilder.GetValue(_query, _metadata, parameters, ve);

                                    Debug.Assert(id.Type == ValueTokenType.String || id.Type == ValueTokenType.Null);

                                    AddId(id.Value?.ToString());
                                }
                                if (value is MethodExpression right)
                                {
                                    var id = QueryBuilder.EvaluateMethod(_query, _metadata, _serverContext, _context, right, ref parameters);
                                    if (id is ValueExpression v)
                                        AddId(v.Token.Value);
                                }
                                break;
                        }
                    }
                }

                public override void VisitBetween(QueryExpression fieldName, QueryExpression firstValue, QueryExpression secondValue, BlittableJsonReaderObject parameters)
                {
                    if (fieldName is MethodExpression me && string.Equals("id", me.Name.Value, StringComparison.OrdinalIgnoreCase) && firstValue is ValueExpression fv && secondValue is ValueExpression sv)
                    {
                        throw new InvalidQueryException("Collection query does not support filtering by id() using Between operator. Supported operators are: =, IN",
                            QueryText, parameters);
                    }
                }

                public override void VisitIn(QueryExpression fieldName, List<QueryExpression> values, BlittableJsonReaderObject parameters)
                {
                    if (Ids == null)
                        Ids = new HashSet<Slice>(SliceComparer.Instance); // this handles a case where IN is used with empty list

                    if (fieldName is MethodExpression me && string.Equals("id", me.Name.Value, StringComparison.OrdinalIgnoreCase))
                    {
                        foreach (var item in values)
                        {
                            if (item is ValueExpression iv)
                            {
                                foreach (var id in QueryBuilder.GetValues(_query, _metadata, parameters, iv))
                                {
                                    AddId(id.Value?.ToString());
                                }
                            }
                        }
                    }
                    else
                    {
                        throw new InvalidQueryException("Collection query does not support filtering by id() using Between operator. Supported operators are: =, IN",
                            QueryText, parameters);
                    }
                }

                public override void VisitMethodTokens(StringSegment name, List<QueryExpression> arguments, BlittableJsonReaderObject parameters)
                {
                    var expression = arguments[arguments.Count - 1];
                    if (expression is BinaryExpression be && be.Operator == OperatorType.Equal)
                    {
                        VisitFieldToken(new MethodExpression("id", new List<QueryExpression>()), be.Right, parameters, be.Operator);
                    }
                    else if (expression is InExpression ie)
                    {
                        VisitIn(new MethodExpression("id", new List<QueryExpression>()), ie.Values, parameters);
                    }
                    else if (string.Equals(name.Value, "startsWith", StringComparison.OrdinalIgnoreCase))
                    {
                        if (expression is ValueExpression iv)
                        {
                            var prefix = QueryBuilder.GetValue(_query, _metadata, parameters, iv);
                            StartsWith = prefix.Value?.ToString();
                        }
                    }
                    else
                    {
                        ThrowNotSupportedCollectionQueryOperator(expression.Type.ToString(), parameters);
                    }
                }

                private void AddId(string id)
                {
                    Slice key;
                    if (string.IsNullOrEmpty(id) == false)
                    {
                        Slice.From(_allocator, id, out key);
                        _allocator.ToLowerCase(ref key.Content);
                    }
                    else
                    {
                        // this is a rare case
                        // we are allocating here, because we are releasing all of the ids later on
                        // if we will use Slices.Empty, then we will release that on a different context
                        Slice.From(_allocator, string.Empty, out key);
                    }

                    if (Ids == null)
                        Ids = new HashSet<Slice>(SliceComparer.Instance);

                    Ids.Add(key);
                }

                private void ThrowNotSupportedCollectionQueryOperator(string @operator, BlittableJsonReaderObject parameters)
                {
                    throw new InvalidQueryException(
                        $"Collection query does not support filtering by {Constants.Documents.Indexing.Fields.DocumentIdFieldName} using {@operator} operator. Supported operators are: =, IN",
                        QueryText, parameters);
                }
            }
        }
    }
}
