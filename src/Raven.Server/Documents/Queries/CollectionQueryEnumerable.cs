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

        public long? InternalQueryOperationStart { get; set; }
        public DocumentFields Fields { get; set; } = DocumentFields.All;

        public IEnumerator<Document> GetEnumerator()
        {
            return new Enumerator(_database, _documents, _fieldsToFetch, _collection, _isAllDocsCollection, _query, _queryTimings, _context, _includeDocumentsCommand, _includeCompareExchangeValuesCommand, _totalResults, InternalQueryOperationStart, Fields);
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
            private readonly long? _queryOperationInternalStart;
            private readonly DocumentFields _fields;

            private bool _initialized;

            private int _returnedResults;

            private readonly HashSet<ulong> _alreadySeenProjections;
            private long _start;
            private IEnumerator<Document> _inner;
            private int _innerCount;
            private readonly List<string> _ids;
            private readonly MapQueryResultRetriever _resultsRetriever;
            private readonly string _startsWith;

            public Enumerator(DocumentDatabase database, DocumentsStorage documents, FieldsToFetch fieldsToFetch, string collection, bool isAllDocsCollection,
                IndexQueryServerSide query, QueryTimingsScope queryTimings, DocumentsOperationContext context, IncludeDocumentsCommand includeDocumentsCommand,
                IncludeCompareExchangeValuesCommand includeCompareExchangeValuesCommand, Reference<int> totalResults, long? queryOperationInternalStart,
                DocumentFields fields)
            {
                _documents = documents;
                _fieldsToFetch = fieldsToFetch;
                _collection = collection;
                _isAllDocsCollection = isAllDocsCollection;
                _query = query;
                _context = context;
                _totalResults = totalResults;
                _totalResults.Value = 0;
                _queryOperationInternalStart = queryOperationInternalStart;
                _fields = fields;

                if (_fieldsToFetch.IsDistinct)
                    _alreadySeenProjections = new HashSet<ulong>();

                _resultsRetriever = new MapQueryResultRetriever(database, query, queryTimings, documents, context, fieldsToFetch, includeDocumentsCommand, includeCompareExchangeValuesCommand);

                (_ids, _startsWith) = query.ExtractIdsFromQuery(database.ServerStore, database.Name);
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

                    if (_isAllDocsCollection)
                    {
                        documents = _documents.GetDocumentsStartingWith(_context, _startsWith, null, null, null, _start, _query.PageSize, fields: _fields);
                    }
                    else
                    {
                        documents = _documents.GetDocumentsStartingWith(_context, _startsWith, null, null, null, _start, _query.PageSize, _collection, _fields);
                    }

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
                        documents = Enumerable.Empty<Document>();
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
                    return _queryOperationInternalStart ?? 0;

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
               
            }

        }
    }
}
