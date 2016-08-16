using System;
using System.Collections;
using System.Collections.Generic;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Queries
{
    public class CollectionQueryEnumerable : IEnumerable<Document>
    {
        private readonly DocumentsStorage _documents;
        private readonly FieldsToFetch _fieldsToFetch;
        private readonly DocumentsOperationContext _context;
        private readonly string _collection;
        private readonly IndexQueryServerSide _query;

        public CollectionQueryEnumerable(DocumentsStorage documents, FieldsToFetch fieldsToFetch, string collection, IndexQueryServerSide query, DocumentsOperationContext context)
        {
            _documents = documents;
            _fieldsToFetch = fieldsToFetch;
            _collection = collection;
            _query = query;
            _context = context;
        }

        public IEnumerator<Document> GetEnumerator()
        {
            return new Enumerator(_documents, _fieldsToFetch, _collection, _query, _context);
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
            private readonly string _collection;
            private readonly IndexQueryServerSide _query;

            private bool _initialized;

            private int _returnedResults;

            private readonly HashSet<ulong> _alreadySeenProjections;
            private int _start;
            private IEnumerator<Document> _inner;
            private int _innerCount;

            public Enumerator(DocumentsStorage documents, FieldsToFetch fieldsToFetch, string collection, IndexQueryServerSide query, DocumentsOperationContext context)
            {
                _documents = documents;
                _fieldsToFetch = fieldsToFetch;
                _collection = collection;
                _query = query;
                _context = context;

                if (_fieldsToFetch.IsDistinct)
                    _alreadySeenProjections = new HashSet<ulong>();
            }

            public bool MoveNext()
            {
                if (_initialized == false)
                    _start = Initialize();

                while (true)
                {
                    if (_inner == null)
                    {
                        _inner = _documents.GetDocumentsAfter(_context, _collection, 0, _start, _query.PageSize).GetEnumerator();
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
                                ? MapQueryResultRetriever.GetProjectionFromDocument(_inner.Current, _fieldsToFetch, _context)
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

            private int Initialize()
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
                    foreach (var document in _documents.GetDocumentsAfter(_context, _collection, 0, start, _query.PageSize))
                    {
                        count++;

                        var doc = _fieldsToFetch.IsProjection
                                ? MapQueryResultRetriever.GetProjectionFromDocument(document, _fieldsToFetch, _context)
                                : document;

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