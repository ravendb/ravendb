using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Dynamic;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.Documents.Sharding.Queries;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Voron;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Documents.Queries
{
    public sealed class CollectionQueryEnumerable : IEnumerable<Document>
    {
        private readonly DocumentDatabase _database;
        private readonly DocumentsStorage _documents;
        private readonly FieldsToFetch _fieldsToFetch;
        private readonly DocumentsOperationContext _context;
        private readonly IncludeDocumentsCommand _includeDocumentsCommand;
        private readonly IncludeRevisionsCommand _includeRevisionsCommand;
        private readonly IncludeCompareExchangeValuesCommand _includeCompareExchangeValuesCommand;
        private readonly Reference<long> _totalResults;
        private readonly Reference<int> _scannedResults;
        private readonly Reference<long> _skippedResults;
        private readonly CancellationToken _token;
        private readonly string _collection;
        private readonly IndexQueryServerSide _query;
        private readonly QueryTimingsScope _queryTimings;
        private readonly bool _isAllDocsCollection;

        public CollectionQueryEnumerable(DocumentDatabase database, DocumentsStorage documents, FieldsToFetch fieldsToFetch, string collection,
            IndexQueryServerSide query, QueryTimingsScope queryTimings, DocumentsOperationContext context, IncludeDocumentsCommand includeDocumentsCommand,
            IncludeRevisionsCommand includeRevisionsCommand, IncludeCompareExchangeValuesCommand includeCompareExchangeValuesCommand, Reference<long> totalResults,
            Reference<int> scannedResults, Reference<long> skippedResults, CancellationToken token)
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
            _includeRevisionsCommand = includeRevisionsCommand;
            _includeCompareExchangeValuesCommand = includeCompareExchangeValuesCommand;
            _totalResults = totalResults;
            _token = token;
            _scannedResults = scannedResults;
            _skippedResults = skippedResults;
        }

        public string StartAfterId { get; set; }

        public Reference<long> AlreadySeenIdsCount { get; set; }

        public DocumentFields Fields { get; set; } = DocumentFields.All;

        public IEnumerator<Document> GetEnumerator()
        {
            return new Enumerator(_database, _documents, _fieldsToFetch, _collection, _isAllDocsCollection, _query,
                _queryTimings, _context, _includeDocumentsCommand, _includeRevisionsCommand, _includeCompareExchangeValuesCommand, _totalResults, _scannedResults, 
                StartAfterId, AlreadySeenIdsCount, Fields, _skippedResults, _token);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public sealed class FilterKey : ScriptRunnerCache.Key
        {
            private readonly QueryMetadata _queryMetadata;

            public FilterKey(QueryMetadata queryMetadata)
            {
                _queryMetadata = queryMetadata;
            }

            public override void GenerateScript(ScriptRunner runner)
            {
                if (_queryMetadata.DeclaredFunctions != null)
                {
                    foreach (var function in _queryMetadata.DeclaredFunctions)
                    {
                        runner.AddScript(function.Value.FunctionText);
                    }
                }

                runner.AddScript(_queryMetadata.FilterScript);
            }

            public override bool Equals(object obj)
            {
                return ReferenceEquals(obj, _queryMetadata);
            }

            public override int GetHashCode()
            {
                return _queryMetadata.GetHashCode();
            }
        }

        private sealed class Enumerator : IEnumerator<Document>
        {
            private readonly DocumentsStorage _documents;
            private readonly FieldsToFetch _fieldsToFetch;
            private readonly DocumentsOperationContext _context;
            private readonly Reference<long> _totalResults;
            private readonly Reference<int> _scannedResults;
            private readonly string _collection;
            private readonly bool _isAllDocsCollection;
            private readonly IndexQueryServerSide _query;
            private readonly QueryTimingsScope _queryTimings;
            private readonly string _startAfterId;
            private readonly Reference<long> _alreadySeenIdsCount;
            private readonly DocumentFields _fields;

            private bool _initialized;

            private int _returnedResults;

            private readonly HashSet<ulong> _alreadySeenProjections;
            private IEnumerator<Document> _inner;
            private bool _hasProjections;
            private List<Document>.Enumerator _projections;
            private readonly List<Slice> _ids;
            private readonly MapQueryResultRetriever _resultsRetriever;
            private readonly string _startsWith;
            private readonly Reference<long> _skippedResults;
            private readonly CancellationToken _token;
            private readonly ScriptRunner.SingleRun _filterScriptRun;
            private ScriptRunner.ReturnRun _releaseFilterScriptRunner;
            private bool _totalResultsCalculated;
            private readonly bool _isCountQuery;

            public Enumerator(DocumentDatabase database, DocumentsStorage documents, FieldsToFetch fieldsToFetch, string collection, bool isAllDocsCollection,
                IndexQueryServerSide query, QueryTimingsScope queryTimings, DocumentsOperationContext context, IncludeDocumentsCommand includeDocumentsCommand,
                IncludeRevisionsCommand includeRevisionsCommand,IncludeCompareExchangeValuesCommand includeCompareExchangeValuesCommand, Reference<long> totalResults, 
                Reference<int> scannedResults, string startAfterId, Reference<long> alreadySeenIdsCount, DocumentFields fields, Reference<long> skippedResults, CancellationToken token)
            {
                _documents = documents;
                _fieldsToFetch = fieldsToFetch;
                _collection = collection;
                _isAllDocsCollection = isAllDocsCollection;
                _query = query;
                _queryTimings = queryTimings;
                _context = context;
                _totalResults = totalResults;
                _scannedResults = scannedResults;
                _totalResults.Value = 0;
                _startAfterId = startAfterId;
                _alreadySeenIdsCount = alreadySeenIdsCount;
                _fields = fields;
                _skippedResults = skippedResults;
                _token = token;
                _isCountQuery = _query.PageSize == 0;
                if (_fieldsToFetch.IsDistinct)
                    _alreadySeenProjections = new HashSet<ulong>();

                _resultsRetriever = new MapQueryResultRetriever(database, query, queryTimings, documents, context, SearchEngineType.None, fieldsToFetch, includeDocumentsCommand, includeCompareExchangeValuesCommand, includeRevisionsCommand);

                (_ids, _startsWith) = query.ExtractIdsFromQuery(database.ServerStore, context.Allocator, database.CompareExchangeStorage);

                if (_query.Metadata.FilterScript != null)
                {
                    var key = new FilterKey(_query.Metadata);
                    _releaseFilterScriptRunner = database.Scripts.GetScriptRunner(key, readOnly: true, patchRun: out _filterScriptRun); 
                }
            }

            public bool MoveNext()
            {
                if (_initialized == false)
                {
                    Initialize(out var processedAllDocuments);
                    
                    // When our query skipped all documents from underlying stream
                    if (processedAllDocuments)
                        return false;
                    
                    ConfigureStreamForUnboundedQueries();
                }

                if (_isCountQuery)
                {
                    CountDocumentsInEnumerator(countQuery: true);
                    return false;
                }
                
                while (_returnedResults < _query.PageSize)
                {
                    var (hasNext, doc) = GetNextDocument();
                    if (doc == null)
                    {
                        if (hasNext == false)
                            return false;
                     
                        _skippedResults.Value++;
                        continue;
                    }

                    if (_query.SkipDuplicateChecking || _fieldsToFetch.IsDistinct == false)
                    {
                        _returnedResults++;
                        Current = doc;
                        return true;
                    }

                    if (_alreadySeenProjections.Add(doc.DataHash))
                    {
                        if (_query.ReturnOptions?.AddDataHashMetadata == true) 
                            doc = doc.EnsureDataHashInQueryResultMetadata();

                        _returnedResults++;
                        Current = doc;
                        return true;
                    }

                    _skippedResults.Value++;
                }

                //In' (or 'equal') is an unbounded query like `startsWith'. However, there are not as many documents to check (since it requires sending list of ids) compared to startsWith
                // where the query can match the whole collection (and more). For this scenario, we'll count the rest of the documents in the underlying enumerator
                // in order to return exact totalResults count.
                if (_ids != null)
                    CountDocumentsInEnumerator(countQuery: false);
                
                return false;
            }

            private void CountDocumentsInEnumerator(bool countQuery)
            {
                // If we know how many documents we have, and we do not have a DISTINCT clause, we can simply return the current value from memory.
                if (_totalResultsCalculated && _query.Metadata.IsDistinct == false && _filterScriptRun == null)
                    return;
                
                // For count(): We need to disable calculating totalResults in the enumerator, so let's set _totalResultsCalculated to true.
                _totalResultsCalculated = true;
                
                if (countQuery)
                    _totalResults.Value = 0;

                while (true)
                {
                    var (hasNext, doc) = GetNextDocument();
                    using (doc)
                    {
                        if (doc == null)
                        {
                            if (hasNext == false)
                                return;
                            _skippedResults.Value++;
                            continue;
                        }

                        if (_query.SkipDuplicateChecking || _fieldsToFetch.IsDistinct == false)
                        {
                            _totalResults.Value++;
                            continue;
                        }

                        if (_alreadySeenProjections.Add(doc.DataHash))
                        {
                            _totalResults.Value++;
                        }
                        else
                        {
                            _skippedResults.Value++; 
                        }
                    }
                    
                    _context.Transaction.ForgetAbout(doc);
                }
            }

            private (bool HasNext, Document Doc) GetNextDocument()
            {
                if (_hasProjections)
                {
                    if (_projections.MoveNext())
                        return (true, _projections.Current);

                    _hasProjections = false;
                    _projections = default;
                }

                Debug.Assert(_inner != null, "_inner != null"); 
                
                if (_inner.MoveNext() == false)
                {
                    Current = null;
                    return (false, null);
                }
                
                if (_filterScriptRun != null)
                {
                    if (_scannedResults.Value == _query.FilterLimit)
                    {
                        return (false, null);
                    }
                    _scannedResults.Value++;
                    object self = _filterScriptRun.Translate(_context, _inner.Current);
                    using (_queryTimings?.For(nameof(QueryTimingsScope.Names.Filter)))
                    using (var result = _filterScriptRun.Run(_context, _context, "execute", _inner.Current!.Id, new[] { self, _query.QueryParameters }, _queryTimings))
                    {
                        if (result.BooleanValue != true)
                            return (true, null);
                    }
                }

                if (_totalResultsCalculated == false)
                    _totalResults.Value++;
                
                if (_fieldsToFetch.IsProjection)
                {
                    RetrieverInput retrieverInput = new(null, QueryResultRetrieverBase.ZeroScore, null);
                    var result = _resultsRetriever.GetProjectionFromDocument(_inner.Current, ref retrieverInput, _fieldsToFetch, _context, _token);
                    if (result.List != null)
                    {
                        var it = result.List.GetEnumerator();
                        if (it.MoveNext() == false)
                            return (true, null);
                        _totalResults.Value += result.List.Count - 1;
                        _projections = it;
                        _hasProjections = true;
                        return (true, it.Current);
                    }

                    if (result.Document != null)
                    {
                        return (true, result.Document);
                    }

                    return (true, null);
                }

                return (true, _inner.Current);
            }

            private IEnumerable<Document> GetDocuments(out bool totalResultsCalculated, long? amountToSkip = null)
            {
                IEnumerable<Document> documents;
                var skip = amountToSkip ?? 0;
                const long takeAll = long.MaxValue; // owner of enumerable decide when to stop, there is no reason to make paging at storage level.
                totalResultsCalculated = false;
                
                if (_startsWith != null)
                {
                    documents = _isAllDocsCollection
                        ? _documents.GetDocumentsStartingWith(_context, _startsWith, null, null, _startAfterId, skip, takeAll, null, fields: _fields, _token) 
                        : _documents.GetDocumentsStartingWith(_context, _startsWith, _startAfterId, skip, takeAll, _collection, _skippedResults,  _fields, _token);
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
                                ? _documents.GetDocuments(_context, ids, 0, takeAll)
                                : _documents.GetDocumentsForCollection(_context, ids, _collection, 0, takeAll);
                        }
                    }
                    else
                    {
                        documents = _isAllDocsCollection
                            ? _documents.GetDocuments(_context, _ids, skip, takeAll)
                            : _documents.GetDocumentsForCollection(_context, _ids, _collection, skip, takeAll);
                        
                    }
                }
                else if (_isAllDocsCollection)
                {
                    documents = _documents.GetDocumentsFrom(_context, 0, skip, takeAll);
                    if (_filterScriptRun == null)
                    {
                        totalResultsCalculated = true;
                        _totalResults.Value = (int)_documents.GetNumberOfDocuments(_context);
                    }
                }
                else
                {
                    documents = _documents.GetDocumentsFrom(_context, _collection, 0, skip, takeAll);
                    if (_filterScriptRun == null)
                    {
                        totalResultsCalculated = true;
                        _totalResults.Value = (int)_documents.GetCollection(_collection, _context).Count;
                    }
                }

                return documents;
            }
            
            private void ConfigureStreamForUnboundedQueries()
            {
                // This is the scenario when we have an unbounded query (which basically means we don't know how many elements are in the underlying enumerable). 
                // We want to avoid materializing just for the correct value of TotalResults and tell Studio that it is unbounded and count it on the front end.
                // To do this, we'll need to disable totalResults incrementation and set totalResults to 'CollectionQueryRunner.UnboundedQueryResultMarker' (0). See more in `CollectionQueryRunner`.
                //However, for count queries we will evaluate it to the end.
                if (_startsWith != null && _isCountQuery == false)
                {
                    _totalResults.Value = CollectionQueryRunner.UnboundedQueryResultMarker;
                    _totalResultsCalculated = true;
                }
            }

            private void Initialize(out bool processedAllDocuments)
            {
                _initialized = true;
                var start = _query.Start;
                var isInSkip = _ids != null && start > 0;
                processedAllDocuments = false;
                
                if (start == 0)
                {
                    _inner = GetDocuments(out _totalResultsCalculated, start).GetEnumerator();
                    return;
                }

                if (_query.SkipDuplicateChecking)
                {
                    _inner = GetDocuments(out _totalResultsCalculated, start).GetEnumerator();
                    return;
                }

                if (isInSkip == false && _fieldsToFetch.IsDistinct == false)
                {
                    _inner = GetDocuments(out _totalResultsCalculated, start).GetEnumerator();
                    return;
                }
                
                _inner = GetDocuments(out _totalResultsCalculated, amountToSkip: 0).GetEnumerator();
                var count = 0;
                while (true)
                {
                    var (hasNextDocument, document) = GetNextDocument();
                    
                    if (document == null && hasNextDocument == false)
                        break;
                    
                    if (IsStartingPoint(document) || hasNextDocument == false)
                    {
                        ReleaseDocument();
                        break;
                    }

                    ReleaseDocument();
                    
                    
                    void ReleaseDocument()
                    {
                        if (document != null)
                        {
                            document.Dispose();
                            _context.Transaction.ForgetAbout(document);
                        }
                    }
                }
                
                bool IsStartingPoint(Document d)
                {
                    count++;
                    
                    if (_fieldsToFetch.IsDistinct && d.Data.Count > 0)
                        _alreadySeenProjections.Add(d.DataHash);
                            
                    // We have to seek to the point where we previously ended.
                    return count == _query.Start;
                }

                if (count < _query.Start)
                    processedAllDocuments = true;
            }

            public void Reset()
            {
                throw new NotSupportedException();
            }

            public Document Current { get; private set; }

            object IEnumerator.Current => Current;

            public void Dispose()
            {

                _releaseFilterScriptRunner.Dispose();
            }

        }
    }
}
