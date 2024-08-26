using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Threading;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Search.Vectorhighlight;
using Lucene.Net.Store;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Queries.Explanation;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Collectors;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Highlightings;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Explanation;
using Raven.Server.Documents.Queries.MoreLikeThis.Lucene;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Sorting.AlphaNumeric;
using Raven.Server.Documents.Queries.Sorting.Custom;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.Exceptions;
using Raven.Server.Indexing;
using Raven.Server.Json;
using Raven.Server.Logging;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;
using Spatial4n.Shapes;
using Voron.Impl;
using Query = Lucene.Net.Search.Query;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public class LuceneIndexReadOperation : IndexReadOperationBase
    {
        private static readonly Sort SortByFieldScore = new Sort(SortField.FIELD_SCORE);
        private readonly IndexType _indexType;
        private readonly bool _indexHasBoostedFields;

        private readonly LuceneRavenPerFieldAnalyzerWrapper _analyzer;
        private readonly IDisposable _releaseReadTransaction;
        private readonly int _maxNumberOfOutputsPerDocument;

        protected readonly IState _state;
        private readonly IDisposable _readLock;

        private FastVectorHighlighter _highlighter;
        private FieldQuery _highlighterQuery;

        protected readonly IndexSearcher _searcher;

        private static readonly LuceneCleaner _luceneCleaner;

        static LuceneIndexReadOperation()
        {
            _luceneCleaner = new LuceneCleaner();
        }

        public LuceneIndexReadOperation(Index index, LuceneVoronDirectory directory,  QueryBuilderFactories queryBuilderFactories, Transaction readTransaction, IndexQueryServerSide query)
            : base(index, RavenLogManager.Instance.GetLoggerForIndex<LuceneIndexReadOperation>(index), queryBuilderFactories, query)
        {
            try
            {
                _analyzer = LuceneIndexingHelpers.CreateLuceneAnalyzer(index, index.Definition, forQuerying: true);
            }
            catch (Exception e)
            {
                throw new IndexAnalyzerException(e);
            }

            _maxNumberOfOutputsPerDocument = index.MaxNumberOfOutputsPerDocument;
            _indexType = index.Type;
            _indexHasBoostedFields = index.HasBoostedFields;
            _releaseReadTransaction = directory.SetTransaction(readTransaction, out _state);
            _searcher = ((LuceneIndexPersistence)_index.IndexPersistence).GetSearcher(readTransaction, _state);
            _readLock = _luceneCleaner.EnterRunningQueryReadLock();
        }

        public override long EntriesCount()
        {
            return Convert.ToInt64(_searcher.IndexReader.NumDocs());
        }

        public override IEnumerable<QueryResult> Query(IndexQueryServerSide query, QueryTimingsScope queryTimings, FieldsToFetch fieldsToFetch, Reference<long> totalResults, Reference<long> skippedResults,
            Reference<long> scannedDocuments, IQueryResultRetriever retriever, DocumentsOperationContext documentsContext, Func<string, SpatialField> getSpatialField, CancellationToken token)
        {
            ExplanationOptions explanationOptions = null;

            // The reason why we keep distincts counts here, is because at the Lucene level there is no sorting unique accesse like we have 
            // in Corax with the SortingMatch. We have to know the page size to account for that. 
            var pageSize = query.PageSize;
            var isDistinctCount = pageSize == 0 && query.Metadata.IsDistinct;
            if (isDistinctCount)
                pageSize = int.MaxValue;
            var position = query.Start;

            if (position > int.MaxValue || pageSize > int.MaxValue)
                ThrowQueryWantToExceedsInt32();

            pageSize = LuceneGetPageSize(_searcher, pageSize);
            var docsToGet = pageSize;


            QueryTimingsScope luceneScope = null;
            QueryTimingsScope highlightingScope = null;
            QueryTimingsScope explanationsScope = null;

            if (queryTimings != null)
            {
                luceneScope = queryTimings.For(nameof(QueryTimingsScope.Names.Lucene), start: false);
                highlightingScope = query.Metadata.HasHighlightings
                    ? queryTimings.For(nameof(QueryTimingsScope.Names.Highlightings), start: false)
                    : null;
                explanationsScope = query.Metadata.HasExplanations
                    ? queryTimings.For(nameof(QueryTimingsScope.Names.Explanations), start: false)
                    : null;
            }

            var returnedResults = 0;

            // We are going to get the actual Lucene query evaluator. 
            var luceneQuery = GetLuceneQuery(documentsContext, query.Metadata, query.QueryParameters, _analyzer, QueryBuilderFactories);

            using (var queryFilter = GetQueryFilter(_index, query, documentsContext, skippedResults, scannedDocuments, retriever, queryTimings))
            using (GetSort(query, _index, getSpatialField, documentsContext, out var sort))
            using (var scope = new LuceneIndexQueryingScope(_indexType, query, fieldsToFetch, _searcher, retriever, _state))
            {
                // Most of the housekeeping work to be done at this level is for keeping track of duplicates, sorting and filtering when necesary. 

                if (query.Metadata.HasHighlightings)
                {
                    // If we have highlightings then we need to setup the Lucene objects that will attach to the evaluator in order
                    // to retrieve the fields and perform the transformations required by Highlightings. 
                    using (highlightingScope?.For(nameof(QueryTimingsScope.Names.Setup)))
                        SetupHighlighter(query, luceneQuery, documentsContext);
                }

                while (true)
                {
                    token.ThrowIfCancellationRequested();

                    // We are going to execute the query (EVAL) and the crawl the results in batches and in-order of score.                    
                    TopDocs search;
                    using (luceneScope?.Start())
                        search = ExecuteQuery(luceneQuery, (int)query.Start, (int)docsToGet, sort);

                    totalResults.Value = search.TotalHits;

                    // We need to filter out search results that had already been seen in previous batches.
                    scope.RecordAlreadyPagedItemsInPreviousPage(search, token);

                    for (; position < search.ScoreDocs.Length && pageSize > 0; position++)
                    {
                        token.ThrowIfCancellationRequested();

                        var scoreDoc = search.ScoreDocs[position];

                        // Retrieve the actual index entry from the Lucene index. 
                        global::Lucene.Net.Documents.Document document;
                        using (luceneScope?.Start())
                            document = _searcher.Doc(scoreDoc.Doc, _state);

                        var retrieverInput = new RetrieverInput(document, scoreDoc, _state);
                        if (retriever.TryGetKeyLucene(ref retrieverInput, out string key) && scope.WillProbablyIncludeInResults(key) == false)
                        {
                            // If either there is no valid projection or we have already seen this document before, we are skipping. 
                            skippedResults.Value++;
                            continue;
                        }
                        // We apply a document scan script if required.                       
                        var filterResult = queryFilter?.Apply(ref retrieverInput, key);
                        if (filterResult is not null and not FilterResult.Accepted)
                        {
                            if (filterResult is FilterResult.Skipped)
                                continue;
                            if (filterResult is FilterResult.LimitReached)
                                break;
                        }

                        // We are going to return the documents to the caller in a streaming fashion.
                        bool markedAsSkipped = false;
                        var r = retriever.Get(ref retrieverInput, token);

                        var parameters = new CreateQueryResultParameters(query, luceneQuery, scoreDoc, document, documentsContext, scope, highlightingScope,
                            explanationsScope, explanationOptions, isDistinctCount);

                        if (r.Document != null)
                        {
                            var qr = CreateQueryResult(r.Document, parameters, ref markedAsSkipped, skippedResults, ref returnedResults);

                            if (qr.Result == null)
                                continue;

                            yield return qr;
                        }
                        else if (r.List != null)
                        {
                            int numberOfProjectedResults = 0;
                            foreach (Document item in r.List)
                            {
                                var qr = CreateQueryResult(item, parameters, ref markedAsSkipped, skippedResults, ref returnedResults);

                                if (qr.Result == null)
                                    continue;

                                yield return qr;
                                numberOfProjectedResults++;
                            }

                            if (numberOfProjectedResults > 1)
                            {
                                totalResults.Value += numberOfProjectedResults - 1;
                            }
                        }
                        else
                        {
                            skippedResults.Value++;
                        }

                        if (returnedResults == pageSize)
                            yield break;
                    }

                    if (search.TotalHits == search.ScoreDocs.Length)
                        break;

                    if (returnedResults >= pageSize || scannedDocuments.Value >= query.FilterLimit)
                        break;

                    Debug.Assert(_maxNumberOfOutputsPerDocument > 0);

                    docsToGet += LuceneGetPageSize(_searcher, (long)(pageSize - returnedResults) * _maxNumberOfOutputsPerDocument);
                }

                if (isDistinctCount)
                    totalResults.Value = returnedResults;
            }
        }

        [DoesNotReturn]
        private static void ThrowQueryWantToExceedsInt32()
        {
            throw new InvalidDataException($"Lucene entries limit is int32 documents. ({int.MaxValue}).");
        }

        protected virtual QueryResult CreateQueryResult(Document doc, CreateQueryResultParameters parameters, ref bool markedAsSkipped, Reference<long> skippedResults, ref int returnedResults)
        {
            // We check again if we are going to be including this document.                             
            if (parameters.QueryingScope.TryIncludeInResults(doc) == false)
            {
                doc?.Dispose();

                if (markedAsSkipped == false)
                {
                    skippedResults.Value++;
                    markedAsSkipped = true;
                }

                return default;
            }

            returnedResults++;

            if (parameters.IsDistinctCount == false)
            {
                // If there are highlightings we retrieve them.
                Dictionary<string, Dictionary<string, string[]>> highlightings = null;
                if (parameters.Query.Metadata.HasHighlightings)
                {
                    using (parameters.HighlightingScope?.Start())
                        highlightings = GetHighlighterResults(parameters.Query, _searcher, parameters.ScoreDoc, doc, parameters.LuceneDocument, parameters.DocumentsContext);
                }

                // If we have been asked for explanations, we get them. 
                ExplanationResult explanation = null;
                if (parameters.Query.Metadata.HasExplanations)
                {
                    using (parameters.ExplanationsScope?.Start())
                    {
                        var explanationOptions = parameters.ExplanationOptions;

                        if (explanationOptions == null)
                            explanationOptions = parameters.Query.Metadata.Explanation.GetOptions(parameters.DocumentsContext, parameters.Query.QueryParameters);

                        explanation = GetQueryExplanations(explanationOptions, parameters.LuceneQuery, _searcher, parameters.ScoreDoc, doc, parameters.LuceneDocument);
                    }
                }

                // We return the document to the caller. 
                return new QueryResult { Result = doc, Highlightings = highlightings, Explanation = explanation };
            }

            return default;
        }

        private ExplanationResult GetQueryExplanations(ExplanationOptions options, Query luceneQuery, IndexSearcher searcher, ScoreDoc scoreDoc, Document document, global::Lucene.Net.Documents.Document luceneDocument)
        {
            string key;
            var hasGroupKey = options != null && string.IsNullOrWhiteSpace(options.GroupKey) == false;
            if (_indexType.IsMapReduce())
            {
                if (hasGroupKey)
                {
                    key = luceneDocument.Get(options.GroupKey, _state);
                    if (key == null && document.Data.TryGet(options.GroupKey, out object value))
                        key = value?.ToString();
                }
                else
                    key = luceneDocument.Get(Constants.Documents.Indexing.Fields.ReduceKeyHashFieldName, _state);
            }
            else
            {
                key = hasGroupKey
                    ? luceneDocument.Get(options.GroupKey, _state)
                    : document.Id;
            }

            return new ExplanationResult
            {
                Key = key,
                Explanation = searcher.Explain(luceneQuery, scoreDoc.Doc, _state)
            };
        }

        private Dictionary<string, Dictionary<string, string[]>> GetHighlighterResults(IndexQueryServerSide query, IndexSearcher searcher, ScoreDoc scoreDoc, Document document, global::Lucene.Net.Documents.Document luceneDocument, JsonOperationContext context)
        {
            Debug.Assert(_highlighter != null);
            Debug.Assert(_highlighterQuery != null);

            var results = new Dictionary<string, Dictionary<string, string[]>>();
            foreach (var highlighting in query.Metadata.Highlightings)
            {
                var fieldName = highlighting.Field.Value;
                var indexFieldName = query.Metadata.IsDynamic
                    ? AutoIndexField.GetSearchAutoIndexFieldName(fieldName)
                    : fieldName;

                var fragments = _highlighter.GetBestFragments(
                    _highlighterQuery,
                    searcher.IndexReader,
                    scoreDoc.Doc,
                    indexFieldName,
                    highlighting.FragmentLength,
                    highlighting.FragmentCount,
                    _state);

                if (fragments == null || fragments.Length == 0)
                    continue;

                var options = highlighting.GetOptions(context, query.QueryParameters);

                string key;
                if (options != null && string.IsNullOrWhiteSpace(options.GroupKey) == false)
                {
                    key = luceneDocument.Get(options.GroupKey, _state);
                }
                else
                {
                    key = document.Id ??
                          // map reduce index
                          luceneDocument.Get(Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName, _state) ??
                          // projection? probably shouldn't happen
                          Guid.NewGuid().ToString();
                }

                if (results.TryGetValue(fieldName, out var result) == false)
                    results[fieldName] = result = new Dictionary<string, string[]>();

                if (result.TryGetValue(key, out var innerResult))
                {
                    Array.Resize(ref innerResult, innerResult.Length + fragments.Length);
                    Array.Copy(fragments, 0, innerResult, innerResult.Length, fragments.Length);
                }
                else
                    result[key] = fragments;
            }

            return results;
        }

        private void SetupHighlighter(IndexQueryServerSide query, Query luceneQuery, JsonOperationContext context)
        {
            var fragmentsBuilder = new PerFieldFragmentsBuilder(query, context);
            _highlighter = new FastVectorHighlighter(
                FastVectorHighlighter.DEFAULT_PHRASE_HIGHLIGHT,
                FastVectorHighlighter.DEFAULT_FIELD_MATCH,
                new SimpleFragListBuilder(),
                fragmentsBuilder);

            _highlighterQuery = _highlighter.GetFieldQuery(luceneQuery);
        }


        public override IEnumerable<QueryResult> IntersectQuery(IndexQueryServerSide query, FieldsToFetch fieldsToFetch, Reference<long> totalResults, Reference<long> skippedResults, Reference<long> scannedDocuments, IQueryResultRetriever retriever, DocumentsOperationContext documentsContext, Func<string, SpatialField> getSpatialField, CancellationToken token)
        {
            var method = query.Metadata.Query.Where as MethodExpression;
            if (query.Start > int.MaxValue || query.PageSize > int.MaxValue)
                ThrowQueryWantToExceedsInt32();

            if (method == null)
                throw new InvalidQueryException($"Invalid intersect query. WHERE clause must contains just an intersect() method call while it got {query.Metadata.Query.Where.Type} expression", query.Metadata.QueryText, query.QueryParameters);

            var methodName = method.Name;

            if (string.Equals("intersect", methodName.Value, StringComparison.OrdinalIgnoreCase) == false)
                throw new InvalidQueryException($"Invalid intersect query. WHERE clause must contains just a single intersect() method call while it got '{methodName}' method", query.Metadata.QueryText, query.QueryParameters);

            if (method.Arguments.Count <= 1)
                throw new InvalidQueryException("The valid intersect query must have multiple intersect clauses.", query.Metadata.QueryText, query.QueryParameters);

            var subQueries = new Query[method.Arguments.Count];

            for (var i = 0; i < subQueries.Length; i++)
            {
                var whereExpression = method.Arguments[i] as QueryExpression;

                if (whereExpression == null)
                    throw new InvalidQueryException($"Invalid intersect query. The intersect clause at position {i} isn't a valid expression", query.Metadata.QueryText, query.QueryParameters);

                subQueries[i] = GetLuceneQuery(documentsContext, query.Metadata, whereExpression, query.QueryParameters, _analyzer, QueryBuilderFactories);
            }

            //Not sure how to select the page size here??? The problem is that only docs in this search can be part
            //of the final result because we're doing an intersection query (but we might exclude some of them)
            var pageSize = LuceneGetPageSize(_searcher, query.PageSize);
            int pageSizeBestGuess = LuceneGetPageSize(_searcher, ((long)query.Start + query.PageSize) * 2);
            int skippedResultsInCurrentLoop = 0;
            int previousBaseQueryMatches = 0;

            var firstSubDocumentQuery = subQueries[0];

            using (var queryFilter = GetQueryFilter(_index, query, documentsContext, skippedResults, scannedDocuments, retriever, null))
            using (GetSort(query, _index, getSpatialField, documentsContext, out var sort))
            using (var scope = new LuceneIndexQueryingScope(_indexType, query, fieldsToFetch, _searcher, retriever, _state))
            {
                //Do the first sub-query in the normal way, so that sorting, filtering etc is accounted for
                var search = ExecuteQuery(firstSubDocumentQuery, 0, pageSizeBestGuess, sort);
                var currentBaseQueryMatches = search.ScoreDocs.Length;
                var intersectionCollector = new IntersectionCollector(_searcher, search.ScoreDocs, _state);

                int intersectMatches;
                do
                {
                    token.ThrowIfCancellationRequested();
                    if (skippedResultsInCurrentLoop > 0)
                    {
                        // We get here because out first attempt didn't get enough docs (after INTERSECTION was calculated)
                        pageSizeBestGuess = pageSizeBestGuess * 2;

                        search = ExecuteQuery(firstSubDocumentQuery, 0, pageSizeBestGuess, sort);
                        previousBaseQueryMatches = currentBaseQueryMatches;
                        currentBaseQueryMatches = search.ScoreDocs.Length;
                        intersectionCollector = new IntersectionCollector(_searcher, search.ScoreDocs, _state);
                    }

                    for (var i = 1; i < subQueries.Length; i++)
                    {
                        _searcher.Search(subQueries[i], null, intersectionCollector, _state);
                    }

                    var currentIntersectResults = intersectionCollector.DocumentsIdsForCount(subQueries.Length).ToList();
                    intersectMatches = currentIntersectResults.Count;
                    skippedResultsInCurrentLoop = pageSizeBestGuess - intersectMatches;
                } while (intersectMatches < pageSize                      //stop if we've got enough results to satisfy the pageSize
                    && currentBaseQueryMatches < search.TotalHits           //stop if increasing the page size wouldn't make any difference
                    && previousBaseQueryMatches < currentBaseQueryMatches); //stop if increasing the page size didn't result in any more "base query" results

                var intersectResults = intersectionCollector.DocumentsIdsForCount(subQueries.Length).ToList();
                //It's hard to know what to do here, the TotalHits from the base search isn't really the TotalSize,
                //because it's before the INTERSECTION has been applied, so only some of those results make it out.
                //Trying to give an accurate answer is going to be too costly, so we aren't going to try.
                totalResults.Value = search.TotalHits;
                skippedResults.Value = skippedResultsInCurrentLoop;

                //Using the final set of results in the intersectionCollector
                int returnedResults = 0;
                for (int i = (int)query.Start; i < intersectResults.Count && (i - query.Start) < pageSizeBestGuess; i++)
                {
                    var indexResult = intersectResults[i];
                    var document = _searcher.Doc(indexResult.LuceneId, _state);

                    var retrieverInput = new RetrieverInput(document, new ScoreDoc(indexResult.LuceneId, indexResult.Score), _state);
                    if (retriever.TryGetKeyLucene(ref retrieverInput, out string key) && scope.WillProbablyIncludeInResults(key) == false)
                    {
                        skippedResults.Value++;
                        skippedResultsInCurrentLoop++;
                        continue;
                    }

                    var filterResult = queryFilter?.Apply(ref retrieverInput, key);
                    if (filterResult is not null and not FilterResult.Accepted)
                    {
                        if (filterResult is FilterResult.Skipped)
                            continue;
                        if (filterResult is FilterResult.LimitReached)
                            break;
                    }

                    var result = retriever.Get(ref retrieverInput, token);

                    if (result.Document != null)
                    {
                        var qr = CreateQueryResult(result.Document);
                        if (qr.Result == null)
                            continue;

                        yield return qr;
                    }
                    else if (result.List != null)
                    {
                        foreach (Document item in result.List)
                        {
                            var qr = CreateQueryResult(item);
                            if (qr.Result == null)
                                continue;

                            yield return qr;
                        }
                    }

                    QueryResult CreateQueryResult(Document d)
                    {
                        if (scope.TryIncludeInResults(d) == false)
                        {
                            d?.Dispose();

                            skippedResults.Value++;
                            skippedResultsInCurrentLoop++;
                            return default;
                        }

                        returnedResults++;

                        return new QueryResult
                        {
                            Result = d
                        };
                    }

                    if (returnedResults == pageSize)
                        yield break;
                }
            }
        }

        private TopDocs ExecuteQuery(Query documentQuery, int start, int pageSize, Sort sort)
        {
            if (sort == null && _indexHasBoostedFields == false && IsBoostedQuery(documentQuery) == false)
            {
                if (pageSize == int.MaxValue || pageSize >= _searcher.MaxDoc) // we want all docs, no sorting required
                {
                    using (var gatherAllCollector = new GatherAllCollector(Math.Min(pageSize, _searcher.MaxDoc)))
                    {
                        _searcher.Search(documentQuery, gatherAllCollector, _state);
                        return gatherAllCollector.ToTopDocs();
                    }
                }

                using (var noSortingCollector = new NonSortingCollector(Math.Abs(pageSize + start)))
                {
                    _searcher.Search(documentQuery, noSortingCollector, _state);
                    return noSortingCollector.ToTopDocs();
                }
            }

            var minPageSize = LuceneGetPageSize(_searcher, (long)pageSize + start);

            if (sort != null)
            {
                _searcher.SetDefaultFieldSortScoring(true, false);
                try
                {
                    return _searcher.Search(documentQuery, null, minPageSize, sort, _state);
                }
                finally
                {
                    _searcher.SetDefaultFieldSortScoring(false, false);
                }
            }

            if (minPageSize <= 0)
            {
                var result = _searcher.Search(documentQuery, null, 1, _state);
                return new TopDocs(result.TotalHits, Array.Empty<ScoreDoc>(), result.MaxScore);
            }
            return _searcher.Search(documentQuery, null, minPageSize, _state);
        }

        private static bool IsBoostedQuery(Query query)
        {
            if (query.Boost > 1)
                return true;

            if (!(query is BooleanQuery booleanQuery))
                return false;

            foreach (var clause in booleanQuery.Clauses)
            {
                if (clause.Query.Boost > 1)
                    return true;
            }

            return false;
        }

        private IDisposable GetSort(IndexQueryServerSide query, Index index, Func<string, SpatialField> getSpatialField, DocumentsOperationContext documentsContext, out Sort sort)
        {
            sort = null;
            if (query.PageSize == 0) // no need to sort when counting only
                return null;

            var orderByFields = query.Metadata.OrderBy;

            if (orderByFields == null)
            {
                if (index.Configuration.OrderByScoreAutomaticallyWhenBoostingIsInvolved == false || query.Metadata.HasBoost == false && index.HasBoostedFields == false)
                    return null;

                AssertCanOrderByScoreAutomaticallyWhenBoostingIsInvolved();
                sort = SortByFieldScore;
                return null;
            }

            int sortIndex = 0;
            var sortArray = new ArraySegment<SortField>(ArrayPool<SortField>.Shared.Rent(orderByFields.Length), sortIndex, orderByFields.Length);

            foreach (var field in orderByFields)
            {
                if (field.OrderingType == OrderByFieldType.Random)
                {
                    string value = null;
                    if (field.Arguments != null && field.Arguments.Length > 0)
                        value = field.Arguments[0].NameOrValue;

                    sortArray[sortIndex++] = new RandomSortField(value);
                    continue;
                }

                if (field.OrderingType == OrderByFieldType.Score)
                {
                    if (field.Ascending)
                        sortArray[sortIndex++] = SortField.FIELD_SCORE;
                    else
                        sortArray[sortIndex++] = new SortField(null, 0, true);
                    continue;
                }

                if (field.OrderingType == OrderByFieldType.Distance)
                {
                    var spatialField = getSpatialField(field.Name);

                    int lastArgument;
                    IPoint point;
                    switch (field.Method)
                    {
                        case MethodType.Spatial_Circle:
                            var cLatitude = field.Arguments[1].GetDouble(query.QueryParameters);
                            var cLongitude = field.Arguments[2].GetDouble(query.QueryParameters);
                            lastArgument = 2;
                            point = spatialField.ReadPoint(cLatitude, cLongitude).Center;
                            break;
                        case MethodType.Spatial_Wkt:
                            var wkt = field.Arguments[0].GetString(query.QueryParameters);
                            SpatialUnits? spatialUnits = null;
                            lastArgument = 1;
                            if (field.Arguments.Length > 1)
                            {
                                spatialUnits = Enum.Parse<SpatialUnits>(field.Arguments[1].GetString(query.QueryParameters), ignoreCase: true);
                                lastArgument = 2;
                            }

                            point = spatialField.ReadShape(wkt, spatialUnits).Center;
                            break;
                        case MethodType.Spatial_Point:
                            var pLatitude = field.Arguments[0].GetDouble(query.QueryParameters);
                            var pLongitude = field.Arguments[1].GetDouble(query.QueryParameters);
                            lastArgument = 2;
                            point = spatialField.ReadPoint(pLatitude, pLongitude).Center;
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }

                    var roundTo = field.Arguments.Length > lastArgument ?
                        field.Arguments[lastArgument].GetDouble(query.QueryParameters)
                        : 0;

                    var dsort = new SpatialDistanceFieldComparatorSource(spatialField, point, query, roundTo);
                    sortArray[sortIndex++] = new SortField(field.Name, dsort, field.Ascending == false);
                    continue;
                }

                var fieldName = field.Name.Value;
                var sortOptions = SortField.STRING;

                switch (field.OrderingType)
                {
                    case OrderByFieldType.Custom:
                        var cName = field.Arguments[0].NameOrValue;
                        var cSort = new CustomComparatorSource(cName, _index.DocumentDatabase.Name, query);
                        sortArray[sortIndex++] = new SortField(fieldName, cSort, field.Ascending == false);
                        continue;
                    case OrderByFieldType.AlphaNumeric:
                        var anSort = new AlphaNumericComparatorSource(documentsContext);
                        sortArray[sortIndex++] = new SortField(fieldName, anSort, field.Ascending == false);
                        continue;
                    case OrderByFieldType.Long:
                        sortOptions = SortField.LONG;
                        fieldName += Constants.Documents.Indexing.Fields.RangeFieldSuffixLong;
                        break;
                    case OrderByFieldType.Double:
                        sortOptions = SortField.DOUBLE;
                        fieldName += Constants.Documents.Indexing.Fields.RangeFieldSuffixDouble;
                        break;
                    case OrderByFieldType.Implicit:
                        if (index.Configuration.OrderByTicksAutomaticallyWhenDatesAreInvolved && index.IndexFieldsPersistence.HasTimeValues(fieldName))
                        {
                            sortOptions = SortField.LONG;
                            fieldName += Constants.Documents.Indexing.Fields.TimeFieldSuffix;
                        }
                        break;
                }

                sortArray[sortIndex++] = new SortField(fieldName, sortOptions, field.Ascending == false);
            }

            sort = new Sort(sortArray);
            return new ReturnSort(sortArray);
        }

        private readonly struct ReturnSort : IDisposable
        {
            private readonly ArraySegment<SortField> _sortArray;

            public ReturnSort(ArraySegment<SortField> sortArray)
            {
                _sortArray = sortArray;
            }

            public void Dispose()
            {
                ArrayPool<SortField>.Shared.Return(_sortArray.Array, clearArray: true);
            }
        }

        public override SortedSet<string> Terms(string field, string fromValue, long pageSize, CancellationToken token)
        {
            var results = new SortedSet<string>(StringComparer.Ordinal);
            using (var termDocs = _searcher.IndexReader.HasDeletions ? _searcher.IndexReader.TermDocs(_state) : null)
            using (var termEnum = _searcher.IndexReader.Terms(new Term(field, fromValue ?? string.Empty), _state))
            {
                if (string.IsNullOrEmpty(fromValue) == false) // need to skip this value
                {
                    while (termEnum.Term == null || fromValue.Equals(termEnum.Term.Text))
                    {
                        token.ThrowIfCancellationRequested();

                        if (termEnum.Next(_state) == false)
                            return results;
                    }
                }
                while (termEnum.Term == null ||
                    field.Equals(termEnum.Term.Field))
                {
                    token.ThrowIfCancellationRequested();

                    if (termEnum.Term != null)
                    {
                        var canAdd = true;
                        if (termDocs != null)
                        {
                            // if we have deletions we need to check
                            // if there are any documents with that term left
                            termDocs.Seek(termEnum.Term, _state);
                            canAdd = termDocs.Next(_state);
                        }

                        if (canAdd)
                            results.Add(termEnum.Term.Text);
                    }

                    if (results.Count >= pageSize)
                        break;

                    if (termEnum.Next(_state) == false)
                        break;
                }
            }

            return results;
        }

        public override IEnumerable<QueryResult> MoreLikeThis(
            IndexQueryServerSide query,
            IQueryResultRetriever retriever,
            DocumentsOperationContext context,
            CancellationToken token)
        {
            IDisposable releaseServerContext = null;
            IDisposable closeServerTransaction = null;
            TransactionOperationContext serverContext = null;
            MoreLikeThisQuery moreLikeThisQuery;

            try
            {
                if (query.Metadata.HasCmpXchg)
                {
                    releaseServerContext = context.DocumentDatabase.ServerStore.ContextPool.AllocateOperationContext(out serverContext);
                    closeServerTransaction = serverContext.OpenReadTransaction();
                }

                using (closeServerTransaction)
                    moreLikeThisQuery = LuceneQueryBuilder.BuildMoreLikeThisQuery(serverContext, context, query.Metadata, _index, query.Metadata.Query.Where, query.QueryParameters, _analyzer, QueryBuilderFactories);
            }
            finally
            {
                releaseServerContext?.Dispose();
            }

            var options = moreLikeThisQuery.Options != null ? JsonDeserializationServer.MoreLikeThisOptions(moreLikeThisQuery.Options) : MoreLikeThisOptions.Default;

            HashSet<string> stopWords = null;
            if (string.IsNullOrWhiteSpace(options.StopWordsDocumentId) == false)
            {
                var stopWordsDoc = context.DocumentDatabase.DocumentsStorage.Get(context, options.StopWordsDocumentId);
                if (stopWordsDoc == null)
                    throw new InvalidOperationException($"Stop words document {options.StopWordsDocumentId} could not be found");

                if (stopWordsDoc.Data.TryGet(nameof(MoreLikeThisStopWords.StopWords), out BlittableJsonReaderArray value) && value != null)
                {
                    stopWords = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    for (var i = 0; i < value.Length; i++)
                        stopWords.Add(value.GetStringByIndex(i));
                }
            }

            var ir = _searcher.IndexReader;
            var mlt = new RavenMoreLikeThis(ir, options, _state);

            int? baseDocId = null;

            if (moreLikeThisQuery.BaseDocument == null)
            {
                var td = _searcher.Search(moreLikeThisQuery.BaseDocumentQuery, 1, _state);

                // get the current Lucene docid for the given RavenDB doc ID
                if (td.ScoreDocs.Length == 0)
                    throw new InvalidOperationException($"Given filtering expression '{query.Query}' did not yield any documents that could be used as a base of comparison");

                baseDocId = td.ScoreDocs[0].Doc;
            }

            if (stopWords != null)
                mlt.SetStopWords(stopWords);

            string[] fieldNames;
            if (options.Fields != null && options.Fields.Length > 0)
                fieldNames = options.Fields;
            else
                fieldNames = ir.GetFieldNames(IndexReader.FieldOption.INDEXED)
                    .Where(x => x != Constants.Documents.Indexing.Fields.DocumentIdFieldName && x != Constants.Documents.Indexing.Fields.SourceDocumentIdFieldName && x != Constants.Documents.Indexing.Fields.ReduceKeyHashFieldName)
                    .ToArray();

            mlt.SetFieldNames(fieldNames);
            mlt.Analyzer = _analyzer;

            var pageSize = LuceneGetPageSize(_searcher, query.PageSize);

            Query mltQuery;
            if (baseDocId.HasValue)
            {
                mltQuery = mlt.Like(baseDocId.Value);
            }
            else
            {
                using (var blittableJson = ParseJsonStringIntoBlittable(moreLikeThisQuery.BaseDocument, context))
                    mltQuery = mlt.Like(blittableJson);
            }

            var tsdc = TopScoreDocCollector.Create(pageSize, true);

            if (moreLikeThisQuery.FilterQuery != null && moreLikeThisQuery.FilterQuery is MatchAllDocsQuery == false)
            {
                mltQuery = new BooleanQuery
                {
                    {mltQuery, Occur.MUST},
                    {moreLikeThisQuery.FilterQuery, Occur.MUST}
                };
            }

            _searcher.Search(mltQuery, tsdc, _state);
            var hits = tsdc.TopDocs().ScoreDocs;

            var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            for (int i = 0; i < hits.Length; i++)
            {
                var hit = hits[i];
                token.ThrowIfCancellationRequested();

                if (hit.Doc == baseDocId)
                    continue;

                var doc = _searcher.Doc(hit.Doc, _state);
                var id = doc.Get(Constants.Documents.Indexing.Fields.DocumentIdFieldName, _state) ?? doc.Get(Constants.Documents.Indexing.Fields.ReduceKeyHashFieldName, _state);
                if (id == null)
                    continue;

                if (ids.Add(id) == false)
                    continue;

                var retrieverInput = new RetrieverInput(doc, hit, _state);
                var result = retriever.Get(ref retrieverInput, token);
                if (result.Document != null)
                {
                    yield return new QueryResult
                    {
                        Result = result.Document
                    };
                }
                else if (result.List != null)
                {
                    foreach (Document item in result.List)
                    {
                        yield return new QueryResult
                        {
                            Result = item
                        };
                    }
                }
            }
        }

        public override IEnumerable<BlittableJsonReaderObject> IndexEntries(IndexQueryServerSide query, Reference<long> totalResults, DocumentsOperationContext documentsContext, Func<string, SpatialField> getSpatialField, bool ignoreLimit, CancellationToken token)
        {
            if (query.PageSize > int.MaxValue || query.Start > int.MaxValue)
                ThrowQueryWantToExceedsInt32();

            var docsToGet = LuceneGetPageSize(_searcher, query.PageSize);
            var position = query.Start;

            var luceneQuery = GetLuceneQuery(documentsContext, query.Metadata, query.QueryParameters, _analyzer, QueryBuilderFactories);
            using (GetSort(query, _index, getSpatialField, documentsContext, out var sort))
            {
                var search = ExecuteQuery(luceneQuery, (int)query.Start, docsToGet, sort);
                var termsDocs = IndexedTerms.ReadAllEntriesFromIndex(_searcher.IndexReader, documentsContext, ignoreLimit, _state);

                totalResults.Value = search.TotalHits;

                for (var index = position; index < search.ScoreDocs.Length; index++)
                {
                    token.ThrowIfCancellationRequested();

                    var scoreDoc = search.ScoreDocs[index];
                    var document = termsDocs[scoreDoc.Doc];

                    yield return document;
                }
            }
        }

        public override IEnumerable<string> DynamicEntriesFields(HashSet<string> staticFields)
        {
            foreach (var fieldName in _searcher
                .IndexReader
                .GetFieldNames(IndexReader.FieldOption.ALL))
            {
                if (staticFields.Contains(fieldName))
                    continue;

                if (fieldName == Constants.Documents.Indexing.Fields.ReduceKeyHashFieldName
                    || fieldName == Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName
                    || fieldName == Constants.Documents.Indexing.Fields.ValueFieldName
                    || fieldName == Constants.Documents.Indexing.Fields.DocumentIdFieldName
                    || fieldName == Constants.Documents.Indexing.Fields.SourceDocumentIdFieldName)
                    continue;

                if (fieldName.EndsWith(LuceneDocumentConverterBase.ConvertToJsonSuffix) ||
                    fieldName.EndsWith(LuceneDocumentConverterBase.IsArrayFieldSuffix) ||
                    fieldName.EndsWith(Constants.Documents.Indexing.Fields.RangeFieldSuffix) ||
                    fieldName.EndsWith(Constants.Documents.Indexing.Fields.TimeFieldSuffix))
                    continue;

                yield return fieldName;
            }
        }

        public override void Dispose()
        {
            using (_readLock)
            {
                base.Dispose();
                _analyzer?.Dispose();
                _releaseReadTransaction?.Dispose();
            }
        }

        protected readonly struct CreateQueryResultParameters
        {
            public readonly IndexQueryServerSide Query;
            public readonly Query LuceneQuery;
            public readonly ScoreDoc ScoreDoc;
            public readonly global::Lucene.Net.Documents.Document LuceneDocument;
            public readonly DocumentsOperationContext DocumentsContext;
            public readonly LuceneIndexQueryingScope QueryingScope;
            public readonly QueryTimingsScope HighlightingScope;
            public readonly QueryTimingsScope ExplanationsScope;
            public readonly ExplanationOptions ExplanationOptions;
            public readonly bool IsDistinctCount;

            public CreateQueryResultParameters(IndexQueryServerSide query, Query luceneQuery, ScoreDoc scoreDoc,
                global::Lucene.Net.Documents.Document luceneDocument, DocumentsOperationContext documentsContext,
                LuceneIndexQueryingScope queryingScope, QueryTimingsScope highlightingScope, QueryTimingsScope explanationsScope, ExplanationOptions explanationOptions,
                bool isDistinctCount)
            {
                Query = query;
                LuceneQuery = luceneQuery;
                ScoreDoc = scoreDoc;
                LuceneDocument = luceneDocument;
                DocumentsContext = documentsContext;
                QueryingScope = queryingScope;
                HighlightingScope = highlightingScope;
                ExplanationsScope = explanationsScope;
                ExplanationOptions = explanationOptions;
                IsDistinctCount = isDistinctCount;
            }
        }
    }
}
