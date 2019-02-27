using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
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
using Raven.Server.Documents.Queries.MoreLikeThis;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Sorting.AlphaNumeric;
using Raven.Server.Documents.Queries.Sorting.Custom;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.Exceptions;
using Raven.Server.Indexing;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;
using Spatial4n.Core.Shapes;
using Voron.Impl;
using Query = Lucene.Net.Search.Query;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public sealed class IndexReadOperation : IndexOperationBase
    {
        private readonly QueryBuilderFactories _queryBuilderFactories;
        private readonly IndexType _indexType;
        private readonly bool _indexHasBoostedFields;

        private readonly IndexSearcher _searcher;
        private readonly RavenPerFieldAnalyzerWrapper _analyzer;
        private readonly IDisposable _releaseSearcher;
        private readonly IDisposable _releaseReadTransaction;
        private readonly int _maxNumberOfOutputsPerDocument;

        private readonly IState _state;

        private FastVectorHighlighter _highlighter;
        private FieldQuery _highlighterQuery;

        public IndexReadOperation(Index index, LuceneVoronDirectory directory, IndexSearcherHolder searcherHolder, QueryBuilderFactories queryBuilderFactories, Transaction readTransaction)
            : base(index, LoggingSource.Instance.GetLogger<IndexReadOperation>(index._indexStorage.DocumentDatabase.Name))
        {
            try
            {
                _analyzer = CreateAnalyzer(() => new LowerCaseKeywordAnalyzer(), index.Definition, forQuerying: true);
            }
            catch (Exception e)
            {
                throw new IndexAnalyzerException(e);
            }

            _queryBuilderFactories = queryBuilderFactories;
            _maxNumberOfOutputsPerDocument = index.MaxNumberOfOutputsPerDocument;
            _indexType = index.Type;
            _indexHasBoostedFields = index.HasBoostedFields;
            _releaseReadTransaction = directory.SetTransaction(readTransaction, out _state);
            _releaseSearcher = searcherHolder.GetSearcher(readTransaction, _state, out _searcher);
        }

        public int EntriesCount()
        {
            return _searcher.IndexReader.NumDocs();
        }


        public IEnumerable<(Document Result, Dictionary<string, Dictionary<string, string[]>> Highlightings, ExplanationResult Explanation)> Query(IndexQueryServerSide query, QueryTimingsScope queryTimings, FieldsToFetch fieldsToFetch, Reference<int> totalResults, Reference<int> skippedResults, IQueryResultRetriever retriever, DocumentsOperationContext documentsContext, Func<string, SpatialField> getSpatialField, CancellationToken token)
        {
            ExplanationOptions explanationOptions = null;

            var pageSize = query.PageSize;
            var isDistinctCount = pageSize == 0 && query.Metadata.IsDistinct;
            if (isDistinctCount)
                pageSize = int.MaxValue;

            pageSize = GetPageSize(_searcher, pageSize);

            var docsToGet = pageSize;
            var position = query.Start;

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

            var luceneQuery = GetLuceneQuery(documentsContext, query.Metadata, query.QueryParameters, _analyzer, _queryBuilderFactories);
            var sort = GetSort(query, _index, getSpatialField, documentsContext);

            using (var scope = new IndexQueryingScope(_indexType, query, fieldsToFetch, _searcher, retriever, _state))
            {
                if (query.Metadata.HasHighlightings)
                {
                    using (highlightingScope?.For(nameof(QueryTimingsScope.Names.Setup)))
                        SetupHighlighter(query, luceneQuery, documentsContext);
                }

                while (true)
                {
                    token.ThrowIfCancellationRequested();

                    TopDocs search;
                    using (luceneScope?.Start())
                        search = ExecuteQuery(luceneQuery, query.Start, docsToGet, sort);

                    totalResults.Value = search.TotalHits;

                    scope.RecordAlreadyPagedItemsInPreviousPage(search);

                    for (; position < search.ScoreDocs.Length && pageSize > 0; position++)
                    {
                        token.ThrowIfCancellationRequested();

                        var scoreDoc = search.ScoreDocs[position];

                        global::Lucene.Net.Documents.Document document;
                        using (luceneScope?.Start())
                            document = _searcher.Doc(scoreDoc.Doc, _state);

                        if (retriever.TryGetKey(document, _state, out string key) && scope.WillProbablyIncludeInResults(key) == false)
                        {
                            skippedResults.Value++;
                            continue;
                        }

                        var result = retriever.Get(document, scoreDoc.Score, _state);
                        if (scope.TryIncludeInResults(result) == false)
                        {
                            skippedResults.Value++;
                            continue;
                        }

                        returnedResults++;

                        if (isDistinctCount == false)
                        {
                            Dictionary<string, Dictionary<string, string[]>> highlightings = null;
                            if (query.Metadata.HasHighlightings)
                            {
                                using (highlightingScope?.Start())
                                    highlightings = GetHighlighterResults(query, _searcher, scoreDoc, result, document, documentsContext);
                            }

                            ExplanationResult explanation = null;
                            if (query.Metadata.HasExplanations)
                            {
                                using (explanationsScope?.Start())
                                {
                                    if (explanationOptions == null)
                                        explanationOptions = query.Metadata.Explanation.GetOptions(documentsContext, query.QueryParameters);

                                    explanation = GetQueryExplanations(explanationOptions, luceneQuery, _searcher, scoreDoc, result, document);
                                }
                            }

                            yield return (result, highlightings, explanation);
                        }

                        if (returnedResults == pageSize)
                            yield break;
                    }

                    if (search.TotalHits == search.ScoreDocs.Length)
                        break;

                    if (returnedResults >= pageSize)
                        break;

                    Debug.Assert(_maxNumberOfOutputsPerDocument > 0);

                    docsToGet += GetPageSize(_searcher, (long)(pageSize - returnedResults) * _maxNumberOfOutputsPerDocument);
                }

                if (isDistinctCount)
                    totalResults.Value = returnedResults;
            }
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
                    key = luceneDocument.Get(options.GroupKey, _state);
                else
                    key = document.Id;

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

        public IEnumerable<(Document Result, Dictionary<string, Dictionary<string, string[]>> Highlightings, ExplanationResult Explanation)> IntersectQuery(IndexQueryServerSide query, FieldsToFetch fieldsToFetch, Reference<int> totalResults, Reference<int> skippedResults, IQueryResultRetriever retriever, DocumentsOperationContext documentsContext, Func<string, SpatialField> getSpatialField, CancellationToken token)
        {
            var method = query.Metadata.Query.Where as MethodExpression;

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

                subQueries[i] = GetLuceneQuery(documentsContext, query.Metadata, whereExpression, query.QueryParameters, _analyzer, _queryBuilderFactories);
            }

            //Not sure how to select the page size here??? The problem is that only docs in this search can be part 
            //of the final result because we're doing an intersection query (but we might exclude some of them)
            var pageSize = GetPageSize(_searcher, query.PageSize);
            int pageSizeBestGuess = GetPageSize(_searcher, ((long)query.Start + query.PageSize) * 2);
            int skippedResultsInCurrentLoop = 0;
            int previousBaseQueryMatches = 0;

            var firstSubDocumentQuery = subQueries[0];
            var sort = GetSort(query, _index, getSpatialField, documentsContext);

            using (var scope = new IndexQueryingScope(_indexType, query, fieldsToFetch, _searcher, retriever, _state))
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
                for (int i = query.Start; i < intersectResults.Count && (i - query.Start) < pageSizeBestGuess; i++)
                {
                    var indexResult = intersectResults[i];
                    var document = _searcher.Doc(indexResult.LuceneId, _state);

                    if (retriever.TryGetKey(document, _state, out string key) && scope.WillProbablyIncludeInResults(key) == false)
                    {
                        skippedResults.Value++;
                        skippedResultsInCurrentLoop++;
                        continue;
                    }

                    var result = retriever.Get(document, indexResult.Score, _state);
                    if (scope.TryIncludeInResults(result) == false)
                    {
                        skippedResults.Value++;
                        skippedResultsInCurrentLoop++;
                        continue;
                    }

                    returnedResults++;

                    yield return (result, null, null);

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

            var minPageSize = GetPageSize(_searcher, (long)pageSize + start);

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

        private Sort GetSort(IndexQueryServerSide query, Index index, Func<string, SpatialField> getSpatialField, DocumentsOperationContext documentsContext)
        {
            if (query.PageSize == 0) // no need to sort when counting only
                return null;

            var orderByFields = query.Metadata.OrderBy;

            if (orderByFields == null)
            {
                if (query.Metadata.HasBoost == false && index.HasBoostedFields == false)
                    return null;

                return new Sort(SortField.FIELD_SCORE);
            }

            var sort = new List<SortField>();

            foreach (var field in orderByFields)
            {
                if (field.OrderingType == OrderByFieldType.Random)
                {
                    string value = null;
                    if (field.Arguments != null && field.Arguments.Length > 0)
                        value = field.Arguments[0].NameOrValue;

                    sort.Add(new RandomSortField(value));
                    continue;
                }

                if (field.OrderingType == OrderByFieldType.Score)
                {
                    if (field.Ascending)
                        sort.Add(SortField.FIELD_SCORE);
                    else
                        sort.Add(new SortField(null, 0, true));
                    continue;
                }

                if (field.OrderingType == OrderByFieldType.Distance)
                {
                    var spatialField = getSpatialField(field.Name);

                    Point point;
                    switch (field.Method)
                    {
                        case MethodType.Spatial_Circle:
                            var cLatitude = field.Arguments[1].GetDouble(query.QueryParameters);
                            var cLongitude = field.Arguments[2].GetDouble(query.QueryParameters);

                            point = spatialField.ReadPoint(cLatitude, cLongitude).GetCenter();
                            break;
                        case MethodType.Spatial_Wkt:
                            var wkt = field.Arguments[0].GetString(query.QueryParameters);
                            SpatialUnits? spatialUnits = null;
                            if (field.Arguments.Length == 2)
                                spatialUnits = Enum.Parse<SpatialUnits>(field.Arguments[1].GetString(query.QueryParameters), ignoreCase: true);

                            point = spatialField.ReadShape(wkt, spatialUnits).GetCenter();
                            break;
                        case MethodType.Spatial_Point:
                            var pLatitude = field.Arguments[0].GetDouble(query.QueryParameters);
                            var pLongitude = field.Arguments[1].GetDouble(query.QueryParameters);

                            point = spatialField.ReadPoint(pLatitude, pLongitude).GetCenter();
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }

                    var dsort = new SpatialDistanceFieldComparatorSource(spatialField, point);
                    sort.Add(new SortField(field.Name, dsort, field.Ascending == false));
                    continue;
                }

                var fieldName = field.Name.Value;
                var sortOptions = SortField.STRING;

                switch (field.OrderingType)
                {
                    case OrderByFieldType.Custom:
                        var cName = field.Arguments[0].NameOrValue;
                        var cSort = new CustomComparatorSource(cName, _index.DocumentDatabase.Name, query);
                        sort.Add(new SortField(fieldName, cSort, field.Ascending == false));
                        continue;
                    case OrderByFieldType.AlphaNumeric:
                        var anSort = new AlphaNumericComparatorSource(documentsContext);
                        sort.Add(new SortField(fieldName, anSort, field.Ascending == false));
                        continue;
                    case OrderByFieldType.Long:
                        sortOptions = SortField.LONG;
                        fieldName = fieldName + Constants.Documents.Indexing.Fields.RangeFieldSuffixLong;
                        break;
                    case OrderByFieldType.Double:
                        sortOptions = SortField.DOUBLE;
                        fieldName = fieldName + Constants.Documents.Indexing.Fields.RangeFieldSuffixDouble;
                        break;
                }

                sort.Add(new SortField(fieldName, sortOptions, field.Ascending == false));
            }

            return new Sort(sort.ToArray());
        }

        public HashSet<string> Terms(string field, string fromValue, int pageSize, CancellationToken token)
        {
            var results = new HashSet<string>();
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
                        results.Add(termEnum.Term.Text);

                    if (results.Count >= pageSize)
                        break;

                    if (termEnum.Next(_state) == false)
                        break;
                }
            }

            return results;
        }

        public IEnumerable<(Document Result, Dictionary<string, Dictionary<string, string[]>> Highlightings, ExplanationResult Explanation)> MoreLikeThis(
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
                    moreLikeThisQuery = QueryBuilder.BuildMoreLikeThisQuery(serverContext, context, query.Metadata, query.Metadata.Query.Where, query.QueryParameters, _analyzer, _queryBuilderFactories);
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
                    throw new InvalidOperationException("Given filtering expression did not yield any documents that could be used as a base of comparison");

                baseDocId = td.ScoreDocs[0].Doc;
            }

            if (stopWords != null)
                mlt.SetStopWords(stopWords);

            string[] fieldNames;
            if (options.Fields != null && options.Fields.Length > 0)
                fieldNames = options.Fields;
            else
                fieldNames = ir.GetFieldNames(IndexReader.FieldOption.INDEXED)
                    .Where(x => x != Constants.Documents.Indexing.Fields.DocumentIdFieldName && x != Constants.Documents.Indexing.Fields.ReduceKeyHashFieldName)
                    .ToArray();

            mlt.SetFieldNames(fieldNames);
            mlt.Analyzer = _analyzer;

            var pageSize = GetPageSize(_searcher, query.PageSize);

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

            foreach (var hit in hits)
            {
                if (hit.Doc == baseDocId)
                    continue;

                var doc = _searcher.Doc(hit.Doc, _state);
                var id = doc.Get(Constants.Documents.Indexing.Fields.DocumentIdFieldName, _state) ?? doc.Get(Constants.Documents.Indexing.Fields.ReduceKeyHashFieldName, _state);
                if (id == null)
                    continue;

                if (ids.Add(id) == false)
                    continue;

                yield return (retriever.Get(doc, hit.Score, _state), null, null);
            }
        }

        public IEnumerable<BlittableJsonReaderObject> IndexEntries(DocumentsOperationContext context, IndexQueryServerSide query, Reference<int> totalResults, DocumentsOperationContext documentsContext, Func<string, SpatialField> getSpatialField, CancellationToken token)
        {
            var docsToGet = GetPageSize(_searcher, query.PageSize);
            var position = query.Start;

            var luceneQuery = GetLuceneQuery(context, query.Metadata, query.QueryParameters, _analyzer, _queryBuilderFactories);
            var sort = GetSort(query, _index, getSpatialField, documentsContext);

            var search = ExecuteQuery(luceneQuery, query.Start, docsToGet, sort);
            var termsDocs = IndexedTerms.ReadAllEntriesFromIndex(_searcher.IndexReader, documentsContext, _state);

            totalResults.Value = search.TotalHits;

            for (var index = position; index < search.ScoreDocs.Length; index++)
            {
                token.ThrowIfCancellationRequested();

                var scoreDoc = search.ScoreDocs[index];
                var document = termsDocs[scoreDoc.Doc];

                yield return document;
            }
        }

        public IEnumerable<string> DynamicEntriesFields(HashSet<string> staticFields)
        {
            foreach (var fieldName in _searcher
                .IndexReader
                .GetFieldNames(IndexReader.FieldOption.ALL))
            {
                if (staticFields.Contains(fieldName))
                    continue;

                if (fieldName == Constants.Documents.Indexing.Fields.ReduceKeyHashFieldName
                    || fieldName == Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName
                    || fieldName == Constants.Documents.Indexing.Fields.DocumentIdFieldName)
                    continue;

                if (fieldName.EndsWith(LuceneDocumentConverterBase.ConvertToJsonSuffix) ||
                    fieldName.EndsWith(LuceneDocumentConverterBase.IsArrayFieldSuffix) ||
                    fieldName.EndsWith(Constants.Documents.Indexing.Fields.RangeFieldSuffix))
                    continue;

                yield return fieldName;
            }
        }

        public override void Dispose()
        {
            _analyzer?.Dispose();
            _releaseSearcher?.Dispose();
            _releaseReadTransaction?.Dispose();
        }

        internal static unsafe BlittableJsonReaderObject ParseJsonStringIntoBlittable(string json, JsonOperationContext context)
        {
            var bytes = Encoding.UTF8.GetBytes(json);
            fixed (byte* ptr = bytes)
            {
                var blittableJson = context.ParseBuffer(ptr, bytes.Length, "MoreLikeThis/ExtractTermsFromJson", BlittableJsonDocumentBuilder.UsageMode.None);
                blittableJson.BlittableValidation(); //precaution, needed because this is user input..                
                return blittableJson;
            }
        }
    }
}
