using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Search.Similar;
using Lucene.Net.Store;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Collectors;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.MoreLikeThis;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Sorting.AlphaNumeric;
using Raven.Server.Exceptions;
using Raven.Server.Indexing;
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
        private readonly IndexType _indexType;
        private readonly bool _indexHasBoostedFields;

        private readonly IndexSearcher _searcher;
        private readonly RavenPerFieldAnalyzerWrapper _analyzer;
        private readonly IDisposable _releaseSearcher;
        private readonly IDisposable _releaseReadTransaction;
        private readonly int _maxNumberOfOutputsPerDocument;

        private readonly IState _state;

        public IndexReadOperation(Index index, LuceneVoronDirectory directory, IndexSearcherHolder searcherHolder, Transaction readTransaction)
            : base(index, LoggingSource.Instance.GetLogger<IndexReadOperation>(index._indexStorage.DocumentDatabase.Name))
        {
            try
            {
                _analyzer = CreateAnalyzer(() => new LowerCaseKeywordAnalyzer(), index.Definition.IndexFields, forQuerying: true);
            }
            catch (Exception e)
            {
                throw new IndexAnalyzerException(e);
            }
            
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

        public IEnumerable<Document> Query(IndexQueryServerSide query, FieldsToFetch fieldsToFetch, Reference<int> totalResults, Reference<int> skippedResults, IQueryResultRetriever retriever, JsonOperationContext documentsContext, Func<string, SpatialField> getSpatialField, CancellationToken token)
        {
            var pageSize = GetPageSize(_searcher, query.PageSize);
            var docsToGet = pageSize;
            var position = query.Start;

            var luceneQuery = GetLuceneQuery(documentsContext, query.Metadata, query.QueryParameters, _analyzer, getSpatialField);
            var sort = GetSort(query, getSpatialField);
            var returnedResults = 0;

            using (var scope = new IndexQueryingScope(_indexType, query, fieldsToFetch, _searcher, retriever, _state))
            {
                while (true)
                {
                    token.ThrowIfCancellationRequested();

                    var search = ExecuteQuery(luceneQuery, query.Start, docsToGet, sort);

                    totalResults.Value = search.TotalHits;

                    scope.RecordAlreadyPagedItemsInPreviousPage(search);

                    for (; position < search.ScoreDocs.Length && pageSize > 0; position++)
                    {
                        token.ThrowIfCancellationRequested();

                        var scoreDoc = search.ScoreDocs[position];
                        var document = _searcher.Doc(scoreDoc.Doc, _state);

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
                        yield return result;

                        if (returnedResults == pageSize)
                            yield break;
                    }

                    docsToGet += GetPageSize(_searcher, (long)(pageSize - returnedResults) * _maxNumberOfOutputsPerDocument);
                    if (search.TotalHits == search.ScoreDocs.Length)
                        break;

                    if (returnedResults >= pageSize)
                        break;
                }
            }
        }

        public IEnumerable<Document> IntersectQuery(IndexQueryServerSide query, FieldsToFetch fieldsToFetch, Reference<int> totalResults, Reference<int> skippedResults, IQueryResultRetriever retriever, JsonOperationContext documentsContext, Func<string, SpatialField> getSpatialField, CancellationToken token)
        {
            var method = query.Metadata.Query.Where as MethodExpression;
            
            if (method  == null)
                throw new InvalidQueryException($"Invalid intersect query. WHERE clause must contains just an intersect() method call while it got {query.Metadata.Query.Where.Type} expression", query.Metadata.QueryText, query.QueryParameters);

            var methodName = method.Name;

            if (string.Equals("intersect", methodName) == false)
                throw new InvalidQueryException($"Invalid intersect query. WHERE clause must contains just a single intersect() method call while it got '{methodName}' method", query.Metadata.QueryText, query.QueryParameters);

            if (method.Arguments.Count <= 1)
                throw new InvalidQueryException("The valid intersect query must have multiple intersect clauses.", query.Metadata.QueryText, query.QueryParameters);

            var subQueries = new Query[method.Arguments.Count];

            for (var i = 0; i < subQueries.Length; i++)
            {
                var whereExpression = method.Arguments[i] as QueryExpression;

                if (whereExpression == null)
                    throw new InvalidQueryException($"Invalid intersect query. The intersect clause at position {i} isn't a valid expression", query.Metadata.QueryText, query.QueryParameters);

                subQueries[i] = GetLuceneQuery(documentsContext, query.Metadata, whereExpression, query.QueryParameters, _analyzer, getSpatialField);
            }

            //Not sure how to select the page size here??? The problem is that only docs in this search can be part 
            //of the final result because we're doing an intersection query (but we might exclude some of them)
            var pageSize = GetPageSize(_searcher, query.PageSize);
            int pageSizeBestGuess = GetPageSize(_searcher, ((long)query.Start + query.PageSize) * 2);
            int skippedResultsInCurrentLoop = 0;
            int previousBaseQueryMatches = 0;

            var firstSubDocumentQuery = subQueries[0];
            var sort = GetSort(query, getSpatialField);

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
                    yield return result;
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
                    var gatherAllCollector = new GatherAllCollector(Math.Min(pageSize, _searcher.MaxDoc));
                    _searcher.Search(documentQuery, gatherAllCollector, _state);
                    return gatherAllCollector.ToTopDocs();
                }

                var noSortingCollector = new NonSortingCollector(Math.Abs(pageSize + start));

                _searcher.Search(documentQuery, noSortingCollector, _state);

                return noSortingCollector.ToTopDocs();
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

            return _searcher.Search(documentQuery, null, minPageSize, _state);
        }

        private static bool IsBoostedQuery(Query query)
        {
            if (query.Boost > 1)
                return true;

            BooleanQuery booleanQuery = query as BooleanQuery;

            if (booleanQuery == null)
                return false;

            foreach (var clause in booleanQuery.Clauses)
            {
                if (clause.Query.Boost > 1)
                    return true;
            }

            return false;
        }

        private static Sort GetSort(IndexQueryServerSide query, Func<string, SpatialField> getSpatialField)
        {
            var orderByFields = query.Metadata.OrderBy;

            if (orderByFields == null)
                return null;

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
                    sort.Add(SortField.FIELD_SCORE);
                    continue;
                }

                if (field.OrderingType == OrderByFieldType.Distance)
                {
                    var spatialField = getSpatialField(field.Name);

                    Point point;
                    switch (field.Method)
                    {
                        case MethodType.Circle:
                            var cLatitude = field.Arguments[1].GetDouble(query.QueryParameters);
                            var cLongitude = field.Arguments[2].GetDouble(query.QueryParameters);

                            point = spatialField.ReadPoint(cLatitude, cLongitude).GetCenter();
                            break;
                        case MethodType.Wkt:
                            var wkt = field.Arguments[0].GetString(query.QueryParameters);
                            SpatialUnits? spatialUnits = null;
                            if (field.Arguments.Length == 2)
                                spatialUnits = Enum.Parse<SpatialUnits>(field.Arguments[1].GetString(query.QueryParameters), ignoreCase: true);

                            point = spatialField.ReadShape(wkt, spatialUnits).GetCenter();
                            break;
                        case MethodType.Point:
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
                    case OrderByFieldType.AlphaNumeric:
                        var anSort = new AlphaNumericComparatorSource();
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

        public IEnumerable<Document> MoreLikeThis(
            MoreLikeThisQueryServerSide query, 
            HashSet<string> stopWords, 
            Func<SelectField[], IQueryResultRetriever> createRetriever, 
            JsonOperationContext context, 
            Func<string, SpatialField> getSpatialField, CancellationToken token)
        {
            int? baseDocId = null;
            if (string.IsNullOrWhiteSpace(query.DocumentId) == false || query.MapGroupFields.Count > 0)
            {
                var documentQuery = new BooleanQuery();

                if (string.IsNullOrWhiteSpace(query.DocumentId) == false)
                    documentQuery.Add(new TermQuery(new Term(Constants.Documents.Indexing.Fields.DocumentIdFieldName, query.DocumentId.ToLowerInvariant())), Occur.MUST);

                foreach (var key in query.MapGroupFields.Keys)
                    documentQuery.Add(new TermQuery(new Term(key, query.MapGroupFields[key])), Occur.MUST);

                var td = _searcher.Search(documentQuery, 1, _state);

                // get the current Lucene docid for the given RavenDB doc ID
                if (td.ScoreDocs.Length == 0)
                    throw new InvalidOperationException("Document " + query.DocumentId + " could not be found");

                baseDocId = td.ScoreDocs[0].Doc;
            }

            var ir = _searcher.IndexReader;
            var mlt = new RavenMoreLikeThis(ir, query, _state);

            AssignParameters(mlt, query);

            if (stopWords != null)
                mlt.SetStopWords(stopWords);

            string[] fieldNames;
            if (query.Fields != null && query.Fields.Length > 0)
                fieldNames = query.Fields;
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
                using (var blittableJson = ParseJsonStringIntoBlittable(query.Document, context))
                    mltQuery = mlt.Like(blittableJson);
            }

            var tsdc = TopScoreDocCollector.Create(pageSize, true);

            if (query.Metadata.WhereFields.Count > 0)
            {
                var additionalQuery = QueryBuilder.BuildQuery(context, query.Metadata, query.Metadata.Query.Where, null, _analyzer, _index.GetQueryBuilderFactories());

                mltQuery = new BooleanQuery
                    {
                        {mltQuery, Occur.MUST},
                        {additionalQuery, Occur.MUST}
                    };
            }

            _searcher.Search(mltQuery, tsdc, _state);
            var hits = tsdc.TopDocs().ScoreDocs;

            var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            var retriever = createRetriever(null);

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

                yield return retriever.Get(doc, hit.Score, _state);
            }
        }

        private static void AssignParameters(MoreLikeThis mlt, MoreLikeThisQueryServerSide parameters)
        {
            if (parameters.Boost != null)
                mlt.Boost = parameters.Boost.Value;
            if (parameters.BoostFactor != null)
                mlt.BoostFactor = parameters.BoostFactor.Value;
            if (parameters.MaximumNumberOfTokensParsed != null)
                mlt.MaxNumTokensParsed = parameters.MaximumNumberOfTokensParsed.Value;
            if (parameters.MaximumQueryTerms != null)
                mlt.MaxQueryTerms = parameters.MaximumQueryTerms.Value;
            if (parameters.MinimumWordLength != null)
                mlt.MinWordLen = parameters.MinimumWordLength.Value;
            if (parameters.MaximumWordLength != null)
                mlt.MaxWordLen = parameters.MaximumWordLength.Value;
            if (parameters.MinimumTermFrequency != null)
                mlt.MinTermFreq = parameters.MinimumTermFrequency.Value;
            if (parameters.MinimumDocumentFrequency != null)
                mlt.MinDocFreq = parameters.MinimumDocumentFrequency.Value;
            if (parameters.MaximumDocumentFrequency != null)
                mlt.MaxDocFreq = parameters.MaximumDocumentFrequency.Value;
            if (parameters.MaximumDocumentFrequencyPercentage != null)
                mlt.SetMaxDocFreqPct(parameters.MaximumDocumentFrequencyPercentage.Value);
        }

        public IEnumerable<BlittableJsonReaderObject> IndexEntries(IndexQueryServerSide query, Reference<int> totalResults, DocumentsOperationContext documentsContext, Func<string, SpatialField> getSpatialField, CancellationToken token)
        {
            var docsToGet = GetPageSize(_searcher, query.PageSize);
            var position = query.Start;

            var luceneQuery = GetLuceneQuery(documentsContext, query.Metadata, query.QueryParameters, _analyzer, getSpatialField);
            var sort = GetSort(query, getSpatialField);

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

        public override void Dispose()
        {
            _analyzer?.Dispose();
            _releaseSearcher?.Dispose();
            _releaseReadTransaction?.Dispose();
        }

        private unsafe BlittableJsonReaderObject ParseJsonStringIntoBlittable(string json, JsonOperationContext context)
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
