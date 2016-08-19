using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;

using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Logging;
using Raven.Client.Data;
using Raven.Client.Data.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Collectors;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.MoreLikeThis;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Sorting;
using Raven.Server.Documents.Queries.Sorting.AlphaNumeric;
using Raven.Server.Indexing;
using Sparrow.Logging;
using Voron.Impl;

using Constants = Raven.Abstractions.Data.Constants;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public class IndexReadOperation : IndexOperationBase
    {
        private static readonly string[] IntersectSeparators = { Constants.IntersectSeparator };

        private const string _Range = "_Range";

        private static Logger _logger;
        private static readonly CompareInfo InvariantCompare = CultureInfo.InvariantCulture.CompareInfo;

        private readonly string _indexName;

        private readonly IndexType _indexType;
        private readonly int? _actualMaxIndexOutputsPerDocument;
        private readonly int _maxIndexOutputsPerDocument;

        private readonly IndexSearcher _searcher;
        private readonly RavenPerFieldAnalyzerWrapper _analyzer;
        private readonly IDisposable _releaseSearcher;
        private readonly IDisposable _releaseReadTransaction;

        public IndexReadOperation(string indexName, IndexType indexType,
            int maxIndexOutputsPerDocument, int? actualMaxIndexOutputsPerDocument,
            Dictionary<string, IndexField> fields, LuceneVoronDirectory directory,
            IndexSearcherHolder searcherHolder, Transaction readTransaction, DocumentDatabase documentDatabase)
        {
            _analyzer = CreateAnalyzer(() => new LowerCaseKeywordAnalyzer(), fields, forQuerying: true);
            _indexName = indexName;
            _indexType = indexType;
            _actualMaxIndexOutputsPerDocument = actualMaxIndexOutputsPerDocument;
            _maxIndexOutputsPerDocument = maxIndexOutputsPerDocument;
            _releaseReadTransaction = directory.SetTransaction(readTransaction);
            _releaseSearcher = searcherHolder.GetSearcher(out _searcher, documentDatabase);
            _logger = LoggerSetup.Instance.GetLogger<IndexReadOperation>(documentDatabase.Name);
        }

        public int EntriesCount()
        {
            return _searcher.IndexReader.NumDocs();
        }

        public IEnumerable<Document> Query(IndexQueryServerSide query, FieldsToFetch fieldsToFetch, Reference<int> totalResults, Reference<int> skippedResults, IQueryResultRetriever retriever, CancellationToken token)
        {
            var docsToGet = query.PageSize;
            var position = query.Start;

            var luceneQuery = GetLuceneQuery(query.Query, query);
            var sort = GetSort(query.SortedFields);
            var returnedResults = 0;

            using (var scope = new IndexQueryingScope(_indexType, query, fieldsToFetch, _searcher, retriever, _maxIndexOutputsPerDocument, _actualMaxIndexOutputsPerDocument))
            {
                while (true)
                {
                    token.ThrowIfCancellationRequested();

                    var search = ExecuteQuery(luceneQuery, query.Start, docsToGet, sort);

                    totalResults.Value = search.TotalHits;

                    scope.RecordAlreadyPagedItemsInPreviousPage(search);

                    for (; position < search.ScoreDocs.Length && query.PageSize > 0; position++)
                    {
                        token.ThrowIfCancellationRequested();

                        var scoreDoc = search.ScoreDocs[position];
                        var document = _searcher.Doc(scoreDoc.Doc);

                        string key;
                        if (retriever.TryGetKey(document, out key) && scope.WillProbablyIncludeInResults(key) == false)
                        {
                            skippedResults.Value++;
                            continue;
                        }

                        var result = retriever.Get(document);
                        if (scope.TryIncludeInResults(result) == false)
                        {
                            skippedResults.Value++;
                            continue;
                        }

                        returnedResults++;
                        yield return result;

                        if (returnedResults == query.PageSize)
                            yield break;
                    }

                    if (scope.HasMultipleIndexOutputs)
                        docsToGet += (query.PageSize - returnedResults) * scope.MaxNumberOfIndexOutputs;
                    else
                        docsToGet += query.PageSize - returnedResults;

                    if (search.TotalHits == search.ScoreDocs.Length)
                        break;

                    if (returnedResults >= query.PageSize)
                        break;
                }
            }
        }

        public IEnumerable<Document> IntersectQuery(IndexQueryServerSide query, FieldsToFetch fieldsToFetch, Reference<int> totalResults, Reference<int> skippedResults, IQueryResultRetriever retriever, CancellationToken token)
        {
            var subQueries = query.Query.Split(IntersectSeparators, StringSplitOptions.RemoveEmptyEntries);
            if (subQueries.Length <= 1)
                throw new InvalidOperationException("Invalid INTERSECT query, must have multiple intersect clauses.");

            //Not sure how to select the page size here??? The problem is that only docs in this search can be part 
            //of the final result because we're doing an intersection query (but we might exclude some of them)
            int pageSizeBestGuess = (query.Start + query.PageSize) * 2;
            int intersectMatches, skippedResultsInCurrentLoop = 0;
            int previousBaseQueryMatches = 0, currentBaseQueryMatches;

            var firstSubDocumentQuery = GetLuceneQuery(subQueries[0], query);
            var sort = GetSort(query.SortedFields);

            using (var scope = new IndexQueryingScope(_indexType, query, fieldsToFetch, _searcher, retriever, _maxIndexOutputsPerDocument, _actualMaxIndexOutputsPerDocument))
            {
                //Do the first sub-query in the normal way, so that sorting, filtering etc is accounted for
                var search = ExecuteQuery(firstSubDocumentQuery, 0, pageSizeBestGuess, sort);
                currentBaseQueryMatches = search.ScoreDocs.Length;
                var intersectionCollector = new IntersectionCollector(_searcher, search.ScoreDocs);

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
                        intersectionCollector = new IntersectionCollector(_searcher, search.ScoreDocs);
                    }

                    for (var i = 1; i < subQueries.Length; i++)
                    {
                        var luceneSubQuery = GetLuceneQuery(subQueries[i], query);
                        _searcher.Search(luceneSubQuery, null, intersectionCollector);
                    }

                    var currentIntersectResults = intersectionCollector.DocumentsIdsForCount(subQueries.Length).ToList();
                    intersectMatches = currentIntersectResults.Count;
                    skippedResultsInCurrentLoop = pageSizeBestGuess - intersectMatches;
                } while (intersectMatches < query.PageSize                      //stop if we've got enough results to satisfy the pageSize
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
                    var document = _searcher.Doc(intersectResults[i].LuceneId);

                    string key;
                    if (retriever.TryGetKey(document, out key) && scope.WillProbablyIncludeInResults(key) == false)
                    {
                        skippedResults.Value++;
                        skippedResultsInCurrentLoop++;
                        continue;
                    }

                    var result = retriever.Get(document);
                    if (scope.TryIncludeInResults(result) == false)
                    {
                        skippedResults.Value++;
                        skippedResultsInCurrentLoop++;
                        continue;
                    }

                    returnedResults++;
                    yield return result;
                    if (returnedResults == query.PageSize)
                        yield break;
                }
            }
        }

        private TopDocs ExecuteQuery(Query documentQuery, int start, int pageSize, Sort sort)
        {
            if (pageSize == int.MaxValue && sort == null) // we want all docs, no sorting required
            {
                var gatherAllCollector = new GatherAllCollector();
                _searcher.Search(documentQuery, gatherAllCollector);
                return gatherAllCollector.ToTopDocs();
            }

            var absFullPage = Math.Abs(pageSize + start); // need to protect against ridiculously high values of pageSize + start that overflow
            var minPageSize = Math.Max(absFullPage, 1);

            // NOTE: We get Start + Pagesize results back so we have something to page on
            if (sort != null)
            {
                _searcher.SetDefaultFieldSortScoring(true, false);
                try
                {
                    return _searcher.Search(documentQuery, null, minPageSize, sort);
                }
                finally
                {
                    _searcher.SetDefaultFieldSortScoring(false, false);
                }
            }

            return _searcher.Search(documentQuery, null, minPageSize);
        }

        private Query GetLuceneQuery(string q, IndexQueryServerSide query)
        {
            Query documentQuery;

            if (string.IsNullOrEmpty(q))
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Issuing query on index {_indexName} for all documents");

                documentQuery = new MatchAllDocsQuery();
            }
            else
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Issuing query on index {_indexName} for: {q}");

                // RavenPerFieldAnalyzerWrapper searchAnalyzer = null;
                try
                {
                    //_persistance._a
                    //searchAnalyzer = parent.CreateAnalyzer(new LowerCaseKeywordAnalyzer(), toDispose, true);
                    //searchAnalyzer = parent.AnalyzerGenerators.Aggregate(searchAnalyzer, (currentAnalyzer, generator) =>
                    //{
                    //    Analyzer newAnalyzer = generator.GenerateAnalyzerForQuerying(parent.PublicName, query.Query, currentAnalyzer);
                    //    if (newAnalyzer != currentAnalyzer)
                    //    {
                    //        DisposeAnalyzerAndFriends(toDispose, currentAnalyzer);
                    //    }
                    //    return parent.CreateAnalyzer(newAnalyzer, toDispose, true);
                    //});

                    documentQuery = QueryBuilder.BuildQuery(q, query, _analyzer);
                }
                finally
                {
                    //DisposeAnalyzerAndFriends(toDispose, searchAnalyzer);
                }
            }

            //var afterTriggers = ApplyIndexTriggers(documentQuery);

            return documentQuery;
        }

        private static Sort GetSort(SortedField[] sortedFields)
        {
            if (sortedFields == null || sortedFields.Length == 0)
                return null;

            return new Sort(sortedFields.Select(x =>
            {
                var sortOptions = SortOptions.String;

                if (InvariantCompare.IsPrefix(x.Field, Constants.AlphaNumericFieldName, CompareOptions.None))
                {
                    var customField = SortFieldHelper.CustomField(x.Field);
                    if (string.IsNullOrEmpty(customField.Name))
                        throw new InvalidOperationException("Alphanumeric sort: cannot figure out what field to sort on!");

                    var anSort = new AlphaNumericComparatorSource();
                    return new SortField(customField.Name, anSort, x.Descending);
                }

                if (InvariantCompare.IsSuffix(x.Field, _Range, CompareOptions.None))
                {
                    sortOptions = SortOptions.NumericDouble; // TODO arek - it seems to be working fine with long values as well however needs to be verified
                }

                return new SortField(IndexField.ReplaceInvalidCharactersInFieldName(x.Field), (int)sortOptions, x.Descending);
            }).ToArray());
        }

        public HashSet<string> Terms(string field, string fromValue, int pageSize)
        {
            var results = new HashSet<string>();
            using (var termEnum = _searcher.IndexReader.Terms(new Term(field, fromValue ?? string.Empty)))
            {
                if (string.IsNullOrEmpty(fromValue) == false) // need to skip this value
                {
                    while (termEnum.Term == null || fromValue.Equals(termEnum.Term.Text))
                    {
                        if (termEnum.Next() == false)
                            return results;
                    }
                }
                while (termEnum.Term == null ||
                    field.Equals(termEnum.Term.Field))
                {
                    if (termEnum.Term != null)
                        results.Add(termEnum.Term.Text);

                    if (results.Count >= pageSize)
                        break;

                    if (termEnum.Next() == false)
                        break;
                }
            }

            return results;
        }

        public IEnumerable<Document> MoreLikeThis(MoreLikeThisQueryServerSide query, HashSet<string> stopWords, Func<string[], IQueryResultRetriever> createRetriever, CancellationToken token)
        {
            var documentQuery = new BooleanQuery();

            if (string.IsNullOrWhiteSpace(query.DocumentId) == false)
                documentQuery.Add(new TermQuery(new Term(Constants.DocumentIdFieldName, query.DocumentId.ToLowerInvariant())), Occur.MUST);

            foreach (var key in query.MapGroupFields.Keys)
                documentQuery.Add(new TermQuery(new Term(key, query.MapGroupFields[key])), Occur.MUST);

            var td = _searcher.Search(documentQuery, 1);

            // get the current Lucene docid for the given RavenDB doc ID
            if (td.ScoreDocs.Length == 0)
                throw new InvalidOperationException("Document " + query.DocumentId + " could not be found");

            var ir = _searcher.IndexReader;
            var mlt = new RavenMoreLikeThis(ir, query);

            if (stopWords != null)
                mlt.SetStopWords(stopWords);

            var fieldNames = query.Fields ?? ir.GetFieldNames(IndexReader.FieldOption.INDEXED)
                                    .Where(x => x != Constants.DocumentIdFieldName && x != Constants.ReduceKeyFieldName)
                                    .ToArray();

            mlt.SetFieldNames(fieldNames);
            mlt.Analyzer = _analyzer;

            var mltQuery = mlt.Like(td.ScoreDocs[0].Doc);
            var tsdc = TopScoreDocCollector.Create(query.PageSize, true);

            if (string.IsNullOrWhiteSpace(query.AdditionalQuery) == false)
            {
                var additionalQuery = QueryBuilder.BuildQuery(query.AdditionalQuery, _analyzer);
                mltQuery = new BooleanQuery
                    {
                        {mltQuery, Occur.MUST},
                        {additionalQuery, Occur.MUST},
                    };
            }

            _searcher.Search(mltQuery, tsdc);
            var hits = tsdc.TopDocs().ScoreDocs;
            var baseDocId = td.ScoreDocs[0].Doc;

            var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            var fieldsToFetch = string.IsNullOrWhiteSpace(query.DocumentId)
                ? _searcher.Doc(baseDocId).GetFields().Cast<AbstractField>().Select(x => x.Name).Distinct().ToArray()
                : null;

            var retriever = createRetriever(fieldsToFetch);

            foreach (var hit in hits)
            {
                if (hit.Doc == baseDocId)
                    continue;

                var doc = _searcher.Doc(hit.Doc);
                var id = doc.Get(Constants.DocumentIdFieldName) ?? doc.Get(Constants.ReduceKeyFieldName);
                if (id == null)
                    continue;

                if (ids.Add(id) == false)
                    continue;

                yield return retriever.Get(doc);
            }
        }

        public override void Dispose()
        {
            _analyzer?.Dispose();
            _releaseSearcher?.Dispose();
            _releaseReadTransaction?.Dispose();
        }
    }
}