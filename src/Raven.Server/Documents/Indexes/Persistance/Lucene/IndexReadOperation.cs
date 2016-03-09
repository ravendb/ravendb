using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using Lucene.Net.Search;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Logging;
using Raven.Server.Documents.Queries;
using Raven.Server.Indexing;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.Persistance.Lucene
{
    public class IndexReadOperation : IDisposable
    {
        private const string _Range = "_Range";

        private static readonly ILog Log = LogManager.GetLogger(typeof(IndexReadOperation).FullName);
        private static readonly CompareInfo InvariantCompare = CultureInfo.InvariantCulture.CompareInfo;

        private readonly string _indexName;
        private readonly IndexSearcher _searcher;
        private readonly LowerCaseKeywordAnalyzer _analyzer;
        private readonly IDisposable _releaseSearcher;
        private readonly IDisposable _releaseReadTransaction;

        public IndexReadOperation(string indexName, LuceneVoronDirectory directory, IndexSearcherHolder searcherHolder, Transaction readTransaction)
        {
            _analyzer = new LowerCaseKeywordAnalyzer();
            _indexName = indexName;
            _releaseReadTransaction = directory.SetTransaction(readTransaction);
            _releaseSearcher = searcherHolder.GetSearcher(out _searcher);
        }

        public IEnumerable<string> Query(IndexQuery query, CancellationToken token, Reference<int> totalResults)
        {
            var docsToGet = query.PageSize;
            var position = query.Start;

            var luceneQuery = GetLuceneQuery(query);
            var returnedResults = 0;
            var endOfResults = false;

            do
            {
                token.ThrowIfCancellationRequested();

                var search = ExecuteQuery(luceneQuery, query.Start, docsToGet, query.SortedFields);

                totalResults.Value = search.TotalHits;

                //RecordAlreadyPagedItemsInPreviousPage(start, search, indexSearcher);

                //SetupHighlighter(documentQuery);

                for (; position < search.ScoreDocs.Length && query.PageSize > 0; position++)
                {
                    token.ThrowIfCancellationRequested();

                    var scoreDoc = search.ScoreDocs[position];
                    var document = _searcher.Doc(scoreDoc.Doc);

                    //var indexQueryResult = parent.RetrieveDocument(document, fieldsToFetch, scoreDoc);
                    //if (indexQueryResult.Key == null && !string.IsNullOrEmpty(indexQuery.HighlighterKeyName))
                    //{
                    //    indexQueryResult.HighlighterKey = document.Get(indexQuery.HighlighterKeyName);
                    //}

                    //if (ShouldIncludeInResults(indexQueryResult) == false)
                    //{
                    //    indexQuery.SkippedResults.Value++;
                    //    continue;
                    //}

                    //AddHighlighterResults(indexSearcher, scoreDoc, indexQueryResult);

                    //AddQueryExplanation(documentQuery, indexSearcher, scoreDoc, indexQueryResult);

                    returnedResults++;

                    yield return document.Get(Abstractions.Data.Constants.DocumentIdFieldName);
                    if (returnedResults == query.PageSize)
                        yield break;
                }

                //if (hasMultipleIndexOutputs)
                //    docsToGet += (pageSize - returnedResults) * maxNumberOfIndexOutputs;
                //else
                docsToGet += (query.PageSize - returnedResults);

                endOfResults = search.TotalHits == search.ScoreDocs.Length;

            } while (returnedResults < query.PageSize && endOfResults == false);
        }

        private TopDocs ExecuteQuery(Query documentQuery, int start, int pageSize, SortedField[] sortedFields)
        {
            // TODO arek
            //if (pageSize == int.MaxValue && sortedFields == null) // we want all docs, no sorting required
            //{
            //    var gatherAllCollector = new GatherAllCollector();
            //    indexSearcher.Search(documentQuery, gatherAllCollector);
            //    return gatherAllCollector.ToTopDocs();
            //}

            var absFullPage = Math.Abs(pageSize + start); // need to protect against ridiculously high values of pageSize + start that overflow
            var minPageSize = Math.Max(absFullPage, 1);

            // NOTE: We get Start + Pagesize results back so we have something to page on
            if (sortedFields != null)
            {
                var sort = GetSort(sortedFields);

                _searcher.SetDefaultFieldSortScoring(true, false);
                try
                {
                    return _searcher.Search(documentQuery, null, minPageSize, sort); ;
                }
                finally
                {
                    _searcher.SetDefaultFieldSortScoring(false, false);
                }
            }

            return _searcher.Search(documentQuery, null, minPageSize);
        }

        private Query GetLuceneQuery(IndexQuery query)
        {
            Query documentQuery;

            if (string.IsNullOrEmpty(query.Query))
            {
                if (Log.IsDebugEnabled)
                    Log.Debug($"Issuing query on index {_indexName} for all documents");

                documentQuery = new MatchAllDocsQuery();
            }
            else
            {
                if (Log.IsDebugEnabled)
                    Log.Debug($"Issuing query on index {_indexName} for: {query.Query}");

                var toDispose = new List<Action>();
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

                    documentQuery = QueryBuilder.BuildQuery(query.Query, query, _analyzer);
                }
                finally
                {
                    //DisposeAnalyzerAndFriends(toDispose, searchAnalyzer);
                }
            }

            //var afterTriggers = ApplyIndexTriggers(documentQuery);

            return documentQuery;
        }

        private Sort GetSort(SortedField[] sortedFields)
        {
            return new Sort(sortedFields.Select(x =>
            {
                var sortOptions = SortOptions.String;

                if (InvariantCompare.IsSuffix(x.Field, _Range, CompareOptions.None))
                {
                    sortOptions = SortOptions.NumbericDouble; // TODO arek
                }

                return new SortField(x.Field, (int)sortOptions, x.Descending);
            }).ToArray());
        }

        public void Dispose()
        {
            _analyzer?.Dispose();
            _releaseSearcher?.Dispose();
            _releaseReadTransaction?.Dispose();
        }
    }
}