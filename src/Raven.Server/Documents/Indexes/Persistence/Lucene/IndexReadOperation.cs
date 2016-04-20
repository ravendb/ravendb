using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;

using Lucene.Net.Index;
using Lucene.Net.Search;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Logging;
using Raven.Client.Data;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Indexing;

using Voron.Impl;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public class IndexReadOperation : IndexOperationBase
    {
        private const string _Range = "_Range";

        private static readonly ILog Log = LogManager.GetLogger(typeof(IndexReadOperation).FullName);
        private static readonly CompareInfo InvariantCompare = CultureInfo.InvariantCulture.CompareInfo;

        private readonly string _indexName;
        private readonly IndexSearcher _searcher;
        private readonly RavenPerFieldAnalyzerWrapper _analyzer;
        private readonly IDisposable _releaseSearcher;
        private readonly IDisposable _releaseReadTransaction;

        public IndexReadOperation(string indexName, Dictionary<string, IndexField> fields, LuceneVoronDirectory directory, IndexSearcherHolder searcherHolder, Transaction readTransaction)
        {
            _analyzer = CreateAnalyzer(() => new LowerCaseKeywordAnalyzer(), fields, forQuerying: true);
            _indexName = indexName;
            _releaseReadTransaction = directory.SetTransaction(readTransaction);
            _releaseSearcher = searcherHolder.GetSearcher(out _searcher);
        }

        public int EntriesCount()
        {
            return _searcher.IndexReader.NumDocs();
        }

        public IEnumerable<Document> Query(IndexQuery query, CancellationToken token, Reference<int> totalResults, IQueryResultRetriever retriever)
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

                for (; position < search.ScoreDocs.Length && query.PageSize > 0; position++)
                {
                    token.ThrowIfCancellationRequested();

                    var scoreDoc = search.ScoreDocs[position];
                    var document = _searcher.Doc(scoreDoc.Doc);

                    returnedResults++;

                    yield return retriever.Get(document);

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
            if (pageSize == int.MaxValue && sortedFields == null) // we want all docs, no sorting required
            {
                throw new NotImplementedException("TODO arek");
                //var gatherAllCollector = new GatherAllCollector();
                //indexSearcher.Search(documentQuery, gatherAllCollector);
                //return gatherAllCollector.ToTopDocs();
            }

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
                    sortOptions = SortOptions.NumericDouble; // TODO arek - it seems to be working fine with long values as well however needs to be verified
                }

                return new SortField(x.Field, (int)sortOptions, x.Descending);
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

        public override void Dispose()
        {
            _analyzer?.Dispose();
            _releaseSearcher?.Dispose();
            _releaseReadTransaction?.Dispose();
        }
    }
}