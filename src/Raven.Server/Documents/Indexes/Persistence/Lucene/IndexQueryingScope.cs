using System;
using System.Collections.Generic;

using Lucene.Net.Search;

using Raven.Abstractions.Data;
using Raven.Client.Data;
using Raven.Client.Data.Indexes;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Results;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public class IndexQueryingScope : IDisposable
    {
        private readonly IndexType _indexType;

        private readonly IndexQuery _query;

        private readonly FieldsToFetch _fieldsToFetch;

        private readonly IndexSearcher _searcher;

        private readonly IQueryResultRetriever _retriever;

        private readonly bool _isSortingQuery;

        private readonly HashSet<ulong> _alreadySeenProjections;

        private readonly HashSet<string> _alreadySeenDocumentKeysInPreviousPage = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        //private bool _hasMultipleIndexOutputs;

        private int _alreadyScannedForDuplicates;

        public IndexQueryingScope(IndexType indexType, IndexQuery query, FieldsToFetch fieldsToFetch, IndexSearcher searcher, IQueryResultRetriever retriever)
        {
            _indexType = indexType;
            _query = query;
            _fieldsToFetch = fieldsToFetch;
            _searcher = searcher;
            _retriever = retriever;
            _isSortingQuery = query.SortedFields != null && query.SortedFields.Length > 0;

            if (_fieldsToFetch.IsDistinct)
                _alreadySeenProjections = new HashSet<ulong>();
        }

        public void Dispose()
        {
        }

        public void RecordAlreadyPagedItemsInPreviousPage(TopDocs search)
        {
            if (_query.Start == 0)
                return;

            if (_query.SkipDuplicateChecking)
                return;

            // we are paging, we need to check that we don't have duplicates in the previous pages
            // see here for details: http://groups.google.com/group/ravendb/browse_frm/thread/d71c44aa9e2a7c6e

            if (_indexType.IsMap() && _fieldsToFetch.IsProjection == false && search.ScoreDocs.Length >= _query.Start)
            {
                if (_isSortingQuery)
                {
                    // we need to scan all records from the beginning to requested 'start' position
                    for (var i = 0; i < _query.Start && i < search.ScoreDocs.Length; i++)
                    {
                        var scoreDoc = search.ScoreDocs[i];
                        var document = _searcher.Doc(scoreDoc.Doc);
                        var alreadyPagedKey = document.Get(Constants.DocumentIdFieldName);

                        _alreadySeenDocumentKeysInPreviousPage.Add(alreadyPagedKey);
                        //_hasMultipleIndexOutputs = true;
                    }
                }
                else
                {
                    // that's not a sorted query so we need just to ensure that we won't return the last item of the previous page
                    var scoreDoc = search.ScoreDocs[_query.Start - 1];
                    var document = _searcher.Doc(scoreDoc.Doc);
                    var alreadyPagedKey = document.Get(Constants.DocumentIdFieldName);

                    _alreadySeenDocumentKeysInPreviousPage.Add(alreadyPagedKey);
                    //_hasMultipleIndexOutputs = true;
                }
            }

            if (_fieldsToFetch.IsDistinct == false)
                return;

            for (; _alreadyScannedForDuplicates < _query.Start; _alreadyScannedForDuplicates++)
            {
                var scoreDoc = search.ScoreDocs[_alreadyScannedForDuplicates];
                var document = _retriever.Get(_searcher.Doc(scoreDoc.Doc));

                if (document.Data.Count > 0) // we don't consider empty projections to be relevant for distinct operations
                    _alreadySeenProjections.Add(document.DataHash);
            }
        }

        public bool ShouldIncludeInResults(Document document)
        {
            //if (shouldIncludeInResults(indexQueryResult) == false)
            //    return false;

            if (_fieldsToFetch.IsDistinct)
                return _alreadySeenProjections.Add(document.DataHash);

            if (_indexType.IsMapReduce())
                return true;

            if (_alreadySeenDocumentKeysInPreviousPage.Contains(document.Key))
            {
                //_hasMultipleIndexOutputs = true;
                return false;
            }

            return true;
        }
    }
}