using System;
using System.Collections.Generic;
using System.Threading;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Results;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene;

public sealed class LuceneIndexQueryingScope : IndexQueryingScopeBase<string>
{
    private readonly IndexSearcher _searcher;

    private readonly IState _state;

    public LuceneIndexQueryingScope(IndexType indexType, IndexQueryServerSide query, FieldsToFetch fieldsToFetch, IndexSearcher searcher, IQueryResultRetriever retriever,
        IState state) : base(indexType, query, fieldsToFetch, retriever, 
        new HashSet<string>(StringComparer.OrdinalIgnoreCase))
    {
        _searcher = searcher;
        _state = state;
    }

    public void RecordAlreadyPagedItemsInPreviousPage(TopDocs search, CancellationToken token)
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
                    var document = _searcher.Doc(scoreDoc.Doc, _state);
                    var alreadyPagedKey = document.Get(Constants.Documents.Indexing.Fields.DocumentIdFieldName, _state);

                    _alreadySeenDocumentKeysInPreviousPage.Add(alreadyPagedKey);
                }
            }
            else
            {
                // that's not a sorted query so we need just to ensure that we won't return the last item of the previous page
                var scoreDoc = search.ScoreDocs[_query.Start - 1];
                var document = _searcher.Doc(scoreDoc.Doc, _state);
                var alreadyPagedKey = document.Get(Constants.Documents.Indexing.Fields.DocumentIdFieldName, _state);

                _alreadySeenDocumentKeysInPreviousPage.Add(alreadyPagedKey);
            }
        }

        if (_fieldsToFetch.IsDistinct == false)
            return;

        if (search.ScoreDocs.Length <= _alreadyScannedForDuplicates)
            return;

        if (search.ScoreDocs.Length <= _query.Start)
            return;

        for (; _alreadyScannedForDuplicates < _query.Start; _alreadyScannedForDuplicates++)
        {
            var scoreDoc = search.ScoreDocs[_alreadyScannedForDuplicates];
            var retrieverInput = new RetrieverInput(_searcher.Doc(scoreDoc.Doc, _state), scoreDoc, _state);
            var result = _retriever.Get(ref retrieverInput, token);

            if (result.Document != null)
            {
                if (result.Document.Data.Count > 0) // we don't consider empty projections to be relevant for distinct operations
                    _alreadySeenProjections.Add(result.Document.DataHash);
            }
            else if (result.List != null)
            {
                foreach (Document item in result.List)
                {
                    if (item.Data.Count > 0) // we don't consider empty projections to be relevant for distinct operations
                        _alreadySeenProjections.Add(item.DataHash);
                }
            }
        }
    }
}
