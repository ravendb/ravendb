using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using Corax;
using Corax.Mappings;
using Corax.Queries;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Utils;
using Sparrow;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public class CoraxIndexQueryingScope : IndexQueryingScopeBase<UnmanagedSpan>
{
    private readonly IndexSearcher _searcher;
    private readonly IndexFieldsMapping _fieldsMapping;

    public CoraxIndexQueryingScope(IndexType indexType, IndexQueryServerSide query, FieldsToFetch fieldsToFetch, IQueryResultRetriever retriever, IndexSearcher searcher,
        IndexFieldsMapping fieldsMapping)
        : base(indexType, query, fieldsToFetch, retriever, new(UnmanagedSpanComparer.Instance))
    {
        _searcher = searcher;
        _fieldsMapping = fieldsMapping;
    }

    public int RecordAlreadyPagedItemsInPreviousPage(Span<long> ids, IQueryMatch match, Reference<int> totalResults, out int read, ref int queryStart, IndexFieldsPersistence indexFieldsPersistence,
        CancellationToken token)
    {
        read = match.Fill(ids);
        totalResults.Value += read;

        if (read == 0)
            return 0;

        if (queryStart <= 0)
            return 0;

        if (read <= queryStart && ids.Length <= queryStart)
        {
            read = 0;
            return 0;
        }

        if (_query.SkipDuplicateChecking)
            return 0;


        var limit = queryStart > read 
            ? read 
            : queryStart;
        
        queryStart -= limit;

        var distinctIds = ids.Slice(0, limit);

        // we are paging, we need to check that we don't have duplicates in the previous pages
        // see here for details: http://groups.google.com/group/ravendb/browse_frm/thread/d71c44aa9e2a7c6e
       
        if (_indexType.IsMap() && _fieldsToFetch.IsProjection == false)
        {
            // Assumptions: we're in Map, so thats mean we have ID of the doc saved in the tree. So we want to keep track what we returns
            foreach (var id in distinctIds)
            {
                var key = _searcher.GetRawIdentityFor(id);
                _alreadySeenDocumentKeysInPreviousPage.Add(key);
            }
        }

        if (_fieldsToFetch.IsDistinct == false)
        {
            return limit;
        }

        foreach (var id in distinctIds)
        {
            var coraxEntry = _searcher.GetReaderAndIdentifyFor(id, out var key);
            var retrieverInput = new RetrieverInput(_searcher, _fieldsMapping, coraxEntry, key, indexFieldsPersistence);
            var result = _retriever.Get(ref retrieverInput, token);

            if (result.Document != null)
            {
                if (result.Document.Data.Count > 0)
                {
                    // we don't consider empty projections to be relevant for distinct operations
                    _alreadySeenProjections.Add(result.Document.DataHash);
                }
            }
            else if (result.List != null)
            {
                foreach (Document item in result.List)
                {
                    if (item.Data.Count > 0)
                    {
                        // we don't consider empty projections to be relevant for distinct operations
                        _alreadySeenProjections.Add(item.DataHash);
                    }
                }
            }
        }

        return limit;
    }
}
