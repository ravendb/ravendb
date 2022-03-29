using System;
using System.Collections.Generic;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Results;

namespace Raven.Server.Documents.Indexes.Persistence;

public abstract class IndexQueryingScopeBase<TKey> : IDisposable
{
    protected readonly IndexType _indexType;

    protected readonly IndexQueryServerSide _query;

    protected readonly FieldsToFetch _fieldsToFetch;
    
    protected readonly IQueryResultRetriever _retriever;

    protected readonly bool _isSortingQuery;

    protected readonly HashSet<ulong> _alreadySeenProjections;

    protected readonly HashSet<TKey> _alreadySeenDocumentKeysInPreviousPage;

    protected int _alreadyScannedForDuplicates;
    
    public IndexQueryingScopeBase(IndexType indexType, IndexQueryServerSide query, FieldsToFetch fieldsToFetch, IQueryResultRetriever retriever, HashSet<TKey> alreadySeenDocumentKeysInPreviousPage)
    {
        _indexType = indexType;
        _query = query;
        _fieldsToFetch = fieldsToFetch;
        _retriever = retriever;
        _isSortingQuery = query.Metadata.OrderBy != null;
        _alreadySeenDocumentKeysInPreviousPage = alreadySeenDocumentKeysInPreviousPage;

        if (_fieldsToFetch.IsDistinct)
            _alreadySeenProjections = new HashSet<ulong>();
    }
    
    public bool WillProbablyIncludeInResults(TKey key)
    {
        if (_fieldsToFetch.IsDistinct)
            return true;

        if (_indexType.IsMapReduce())
            return true;

        if (_query.SkipDuplicateChecking)
            return true;

        if (_fieldsToFetch.IsProjection && _alreadySeenDocumentKeysInPreviousPage.Contains(key))
            return false;

        if (_fieldsToFetch.IsProjection == false && _alreadySeenDocumentKeysInPreviousPage.Add(key) == false)
        {
            return false;
        }

        return true;
    }

    public bool TryIncludeInResults(Document document)
    {
        if (document == null)
            return false;

        if (_fieldsToFetch.IsDistinct)
            return _alreadySeenProjections.Add(document.DataHash);
            
        return true;
    }

    public void Dispose()
    {
    }
}
