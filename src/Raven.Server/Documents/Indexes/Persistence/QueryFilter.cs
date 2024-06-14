using System;
using Lucene.Net.Store;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Indexes.Persistence;

public enum FilterResult
{
    Accepted,
    Skipped,
    LimitReached
}

public class QueryFilter : IDisposable
{
    private readonly IndexQueryServerSide _query;
    private readonly DocumentsOperationContext _documentsContext;
    private readonly Reference<int> _skippedResults;
    private readonly Reference<int> _scannedDocuments;
    private readonly IQueryResultRetriever _retriever;
    private readonly QueryTimingsScope _queryTimings;
    private readonly ScriptRunner.SingleRun _filterScriptRun;
    private ScriptRunner.ReturnRun _filterSingleRun;

    public QueryFilter(Index index, IndexQueryServerSide query, DocumentsOperationContext documentsContext, Reference<int> skippedResults,
        Reference<int> scannedDocuments, IQueryResultRetriever retriever, QueryTimingsScope queryTimings)
    {
        _query = query;
        _documentsContext = documentsContext;
        _skippedResults = skippedResults;
        _scannedDocuments = scannedDocuments;
        _retriever = retriever;
        _queryTimings = queryTimings;

        var key = new CollectionQueryEnumerable.FilterKey(query.Metadata);
        _filterSingleRun = index.DocumentDatabase.Scripts.GetScriptRunner(key, readOnly: true, patchRun: out _filterScriptRun);
    }

    public FilterResult Apply(ref RetrieverInput input, string key)
    {
        var doc = _retriever.DirectGet(ref input, key, DocumentFields.All);
        if (doc == null)
        {
            _skippedResults.Value++;
            return FilterResult.Skipped;
        }

        if (_scannedDocuments.Value >= _query.FilterLimit)
        {
            return FilterResult.LimitReached;
        }

        _scannedDocuments.Value++;
        object self = _filterScriptRun.Translate(_documentsContext, doc);
        using (_queryTimings?.For(nameof(QueryTimingsScope.Names.Filter)))
        using (var result = _filterScriptRun.Run(_documentsContext, _documentsContext, "execute", new[] { self, _query.QueryParameters }, _queryTimings))
        {
            if (result.BooleanValue == true)
            {
                return FilterResult.Accepted;
            }

            _skippedResults.Value++;
            return FilterResult.Skipped;
        }
    }

    public void Dispose()
    {
        _filterSingleRun.Dispose();
    }
}
