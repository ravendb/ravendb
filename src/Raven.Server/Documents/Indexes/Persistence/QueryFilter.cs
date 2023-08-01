using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Indexes.Persistence;

public sealed class QueryFilter : AbstractQueryFilter
{
    private readonly Reference<long> _skippedResults;
    private readonly Reference<long> _scannedDocuments;
    private readonly IQueryResultRetriever _retriever;
    private readonly DocumentsOperationContext _documentsContext;

    protected override long? ScannedDocuments => _scannedDocuments.Value;

    public QueryFilter(Index index, IndexQueryServerSide query, DocumentsOperationContext documentsContext, Reference<long> skippedResults,
        Reference<long> scannedDocuments, IQueryResultRetriever retriever, QueryTimingsScope queryTimings)
        : base(query, queryTimings, index.DocumentDatabase.Scripts, documentsContext)
    {
        _skippedResults = skippedResults;
        _scannedDocuments = scannedDocuments;
        _retriever = retriever;
        _documentsContext = documentsContext;
    }

    public FilterResult Apply(ref RetrieverInput input, string key)
    {
        var doc = _retriever.DirectGet(ref input, key, DocumentFields.All);
        return Apply(doc);
    }

    protected override ScriptRunnerResult GetScriptRunnerResult(object translatedDoc)
    {
        return FilterScriptRun.Run(Context, _documentsContext, "execute", new[] { translatedDoc, Query.QueryParameters }, QueryTimings);
    }

    protected override void IncrementSkippedResults()
    {
        _skippedResults.Value++;
    }

    protected override void IncrementScannedDocuments()
    {
        _scannedDocuments.Value++;
    }
}
