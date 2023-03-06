using System;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Sharding;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Persistence;

public enum FilterResult
{
    Accepted,
    Skipped,
    LimitReached
}

public abstract class QueryFilterBase : IDisposable
{
    private ScriptRunner.ReturnRun _filterSingleRun;

    protected readonly IndexQueryServerSide Query;
    protected readonly QueryTimingsScope QueryTimings;
    protected readonly JsonOperationContext Context;
    protected readonly ScriptRunner.SingleRun FilterScriptRun;

    protected abstract long? ScannedDocuments { get; }
    
    protected QueryFilterBase(IndexQueryServerSide query, QueryTimingsScope queryTimings,
        ScriptRunnerCache scriptRunnerCache, JsonOperationContext context)
    {
        Query = query;
        QueryTimings = queryTimings;
        Context = context;

        var key = new CollectionQueryEnumerable.FilterKey(Query.Metadata);
        _filterSingleRun = scriptRunnerCache.GetScriptRunner(key, readOnly: true, patchRun: out FilterScriptRun);
    }

    protected abstract ScriptRunnerResult GetScriptRunnerResult(object translatedDoc);
    protected abstract void IncrementSkippedResults();
    protected abstract void IncrementScannedDocuments();

    protected FilterResult Apply(object doc)
    {
        if (doc == null)
        {
            IncrementSkippedResults();
            return FilterResult.Skipped;
        }

        if (ScannedDocuments >= Query.FilterLimit)
        {
            return FilterResult.LimitReached;
        }

        IncrementScannedDocuments();

        object self = FilterScriptRun.Translate(Context, doc);
        using (QueryTimings?.For(nameof(QueryTimingsScope.Names.Filter)))
        using (var result = GetScriptRunnerResult(self))
        {
            if (result.BooleanValue == true)
            {
                return FilterResult.Accepted;
            }

            IncrementSkippedResults();
            return FilterResult.Skipped;
        }
    }

    public void Dispose()
    {
        _filterSingleRun.Dispose();
    }
}

public class QueryFilter : QueryFilterBase
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

public class ShardedQueryFilter : QueryFilterBase
{
    private readonly ShardedQueryResult _result;

    protected override long? ScannedDocuments => _result.ScannedResults;

    public ShardedQueryFilter(IndexQueryServerSide query, ShardedQueryResult shardedQueryResult,
        QueryTimingsScope queryTimings, ScriptRunnerCache scriptRunnerCache, JsonOperationContext context)
        : base(query, queryTimings, scriptRunnerCache, context)
    {
        _result = shardedQueryResult;
    }

    public FilterResult Apply(BlittableJsonReaderObject result)
    {
        return base.Apply(result);
    }


    protected override ScriptRunnerResult GetScriptRunnerResult(object translatedDoc)
    {
        return FilterScriptRun.Run(Context, null, "execute", new[] { translatedDoc, Query.QueryParameters }, QueryTimings);
    }

    protected override void IncrementSkippedResults()
    {
        _result.SkippedResults++;
    }

    protected override void IncrementScannedDocuments()
    {
        _result.ScannedResults++;
    }
}
