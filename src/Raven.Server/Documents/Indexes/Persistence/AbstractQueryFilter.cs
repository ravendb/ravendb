using System;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Timings;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Persistence;

public abstract class AbstractQueryFilter : IDisposable
{
    private ScriptRunner.ReturnRun _filterSingleRun;

    protected readonly IndexQueryServerSide Query;
    protected readonly QueryTimingsScope QueryTimings;
    protected readonly JsonOperationContext Context;
    protected readonly ScriptRunner.SingleRun FilterScriptRun;

    protected abstract long? ScannedDocuments { get; }
    
    protected AbstractQueryFilter(IndexQueryServerSide query, QueryTimingsScope queryTimings,
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

public enum FilterResult
{
    Accepted,
    Skipped,
    LimitReached
}
