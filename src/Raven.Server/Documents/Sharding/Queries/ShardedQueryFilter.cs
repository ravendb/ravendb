using Raven.Server.Documents.Indexes.Persistence;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Sharding;
using Raven.Server.Documents.Queries.Timings;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Queries;

public sealed class ShardedQueryFilter : AbstractQueryFilter
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
