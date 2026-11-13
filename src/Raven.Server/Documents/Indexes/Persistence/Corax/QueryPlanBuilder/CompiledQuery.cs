using System;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Querying.Planning;
using Corax.Utils;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

// This is used across yield boundary, so it cannot be a `ref struct`, should NOT be copied 
internal record struct CompiledQuery(
    IQueryMatch QueryMatch,
    IQueryMatch ExecutedMatch,
    IQueryMatch SortingWrapper,
    QueryExecution Execution,
    QueryBuilderParameters QueryBuilderParams,
    OrderMetadata[] OrderByFields) : IDisposable
{
    /// <summary>
    /// Vector post-filter streams its HNSW output in score order, we skipped adding SortingMatch (which does scoring)
    /// so we need to explicitly Score() after the Fill() call
    /// </summary>
    public bool ScoresProducedDuringFill => Execution is { VectorPostFilterProvidesScoreOrder: true };

    public bool WritesScores => ScoresProducedDuringFill
                                || (QueryMatch is IRequireSortingDataTransfer && Array.Exists(OrderByFields ?? [], x => x.FieldType == MatchCompareFieldType.Score));

    public void Dispose()
    {
        (QueryMatch as IDisposable)?.Dispose();
        QueryMatch = null;
    }
}
