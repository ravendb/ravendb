using System.Collections.Generic;
using Corax.Querying.Planning;
using Raven.Server.Documents.Queries;
using Sparrow.Json;
using IndexSearcher = Corax.Querying.IndexSearcher;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal sealed class ResolutionContext
{
    public readonly List<string> Errors = [];
    // Where to find the values that the clauses need to execute (indexed using the ValueOrdinal)
    public readonly List<ParameterBinding> SlotBindings = [];

    public readonly BlittableJsonReaderObject QueryParameters;
    public readonly QueryMetadata Metadata;
    public readonly IndexSearcher IndexSearcher;
    public readonly QueryBuilderParameters BuilderParams;
    public int WhenCount;
    public bool IsOr;
    public List<ClauseInfo> SpatialClauses;
    public List<ClauseInfo> VectorClauses;
    public List<ClauseInfo> Clauses;
    public List<PendingBoost> PendingBoosts;

    public (int First, int Second) CompoundExact = (-1, -1);
    public bool CompoundExactAFirst;
    public string CompoundExactName;
    public int CompoundFieldDrivingClause = -1;
    public string CompoundFieldSortName;
    public string CompoundFieldName;
    public int CompoundFieldField2Range = -1;

    public ResolutionContext(PlanParameters p)
        : this(p.QueryParameters, p.Metadata, p.IndexSearcher)
    {
    }

    public ResolutionContext(QueryBuilderParameters b)
        : this(b.QueryParameters, b.Metadata, b.IndexSearcher)
    {
        BuilderParams = b;
    }

    private ResolutionContext(BlittableJsonReaderObject queryParameters, QueryMetadata metadata, IndexSearcher indexSearcher)
    {
        QueryParameters = queryParameters;
        Metadata = metadata;
        IndexSearcher = indexSearcher;
    }

    public void Report(string error) => Errors.Add(error);

    public void RecordPendingBoost(List<ClauseInfo> innerClauses, ParameterBinding factor)
    {
        PendingBoosts ??= [];
        PendingBoosts.Add(new PendingBoost(innerClauses, factor));
    }
}
