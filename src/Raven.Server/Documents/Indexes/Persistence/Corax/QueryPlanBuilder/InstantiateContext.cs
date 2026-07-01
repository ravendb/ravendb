using Corax.Querying.Planning;
using Corax.Utils;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal ref struct InstantiateContext(CompiledPlan plan, QueryExecution exec, OrderMetadata[] orderByFields, PlanParameters planParams, QueryBuilderParameters builderParams, bool wantTimings)
{
    public readonly CompiledPlan Plan = plan;
    public readonly QueryExecution Exec = exec;
    public readonly OrderMetadata[] OrderByFields = orderByFields; // may be null when PageSize == 0
    public readonly PlanParameters PlanParams = planParams;
    public readonly QueryBuilderParameters BuilderParams = builderParams;

    public readonly bool WantTimings = wantTimings;

    public string RejectReason;
}
