using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal sealed class WhenConditionEvaluator(QueryExpression condition, QueryMetadata metadata)
{   // using a dedicated class here for this, to explicitly control what is captured
    public bool Evaluate(BlittableJsonReaderObject queryParams) =>
        QueryBuilderHelper.EvaluateConstantExpressionForWhenQuery(condition, metadata.Query, metadata, queryParams);
}
