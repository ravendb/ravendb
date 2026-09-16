using System.Collections.Generic;
using Corax.Querying.Planning;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal readonly record struct PendingBoost(List<ClauseInfo> InnerClauses, ParameterBinding Factor);
