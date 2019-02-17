using System;
using System.Collections.Generic;
using System.Text;

namespace Raven.Server.Documents.Queries.Graph
{
    public class QueryQueryStepGatherer : QueryPlanVisitor
    {
        public readonly List<QueryQueryStep> QuerySteps = new List<QueryQueryStep>();
        public override void VisitQueryQueryStep(QueryQueryStep qqs) => QuerySteps.Add(qqs);
    }
}
