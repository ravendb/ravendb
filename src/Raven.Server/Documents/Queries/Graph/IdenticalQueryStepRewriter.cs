using System.Collections.Generic;
using System.Linq;

namespace Raven.Server.Documents.Queries.Graph
{
    public class IdenticalQueryStepRewriter : QueryPlanRewriter
    {
        private Dictionary<string, List<QueryQueryStep>> _identicalQuerySteps;

        public IdenticalQueryStepRewriter(Dictionary<string, List<QueryQueryStep>> identicalQuerySteps)
        {
            _identicalQuerySteps = identicalQuerySteps;
        }

        public override IGraphQueryStep VisitQueryQueryStep(QueryQueryStep qqs)
        {
            var queryStr = qqs.Query.ToString();
            //This should be populated with atleast this query step
            var identicalSteps = _identicalQuerySteps[queryStr];
            if (identicalSteps.Count <= 1)
                return qqs;
            return  new ForwardedQueryStep(identicalSteps[0], qqs.GetOutputAlias());
        }
    }
}
