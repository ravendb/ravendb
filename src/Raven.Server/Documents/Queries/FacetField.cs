using System.Collections.Generic;
using Raven.Client.Documents.Queries.Facets;
using Raven.Server.Documents.Queries.AST;

namespace Raven.Server.Documents.Queries
{
    public class FacetField : SelectField
    {
        public Dictionary<FacetAggregation, string> Aggregations;

        public List<QueryExpression> Ranges;

        public string FacetSetupDocumentId;

        public FacetField()
        {
            IsFacet = true;
            Aggregations = new Dictionary<FacetAggregation, string>();
            Ranges = new List<QueryExpression>();
        }

        public void AddAggregation(FacetAggregation aggregation, QueryFieldName name)
        {
            Aggregations[aggregation] = name;
        }
    }
}
