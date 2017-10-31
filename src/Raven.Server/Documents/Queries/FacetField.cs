using System.Collections.Generic;
using Raven.Client.Documents.Queries.Facets;

namespace Raven.Server.Documents.Queries
{
    public class FacetField : SelectField
    {
        public Dictionary<FacetAggregation, string> Aggregations;

        public FacetField()
        {
            IsFacet = true;
            Aggregations = new Dictionary<FacetAggregation, string>();
        }

        public void AddAggregation(FacetAggregation aggregation, QueryFieldName name)
        {
            Aggregations[aggregation] = name;
        }
    }
}
