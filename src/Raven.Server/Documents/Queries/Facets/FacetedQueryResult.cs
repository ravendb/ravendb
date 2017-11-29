using System.Collections.Generic;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Facets;

namespace Raven.Server.Documents.Queries.Facets
{
    public class FacetedQueryResult : QueryResult<List<FacetResult>, List<Document>>
    {
        public static readonly FacetedQueryResult NotModifiedResult = new FacetedQueryResult { NotModified = true };

        public FacetedQueryResult()
        {
            Results = new List<FacetResult>();
            Includes = new List<Document>();
        }

        public bool NotModified { get; protected set; }
    }
}
