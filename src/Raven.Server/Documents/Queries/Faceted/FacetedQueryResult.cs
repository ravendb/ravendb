using System.Collections.Generic;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Queries;

namespace Raven.Server.Documents.Queries.Faceted
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
