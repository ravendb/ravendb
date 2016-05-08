using Raven.Abstractions.Data;
using Raven.Client.Data;
using Raven.Client.Data.Queries;

namespace Raven.Server.Documents.Queries
{
    public class DocumentQueryResult : QueryResult<Document>
    {
        public bool NotModified { get; set; }
    }
}