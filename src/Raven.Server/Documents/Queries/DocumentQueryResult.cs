using Raven.Abstractions.Data;

namespace Raven.Server.Documents.Queries
{
    public class DocumentQueryResult : QueryResult<Document>
    {
        public bool NotModified { get; set; }
    }
}