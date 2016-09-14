using Raven.Client.Data.Queries;

namespace Raven.Server.Documents.Queries
{
    public class DocumentQueryResult : QueryResult<Document>
    {
        public static readonly DocumentQueryResult NotModifiedResult = new DocumentQueryResult { NotModified = true };

        public bool NotModified { get; private set; }
    }
}