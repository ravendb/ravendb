using Raven.Client.Data.Queries;

namespace Raven.Server.Documents.Queries
{
    public class DocumentQueryResult : QueryResultServerSide
    {
        public static readonly DocumentQueryResult NotModifiedResult = new DocumentQueryResult { NotModified = true };

        public override bool SupportsInclude => true;

        public override void AddResult(Document result)
        {
            Results.Add(result);
        }
    }
}