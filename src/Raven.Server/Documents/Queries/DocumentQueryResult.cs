using System;

namespace Raven.Server.Documents.Queries
{
    public class DocumentQueryResult : QueryResultServerSide
    {
        public static readonly DocumentQueryResult NotModifiedResult = new DocumentQueryResult { NotModified = true };

        public override bool SupportsInclude => true;

        public override bool SupportsExceptionHandling => false;

        public override void AddResult(Document result)
        {
            Results.Add(result);
        }

        public override void HandleException(Exception e)
        {
            throw new NotSupportedException();
        }
    }
}