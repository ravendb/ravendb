using Raven.Client.Data.Queries;

namespace Raven.Server.Documents.Queries
{
    public class TermsQueryResultServerSide : TermsQueryResult
    {
        public static readonly TermsQueryResultServerSide NotModifiedResult = new TermsQueryResultServerSide { NotModified = true };

        public bool NotModified { get; private set; }
    }
}