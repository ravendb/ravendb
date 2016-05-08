using Raven.Client.Data.Queries;

namespace Raven.Server.Documents.Queries.MoreLikeThis
{
    public class MoreLikeThisQueryResultServerSide : MoreLikeThisQueryResult<Document>
    {
        public bool NotModified { get; set; }
    }
}