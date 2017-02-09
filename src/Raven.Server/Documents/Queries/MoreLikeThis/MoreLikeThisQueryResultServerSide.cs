using System.Collections.Generic;
using Raven.NewClient.Client.Data.Queries;

namespace Raven.Server.Documents.Queries.MoreLikeThis
{
    public class MoreLikeThisQueryResultServerSide : MoreLikeThisQueryResult<List<Document>>
    {
        public static readonly MoreLikeThisQueryResultServerSide NotModifiedResult = new MoreLikeThisQueryResultServerSide { NotModified = true };

        public bool NotModified { get; private set; }
    }
}