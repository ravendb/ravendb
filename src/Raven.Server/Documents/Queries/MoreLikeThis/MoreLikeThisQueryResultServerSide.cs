using System.Collections.Generic;
using Raven.Client.Data.Queries;

namespace Raven.Server.Documents.Queries.MoreLikeThis
{
    public class MoreLikeThisQueryResultServerSide : MoreLikeThisQueryResult<List<Document>>
    {
        public static readonly MoreLikeThisQueryResultServerSide NotModifiedResult = new MoreLikeThisQueryResultServerSide { NotModified = true };

        public bool NotModified { get; private set; }

        public MoreLikeThisQueryResultServerSide()
        {
            Results = new List<Document>();
            Includes = new List<Document>();
        }
    }
}