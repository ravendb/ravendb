using Raven.Json.Linq;

namespace Raven.Client.Data.Queries
{
    public class MoreLikeThisQueryResult : MoreLikeThisQueryResult<RavenJObject>
    {
    }

    public abstract class MoreLikeThisQueryResult<T> : QueryResultBase<T>
    {
    }
}
