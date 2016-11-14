using Raven.NewClient.Json.Linq;

namespace Raven.NewClient.Client.Data.Queries
{
    public class MoreLikeThisQueryResult : MoreLikeThisQueryResult<RavenJObject>
    {
    }

    public abstract class MoreLikeThisQueryResult<T> : QueryResultBase<T>
    {
    }
}
