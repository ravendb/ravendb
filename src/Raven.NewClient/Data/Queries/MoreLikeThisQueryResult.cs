using Sparrow.Json;

namespace Raven.NewClient.Client.Data.Queries
{
    public class MoreLikeThisQueryResult : MoreLikeThisQueryResult<BlittableJsonReaderArray>
    {
    }

    public abstract class MoreLikeThisQueryResult<T> : QueryResultBase<T>
    {
    }
}
