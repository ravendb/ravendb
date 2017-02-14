using Sparrow.Json;

namespace Raven.Client.Documents.Queries.MoreLikeThis
{
    public class MoreLikeThisQueryResult : MoreLikeThisQueryResult<BlittableJsonReaderArray>
    {
    }

    public abstract class MoreLikeThisQueryResult<T> : QueryResultBase<T>
    {
    }
}
