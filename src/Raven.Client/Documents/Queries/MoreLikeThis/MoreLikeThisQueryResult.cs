using Sparrow.Json;

namespace Raven.Client.Documents.Queries.MoreLikeThis
{
    public class MoreLikeThisQueryResult : MoreLikeThisQueryResult<BlittableJsonReaderArray>
    {
    }

    public abstract class MoreLikeThisQueryResult<T> : QueryResultBase<T>
    {
        /// <summary>
        /// The duration of actually executing the query server side
        /// </summary>
        public long DurationMilliseconds { get; set; }
    }
}
