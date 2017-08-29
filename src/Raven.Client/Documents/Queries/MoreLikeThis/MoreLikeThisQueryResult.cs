using Sparrow.Json;

namespace Raven.Client.Documents.Queries.MoreLikeThis
{
    public class MoreLikeThisQueryResult : MoreLikeThisQueryResult<BlittableJsonReaderArray, BlittableJsonReaderObject>
    {
    }

    public abstract class MoreLikeThisQueryResult<TResult, TInclude> : QueryResultBase<TResult, TInclude>
    {
        /// <summary>
        /// The duration of actually executing the query server side
        /// </summary>
        public long DurationInMs { get; set; }
    }
}
