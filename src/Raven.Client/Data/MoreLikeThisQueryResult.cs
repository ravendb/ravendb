namespace Raven.Abstractions.Data
{
    public class MoreLikeThisQueryResult
    {
        public MoreLikeThisQueryResult()
        {
            
        }

        public LoadResult Result { get; set; }
        public long? Etag { get; set; }
    }
}
