namespace Raven.Abstractions.Data
{
    public class MoreLikeThisQueryResult
    {
        public MoreLikeThisQueryResult()
        {
            
        }

        public MultiLoadResult Result { get; set; }
        public long? Etag { get; set; }
    }
}
