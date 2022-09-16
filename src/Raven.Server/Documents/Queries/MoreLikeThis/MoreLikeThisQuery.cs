using Corax.Queries;
using Lucene.Net.Search;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries.MoreLikeThis
{
    public class CoraxMoreLikeThisQuery : MoreLikeThisQuery
    {
        public IQueryMatch BaseDocumentQuery { get; set; }

        public IQueryMatch FilterQuery { get; set; }
    }
    
    public class LuceneMoreLikeThisQuery : MoreLikeThisQuery
    {
        public Query BaseDocumentQuery { get; set; }

        public Query FilterQuery { get; set; }
    }
    
    public abstract class MoreLikeThisQuery 
    {
        public string BaseDocument { get; set; }
        
        public BlittableJsonReaderObject Options { get; set; }
    }
}
