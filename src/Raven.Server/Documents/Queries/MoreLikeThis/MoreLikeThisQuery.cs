using Lucene.Net.Search;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries.MoreLikeThis
{
    public class MoreLikeThisQuery
    {
        public string BaseDocument { get; set; }

        public Query BaseDocumentQuery { get; set; }

        public Query FilterQuery { get; set; }

        public BlittableJsonReaderObject Options { get; set; }
    }
}
