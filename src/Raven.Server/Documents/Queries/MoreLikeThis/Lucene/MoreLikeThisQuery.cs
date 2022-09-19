using Lucene.Net.Search;

namespace Raven.Server.Documents.Queries.MoreLikeThis.Lucene;

public class MoreLikeThisQuery : MoreLikeThisQueryBase
{
    public Query BaseDocumentQuery { get; set; }

    public Query FilterQuery { get; set; }
}
