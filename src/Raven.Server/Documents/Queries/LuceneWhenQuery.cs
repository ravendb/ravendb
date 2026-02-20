namespace Raven.Server.Documents.Queries;

public class LuceneWhenQuery : Lucene.Net.Search.Query
{
    public override string ToString(string field)
    {
        return nameof(LuceneWhenQuery);
    }
}
