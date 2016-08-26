using Lucene.Net.Search;

namespace Raven.Server.Documents.Queries.Results
{
    public interface IQueryResultRetriever
    {
        Document Get(Lucene.Net.Documents.Document input, float score);

        bool TryGetKey(Lucene.Net.Documents.Document document, out string key);
    }
}