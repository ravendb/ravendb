using Lucene.Net.Store;

namespace Raven.Server.Documents.Queries.Results
{
    public interface IQueryResultRetriever
    {
        Document Get(Lucene.Net.Documents.Document input, float score, IState state);

        bool TryGetKey(Lucene.Net.Documents.Document document, IState state, out string key);
    }
}