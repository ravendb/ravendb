using Lucene.Net.Store;
using Raven.Server.Documents.Indexes.Static.Spatial;

namespace Raven.Server.Documents.Queries.Results
{
    public interface IQueryResultRetriever
    {
        Document Get(Lucene.Net.Documents.Document input, float score, IState state, int resultIndex);

        bool TryGetKey(Lucene.Net.Documents.Document document, IState state, out string key);
    }
}