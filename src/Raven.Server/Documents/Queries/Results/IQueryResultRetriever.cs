namespace Raven.Server.Documents.Queries.Results
{
    public interface IQueryResultRetriever
    {
        Document Get(Lucene.Net.Documents.Document input);
    }
}