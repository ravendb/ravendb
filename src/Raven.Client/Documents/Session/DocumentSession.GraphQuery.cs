namespace Raven.Client.Documents.Session
{
    public partial class DocumentSession
    {
        public IGraphQuery<T> GraphQuery<T>(string query)
        {
            var documentQuery = new DocumentQuery<T>(this, null, null, false);
            documentQuery.GraphQuery(query);
            return documentQuery;
        }
    }
}
