namespace Raven.Client.Documents.Session
{
    public partial class DocumentSession
    {
#pragma warning disable CS0618
        public IGraphQuery<T> GraphQuery<T>(string query)
        {
            var documentQuery = new DocumentQuery<T>(this, null, null, false);
            documentQuery.GraphQuery(query);
            return documentQuery;
        }
#pragma warning restore CS0618
    }
}
