namespace Raven.Client.Documents.Session
{
    public partial class DocumentSession
    {
        public IRawDocumentQuery<T> RawQuery<T>(string query)
        {
            var documentQuery = new DocumentQuery<T>(this, null, null, false);
            documentQuery.RawQuery(query);
            return documentQuery;
        }
    }
}
