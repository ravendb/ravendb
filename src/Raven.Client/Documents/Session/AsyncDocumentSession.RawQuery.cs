namespace Raven.Client.Documents.Session
{
    public partial class AsyncDocumentSession
    {
        public IAsyncRawDocumentQuery<T> AsyncRawQuery<T>(string query)
        {
            var asyncDocumentQuery = new AsyncDocumentQuery<T>(this,null,null,false);
            asyncDocumentQuery.RawQuery(query);
            return asyncDocumentQuery;
        }
    }
}
