namespace Raven.Client.Documents.Session
{
    public partial class AsyncDocumentSession
    {
        /// <summary>
        /// Query the specified index using provided raw query
        /// </summary>
        /// <typeparam name="T">The query result type</typeparam>
        /// <param name="query">The raw query string</param>
        /// <returns>An async raw document query instance</returns>
        public IAsyncRawDocumentQuery<T> AsyncRawQuery<T>(string query)
        {
            var asyncDocumentQuery = new AsyncDocumentQuery<T>(this,null,null,false);
            asyncDocumentQuery.RawQuery(query);
            return asyncDocumentQuery;
        }
    }
}
