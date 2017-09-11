using System;
using System.Collections.Generic;
using System.Text;
using Raven.Client.Documents.Indexes;

namespace Raven.Client.Documents.Session
{
    public partial class AsyncDocumentSession
    {
        public IAsyncRawDocumentQuery<T> AsyncRawQuery<T>(string query, string indexName = null)
        {
            var asyncDocumentQuery = new AsyncDocumentQuery<T>(this,indexName,null,false);
            asyncDocumentQuery.RawQuery(query);
            return asyncDocumentQuery;
        }

        public IAsyncRawDocumentQuery<T> AsyncRawQuery<T, TIndexCreator>(string query) where TIndexCreator : AbstractIndexCreationTask, new()
        {
            var index = new TIndexCreator();
            return AsyncRawQuery<T>(query, index.IndexName);
        }
    }
}
