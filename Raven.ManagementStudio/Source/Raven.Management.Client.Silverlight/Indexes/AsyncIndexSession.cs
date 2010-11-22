namespace Raven.Management.Client.Silverlight.Indexes
{
    using System;
    using System.Net;
    using Raven.Database.Data;
    using Raven.Management.Client.Silverlight.Client;
    using Raven.Management.Client.Silverlight.Common;
    using Raven.Management.Client.Silverlight.Document;

    public class AsyncIndexSession : IAsyncIndexSession, IDisposable
    {
        public AsyncIndexSession(string databaseUrl)
        {
            Convention = new DocumentConvention();
            Client = new AsyncServerClient(databaseUrl, Convention, Credentials);
        }

        private IAsyncDatabaseCommands Client { get; set; }

        private DocumentConvention Convention { get; set; }

        private ICredentials Credentials { get; set; }

        #region IAsyncIndexSession Members

        public void Query(string index, IndexQuery query, string[] includes, CallbackFunction.Load<QueryResult> callback)
        {
            Client.Query(index, query, includes, callback);
        }

        #endregion

        #region IDisposable Members

        public void Dispose()
        {
        }

        #endregion
    }
}