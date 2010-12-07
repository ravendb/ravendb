namespace Raven.Management.Client.Silverlight.Indexes
{
    using System;
    using System.Collections.Generic;
    using System.Net;
    using Raven.Database.Data;
    using Raven.Database.Indexing;
    using Raven.Management.Client.Silverlight.Client;
    using Raven.Management.Client.Silverlight.Common;
    using Raven.Management.Client.Silverlight.Document;

    public class AsyncIndexSession : IAsyncIndexSession, IDisposable
    {
        public AsyncIndexSession(string databaseUrl)
        {
            this.Convention = new DocumentConvention();
            this.Client = new AsyncServerClient(databaseUrl, this.Convention, this.Credentials);
        }

        private IAsyncDatabaseCommands Client { get; set; }

        private DocumentConvention Convention { get; set; }

        private ICredentials Credentials { get; set; }

        #region IAsyncIndexSession Members

        public void Query(string index, IndexQuery query, string[] includes, CallbackFunction.Load<QueryResult> callback)
        {
            this.Client.Query(index, query, includes, callback);
        }

        public void LinearQuery(string query, int start, int pageSize, CallbackFunction.Load<IList<Database.JsonDocument>> callback)
        {
            Client.LinearQuery(query, start, pageSize, callback);
        }

        public void LoadMany(CallbackFunction.Load<IDictionary<string, IndexDefinition>> callback)
        {
            this.Client.IndexGetMany(null, null, callback);
        }

        public void Save(string name, IndexDefinition definition, CallbackFunction.SaveOne<KeyValuePair<string, IndexDefinition>> callback)
        {
            this.Client.IndexPut(name, definition, callback);
        }

        public void Delete(string name, CallbackFunction.SaveOne<string> callback)
        {
            this.Client.IndexDelete(name, callback);
        }

        #endregion

        #region IDisposable Members

        public void Dispose()
        {
        }

        #endregion
    }
}