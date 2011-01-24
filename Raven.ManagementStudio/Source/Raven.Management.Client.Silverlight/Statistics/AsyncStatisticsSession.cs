namespace Raven.Management.Client.Silverlight.Statistics
{
    using System;
    using System.Net;
    using Raven.Database.Data;
    using Raven.Management.Client.Silverlight.Client;
    using Raven.Management.Client.Silverlight.Common;
    using Raven.Management.Client.Silverlight.Document;

    public class AsyncStatisticsSession : IAsyncStatisticsSession, IDisposable
    {
        public AsyncStatisticsSession(string databaseUrl)
        {
            Convention = new DocumentConvention();
            Client = new AsyncServerClient(databaseUrl, Convention, Credentials);
        }

        private IAsyncDatabaseCommands Client { get; set; }

        private DocumentConvention Convention { get; set; }

        private ICredentials Credentials { get; set; }

        #region IAsyncStatisticsSession Members

        public void Load(CallbackFunction.Load<DatabaseStatistics> callback)
        {
            this.Client.StatisticsGet(callback);
        }

        #endregion

        #region IDisposable Members

        public void Dispose()
        {
        }

        #endregion
    }
}