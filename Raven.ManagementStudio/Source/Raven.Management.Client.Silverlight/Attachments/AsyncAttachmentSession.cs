namespace Raven.Management.Client.Silverlight.Attachments
{
    using System;
    using System.Collections.Generic;
    using System.Net;
    using System.Threading;
    using Client;
    using Common;
    using Database.Data;
    using Document;
    using System.Linq;

    public class AsyncAttachmentSession : IAsyncAttachmentSession
    {
        public AsyncAttachmentSession(string databaseAddress)
        {
            Convention = new DocumentConvention();

            Client = new AsyncServerClient(databaseAddress, Convention, Credentials);
        }

        private IAsyncDatabaseCommands Client { get; set; }

        private DocumentConvention Convention { get; set; }

        private ICredentials Credentials { get; set; }

        #region IAsyncAttachmentSession Members

        public void Load(string key, CallbackFunction.Load<KeyValuePair<string, Attachment>> callback)
        {
            Client.AttachmentGet(key, callback);
        }

        public void LoadMany(CallbackFunction.Load<IList<KeyValuePair<string, Attachment>>> callback)
        {
            Client.AttachmentGetAll(callback);
        }

        public void LoadPlugins(CallbackFunction.Load<IList<KeyValuePair<string, Attachment>>> callback)
        {
            var context = SynchronizationContext.Current;

            Client.AttachmentGetAll((result) => context.Post(delegate
                                                                 {
                                                                     callback.Invoke(new LoadResponse<IList<KeyValuePair<string, Attachment>>>()
                                                                                         {
                                                                                             Data = result.Data.Where(x => x.Key.EndsWith(".xap", StringComparison.InvariantCultureIgnoreCase)).ToList(),
                                                                                             Exception = result.Exception,
                                                                                             StatusCode = result.StatusCode
                                                                                         });
                                                                 }
                                                             , null));
        }

        #endregion
    }
}