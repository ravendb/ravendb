namespace Raven.Management.Client.Silverlight.Collections
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using Newtonsoft.Json;
    using Raven.Database;
    using Raven.Database.Data;
    using Raven.Management.Client.Silverlight.Common;
    using Raven.Management.Client.Silverlight.Indexes;

    public class AsyncCollectionSession : IAsyncCollectionSession, IDisposable
    {
        public AsyncCollectionSession(string databaseUrl)
        {
            IndexSession = new AsyncIndexSession(databaseUrl);
        }

        private IAsyncIndexSession IndexSession { get; set; }

        #region IAsyncCollectionSession Members

        public void Load(string collectionName, CallbackFunction.Load<IList<JsonDocument>> callback)
        {
            Guard.Assert(() => !string.IsNullOrEmpty(collectionName));
            Guard.Assert(() => callback != null);

            SynchronizationContext context = SynchronizationContext.Current;

            IndexSession.Query("Raven/DocumentsByEntityName", new IndexQuery {Query = string.Format("Tag:{0}", collectionName)}, null, (result) =>
                                                                                                                                           {
                                                                                                                                               var documents = new List<JsonDocument>();

                                                                                                                                               if (result.IsSuccess)
                                                                                                                                               {
                                                                                                                                                   var jsonSerializer = new JsonSerializer();
                                                                                                                                                   documents =
                                                                                                                                                       result.Data.Results.Select(
                                                                                                                                                           document =>
                                                                                                                                                           (JsonDocument) jsonSerializer.Deserialize(document.CreateReader(), typeof (JsonDocument))).
                                                                                                                                                           ToList();
                                                                                                                                               }

                                                                                                                                               var loadResponse = new LoadResponse<IList<JsonDocument>>
                                                                                                                                                                      {
                                                                                                                                                                          Data = documents,
                                                                                                                                                                          Exception = result.Exception,
                                                                                                                                                                          StatusCode = result.StatusCode
                                                                                                                                                                      };

                                                                                                                                               context.Post(delegate { callback.Invoke(loadResponse); },
                                                                                                                                                            null);
                                                                                                                                           });
        }

        #endregion

        #region IDisposable Members

        public void Dispose()
        {
        }

        #endregion
    }
}