// -----------------------------------------------------------------------
//  <copyright file="DatabaseSmugglerRemoteBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Net;
using System.Threading.Tasks;

using Raven.Abstractions.Connection;
using Raven.Abstractions.Database.Smuggler.Database;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Json.Linq;

namespace Raven.Smuggler.Database.Remote
{
    public abstract class DatabaseSmugglerRemoteBase
    {
        protected async Task InitializeBatchSizeAsync(DocumentStore store, DatabaseSmugglerOptions options)
        {
            if (store.HasJsonRequestFactory == false)
                return;

            var url = store.Url.ForDatabase(store.DefaultDatabase) + "/debug/config";
            try
            {
                using (var request = store.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, url, HttpMethods.Get, store.DatabaseCommands.PrimaryCredentials, store.Conventions)))
                {
                    var configuration = (RavenJObject)await request.ReadResponseJsonAsync().ConfigureAwait(false);

                    var maxNumberOfItemsToProcessInSingleBatch = configuration["Core"].Value<int>("MaxNumberOfItemsToProcessInSingleBatch");
                    if (maxNumberOfItemsToProcessInSingleBatch <= 0)
                        return;

                    var current = options.BatchSize;
                    options.BatchSize = Math.Min(current, maxNumberOfItemsToProcessInSingleBatch);
                }
            }
            catch (ErrorResponseException e)
            {
                if (e.StatusCode == HttpStatusCode.Forbidden) // let it continue with the user defined batch size
                    return;

                throw;
            }
        }
    }
}
