//-----------------------------------------------------------------------
// <copyright file="ExpiredDocumentsCleaner.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Linq;
using Raven.Database.Plugins;

namespace Raven.Bundles.Expiration
{
    public class ExpiredDocumentsCleaner : AbstractBackgroundTask
    {
        private const string RavenDocumentsByExpirationDate = "Raven/DocumentsByExpirationDate";

        protected override void Initialize()
        {
            var indexDefinition = Database.GetIndexDefinition(RavenDocumentsByExpirationDate);
            if (indexDefinition != null)
                return;

            Database.PutIndex(RavenDocumentsByExpirationDate,
                              new IndexDefinition
                              {
                                  Map = @"
    from doc in docs
    let expiry = doc[""@metadata""][""Raven-Expiration-Date""]
    where expiry != null
    select new { Expiry = expiry }
"});
        }

        protected override bool HandleWork()
        {
            var currentTime = ExpirationReadTrigger.GetCurrentUtcDate();
            var nowAsStr = DateTools.DateToString(currentTime, DateTools.Resolution.SECOND);
            int retries = 0;
            QueryResult queryResult;
            do
            {
                queryResult = Database.Query(RavenDocumentsByExpirationDate, new IndexQuery
                {
                    Cutoff = currentTime,
                    Query = "Expiry:[* TO "+nowAsStr+"]",
                    FieldsToFetch = new []{"__document_id"}
                });

                if(queryResult.IsStale)
                {
                    if (++retries > 25)
                        return false;
                    Thread.Sleep(100);
                }
            } while (queryResult.IsStale );

            foreach (var result in queryResult.Results)
            {
                var docId = result.Value<string>("__document_id");
                Database.Delete(docId, null, null);
            }

            return queryResult.Results.Count > 0;
        }
    }
}
