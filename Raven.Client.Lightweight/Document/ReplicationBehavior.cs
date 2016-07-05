// -----------------------------------------------------------------------
//  <copyright file="ReplicationBehavior.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Replication;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;

namespace Raven.Client.Document
{
    using Raven.Abstractions.Connection;

    public class ReplicationBehavior
    {
        private readonly DocumentStore documentStore;

#if !DNXCORE50
        private static readonly ILog log = LogManager.GetCurrentClassLogger();
#else
        private readonly static ILog log = LogManager.GetLogger(typeof(ReplicationBehavior));
#endif

        public ReplicationBehavior(DocumentStore documentStore)
        {
            this.documentStore = documentStore;
        }

        /// <summary>
        /// Represents an replication operation to all destination servers of an item specified by ETag
        /// </summary>
        /// <param name="etag">ETag of an replicated item</param>
        /// <param name="timeout">Optional timeout - by default, 30 seconds</param>
        /// <param name="database">The database from which to check, if null, the default database for the document store connection string</param>
        /// <param name="replicas">The min number of replicas that must have the value before we can return (or the number of destinations, if higher)</param>
        /// <returns>Task which will have the number of nodes that the caught up to the specified etag</returns>
        public async Task<int> WaitAsync(Etag etag = null, TimeSpan? timeout = null, string database = null, int replicas = 2)
        {
            etag = etag ?? documentStore.LastEtagHolder.GetLastWrittenEtag();
            if (etag == Etag.Empty || etag == null)
                return replicas; // if the etag is empty, nothing to do

            var asyncDatabaseCommands = (AsyncServerClient)documentStore.AsyncDatabaseCommands;
            database = database ?? documentStore.DefaultDatabase;
            asyncDatabaseCommands = (AsyncServerClient)asyncDatabaseCommands.ForDatabase(database);

            asyncDatabaseCommands.ForceReadFromMaster();

            var replicationDocument = await asyncDatabaseCommands.ExecuteWithReplication("GET", operationMetadata => asyncDatabaseCommands.DirectGetReplicationDestinationsAsync(operationMetadata));
            if (replicationDocument == null)
                return -1;

            var destinationsToCheck = replicationDocument.Destinations
                                                         .Where(x => x.Disabled == false && x.IgnoredClient == false)
                                                         .Select(x => string.IsNullOrEmpty(x.ClientVisibleUrl) ? x.Url.ForDatabase(x.Database) : x.ClientVisibleUrl.ForDatabase(x.Database))
                                                         .ToList();


            if (destinationsToCheck.Count == 0)
                return 0;

            int toCheck = Math.Min(replicas, destinationsToCheck.Count);

            var cts = new CancellationTokenSource();
            cts.CancelAfter(timeout ?? TimeSpan.FromSeconds(60));

            var sp = Stopwatch.StartNew();

            var sourceCommands = documentStore.AsyncDatabaseCommands.ForDatabase(database ?? documentStore.DefaultDatabase);
            var sourceUrl = documentStore.Url.ForDatabase(database ?? documentStore.DefaultDatabase);
            var sourceStatistics = await sourceCommands.GetStatisticsAsync(cts.Token);
            var sourceDbId = sourceStatistics.DatabaseId.ToString();

            var latestEtags = new ReplicatedEtagInfo[destinationsToCheck.Count];
            for (int i = 0; i < destinationsToCheck.Count; i++)
            {
                latestEtags[i]= new ReplicatedEtagInfo {DestinationUrl = destinationsToCheck[i]};
            }

            var tasks = destinationsToCheck.Select((url,index) => WaitForReplicationFromServerAsync(url, sourceUrl, sourceDbId, etag, latestEtags, index, cts.Token)).ToArray();

            try
            {
                await Task.WhenAll(tasks);
                return tasks.Length;
            }
            catch (Exception e)
            {
                var successCount = tasks.Count(x => x.IsCompleted && x.IsFaulted == false && x.IsCanceled == false);
                if (successCount >= toCheck)
                {
                    // we have nothing to do here, we replicated to at least the 
                    // number we had to check, so that is good
                    return successCount;
                }
               
                if (tasks.Any(x => x.IsFaulted) && successCount == 0)
                {
                    // there was an error here, not just cancellation, let us just let it bubble up.
                    throw;
                }

                // we have either completed (but not enough) or cancelled, meaning timeout
                var message = string.Format("Could only confirm that the specified Etag {0} was replicated to {1} of {2} servers after {3}\r\nDetails: {4}", 
                    etag,
                    successCount,
                    destinationsToCheck.Count,
                    sp.Elapsed,
                    string.Join<ReplicatedEtagInfo>("; ", latestEtags));

                if(e is OperationCanceledException)
                    throw new TimeoutException(message);

                throw new TimeoutException(message, e);
            }
        }

        private async Task WaitForReplicationFromServerAsync(string url, string sourceUrl, string sourceDbId, Etag etag, ReplicatedEtagInfo[] latestEtags, int index, CancellationToken cancellationToken)
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    var etags = await GetReplicatedEtagsFor(url, sourceUrl, sourceDbId);

                    latestEtags[index] = etags;

                    var replicated = etag.CompareTo(etags.DocumentEtag) <= 0;

                    if (replicated)
                        return;
                }
                catch (Exception e)
                {
                    log.DebugException(string.Format("Failed to get replicated etags for '{0}'.", sourceUrl), e);

                    throw;
                }

                await Task.Delay(100, cancellationToken);
            }
        }

        private async Task<ReplicatedEtagInfo> GetReplicatedEtagsFor(string destinationUrl, string sourceUrl, string sourceDbId)
        {
            var createHttpJsonRequestParams = new CreateHttpJsonRequestParams(
                null,
                destinationUrl.LastReplicatedEtagFor(sourceUrl, sourceDbId),
                "GET",
                new OperationCredentials(documentStore.ApiKey, documentStore.Credentials), 
                documentStore.Conventions);

            using (var request = documentStore.JsonRequestFactory.CreateHttpJsonRequest(createHttpJsonRequestParams))
            {
                var json = await request.ReadResponseJsonAsync();
                var etag = Etag.Parse(json.Value<string>("LastDocumentEtag"));

                if ( log.IsDebugEnabled )
                {
                    log.Debug("Received last replicated document Etag {0} from server {1}", etag, destinationUrl);
                }
                                
                return new ReplicatedEtagInfo
                {
                    DestinationUrl = destinationUrl,
                    DocumentEtag = etag 
                };
            }
        }
    }
}
