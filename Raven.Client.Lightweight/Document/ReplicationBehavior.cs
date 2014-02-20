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
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Replication;
using Raven.Client.Connection;

namespace Raven.Client.Document
{
	using Raven.Abstractions.Connection;

	public class ReplicationBehavior
	{
		private readonly DocumentStore documentStore;

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
			if (etag == Etag.Empty)
				return replicas; // if the etag is empty, nothing to do

			var asyncDatabaseCommands = documentStore.AsyncDatabaseCommands;
			if (database != null)
				asyncDatabaseCommands = asyncDatabaseCommands.ForDatabase(database);

			asyncDatabaseCommands.ForceReadFromMaster();

			var doc = await asyncDatabaseCommands.GetAsync("Raven/Replication/Destinations");
			if (doc == null)
				return -1;

			var replicationDocument = doc.DataAsJson.JsonDeserialization<ReplicationDocument>();
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
            cts.CancelAfter(timeout ?? TimeSpan.FromSeconds(30));

            var sp = Stopwatch.StartNew();

			var tasks = destinationsToCheck.Select(url => WaitForReplicationFromServerAsync(url, database, etag, cts.Token)).ToArray();

		    try
		    {

#if !SILVERLIGHT
                await Task.WhenAll(tasks);
#else
		        await TaskEx.WhenAll(tasks);
#endif

		        return tasks.Length;
		    }
		    catch (Exception e)
		    {
		        var completedCount = tasks.Count(x => x.IsCompleted && x.IsFaulted == false);
		        if (completedCount >= toCheck)
		        {
		            // we have nothing to do here, we replicated to at least the 
                    // number we had to check, so that is good
			        return completedCount;
		        }
			    if (tasks.Any(x => x.IsFaulted) && completedCount == 0)
			    {
				    // there was an error here, not just cancellation, let us just let it bubble up.
				    throw;
			    }

			    // we have either completed (but not enough) or cancelled, meaning timeout
		        var message = string.Format("Confirmed that the specified etag {0} was replicated to {1} of {2} servers after {3}", etag,
		            (toCheck - completedCount),
		            toCheck,
                    sp.Elapsed);

			    throw new TimeoutException(message, e);
		    }
		}

		private async Task WaitForReplicationFromServerAsync(string url, string database, Etag etag, CancellationToken cancellationToken)
		{
		    while (true)
		    {
		        cancellationToken.ThrowIfCancellationRequested();

		        var etags = await GetReplicatedEtagsFor(url, database);

		        var replicated = etag.CompareTo(etags.DocumentEtag) <= 0 || etag.CompareTo(etags.AttachmentEtag) <= 0;

		        if (replicated)
		            return;

#if !SILVERLIGHT
                await Task.Delay(100, cancellationToken);
#else
                await TaskEx.Delay(100, cancellationToken);
#endif   
		    }
		}

	    private async Task<ReplicatedEtagInfo> GetReplicatedEtagsFor(string destinationUrl, string database)
		{
			var createHttpJsonRequestParams = new CreateHttpJsonRequestParams(
				null,
				destinationUrl.LastReplicatedEtagFor(documentStore.Url.ForDatabase(database ?? documentStore.DefaultDatabase)),
				"GET",
				new OperationCredentials(documentStore.ApiKey, documentStore.Credentials), 
				documentStore.Conventions);
			var httpJsonRequest = documentStore.JsonRequestFactory.CreateHttpJsonRequest(createHttpJsonRequestParams);
			var json = await httpJsonRequest.ReadResponseJsonAsync();

			return new ReplicatedEtagInfo
			{
				DestinationUrl = destinationUrl,
				DocumentEtag = Etag.Parse(json.Value<string>("LastDocumentEtag")),
				AttachmentEtag = Etag.Parse(json.Value<string>("LastAttachmentEtag"))
			};
		}
	}
}
