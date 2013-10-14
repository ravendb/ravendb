// -----------------------------------------------------------------------
//  <copyright file="ReplicationBehavior.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Replication;
using Raven.Client.Connection;
using Raven.Client.Util;

namespace Raven.Client.Document
{
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
		/// <param name="timeout">Optional timeout</param>
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
			                                             .Where(
				                                             x => x.Disabled == false && x.IgnoredClient == false)
			                                             .Select(x => x.ClientVisibleUrl ?? x.Url)
			                                             .ToList();


			if (destinationsToCheck.Count == 0)
				return 0;

			int toCheck = Math.Min(replicas, destinationsToCheck.Count);

			var countDown = new AsyncCountdownEvent(toCheck);
			var errors = new BlockingCollection<Exception>();

			foreach (var url in destinationsToCheck)
			{
				WaitForReplicationFromServerAsync(url, countDown, etag, errors);
			}

			if (await countDown.WaitAsync().WaitWithTimeout(timeout) == false)
			{
				throw new TimeoutException(
					string.Format("Confirmed that the specified etag {0} was replicated to {1} of {2} servers, during {3}", etag,
					              (toCheck - countDown.Count),
					              toCheck,
					              timeout));
			}

			if (errors.Count > 0 && countDown.Count > 0)
				throw new AggregateException(errors);

			return countDown.Count;
		}

		private async void WaitForReplicationFromServerAsync(string url, AsyncCountdownEvent countDown, Etag etag, BlockingCollection<Exception> errors)
		{
			try
			{
				while (countDown.Active)
				{
					var etags = await GetReplicatedEtagsFor(url);

					var replicated = etag.CompareTo(etags.DocumentEtag) <= 0 || etag.CompareTo(etags.AttachmentEtag) <= 0;

					if (!replicated)
					{
						if (countDown.Active)
						{
#if SILVERLIGHT
							await TaskEx.Delay(100);
#else
							await Task.Delay(100);
#endif
						}
						continue;
					}
					countDown.Signal();
					return;
				}
			}
			catch (Exception ex)
			{
				errors.Add(ex);
				countDown.Error();
			}
		}

		private async Task<ReplicatedEtagInfo> GetReplicatedEtagsFor(string destinationUrl)
		{
			var createHttpJsonRequestParams = new CreateHttpJsonRequestParams(
				null,
				destinationUrl.LastReplicatedEtagFor(documentStore.Url),
				"GET",
				documentStore.Credentials,
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
