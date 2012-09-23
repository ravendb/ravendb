//-----------------------------------------------------------------------
// <copyright file="ReplicationTask.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Replication;
using Raven.Bundles.Replication.Data;
using Raven.Database;
using Raven.Database.Data;
using Raven.Database.Impl;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Bundles.Replication.Tasks
{
	[ExportMetadata("Bundle", "Replication")]
	[InheritedExport(typeof(IStartupTask))]
	public class ReplicationTask : IStartupTask
	{
		public class IntHolder
		{
			public int Value;
		}

		private DocumentDatabase docDb;
		private readonly ILog log = LogManager.GetCurrentClassLogger();
		private bool firstTimeFoundNoReplicationDocument = true;
		private readonly ConcurrentDictionary<string, IntHolder> activeReplicationTasks = new ConcurrentDictionary<string, IntHolder>();

		private int replicationAttempts;
		private int workCounter;
		private HttpRavenRequestFactory httpRavenRequestFactory;
		public void Execute(DocumentDatabase database)
		{
			docDb = database;
			var replicationRequestTimeoutInMs =
				docDb.Configuration.GetConfigurationValue<int>("Raven/Replication/ReplicationRequestTimeout") ??
				60 * 1000;

			httpRavenRequestFactory = new HttpRavenRequestFactory { RequestTimeoutInMs = replicationRequestTimeoutInMs };

			var thread = new Thread(Execute)
			{
				IsBackground = true,
				Name = "Replication Thread"
			};
			var disposableAction = new DisposableAction(thread.Join);
			// make sure that the doc db waits for the replication thread shutdown
			docDb.ExtensionsState.GetOrAdd(Guid.NewGuid().ToString(), s => disposableAction);
			thread.Start();
		}

		private void Execute()
		{
			var name = GetType().Name;

			var timeToWaitInMinutes = TimeSpan.FromMinutes(5);
			bool runningBecauseOfDataModifications = false;
			var context = docDb.WorkContext;
			while (context.DoWork)
			{
				try
				{
					using (docDb.DisableAllTriggersForCurrentThread())
					{
						var destinations = GetReplicationDestinations();

						if (destinations.Length == 0)
						{
							WarnIfNoReplicationTargetsWereFound();
						}
						else
						{
							var currentReplicationAttempts = Interlocked.Increment(ref replicationAttempts);

							var copyOfrunningBecauseOfDataModifications = runningBecauseOfDataModifications;
							var destinationForReplication = destinations
								.Where(dest =>
								{
									if (copyOfrunningBecauseOfDataModifications == false)
										return true;
									return IsNotFailing(dest, currentReplicationAttempts);
								});

							foreach (var dest in destinationForReplication)
							{
								var destination = dest;
								var holder = activeReplicationTasks.GetOrAdd(destination.ConnectionStringOptions.Url, new IntHolder());
								if (Thread.VolatileRead(ref holder.Value) == 1)
									continue;
								Thread.VolatileWrite(ref holder.Value, 1);
								Task.Factory.StartNew(() => ReplicateTo(destination), TaskCreationOptions.LongRunning)
									.ContinueWith(completedTask =>
									{
										if (completedTask.Exception != null)
										{
											log.ErrorException("Could not replicate to " + destination, completedTask.Exception);
											return;
										}
										if (completedTask.Result) // force re-evaluation of replication again
											docDb.WorkContext.NotifyAboutWork();
									});

							}
						}
					}
				}
				catch (Exception e)
				{
					log.ErrorException("Failed to perform replication", e);
				}

				runningBecauseOfDataModifications = context.WaitForWork(timeToWaitInMinutes, ref workCounter, name);
				timeToWaitInMinutes = runningBecauseOfDataModifications
										? TimeSpan.FromSeconds(30)
										: TimeSpan.FromMinutes(5);
			}
		}

		private bool IsNotFailing(ReplicationStrategy dest, int currentReplicationAttempts)
		{
			var jsonDocument = docDb.Get(Constants.RavenReplicationDestinationsBasePath + EscapeDestinationName(dest), null);
			if (jsonDocument == null)
				return true;
			var failureInformation = jsonDocument.DataAsJson.JsonDeserialization<DestinationFailureInformation>();
			if (failureInformation.FailureCount > 1000)
			{
				var shouldReplicateTo = currentReplicationAttempts % 10 == 0;
				log.Debug("Failure count for {0} is {1}, skipping replication: {2}",
					dest, failureInformation.FailureCount, shouldReplicateTo == false);
				return shouldReplicateTo;
			}
			if (failureInformation.FailureCount > 100)
			{
				var shouldReplicateTo = currentReplicationAttempts % 5 == 0;
				log.Debug("Failure count for {0} is {1}, skipping replication: {2}",
					dest, failureInformation.FailureCount, shouldReplicateTo == false);
				return shouldReplicateTo;
			}
			if (failureInformation.FailureCount > 10)
			{
				var shouldReplicateTo = currentReplicationAttempts % 2 == 0;
				log.Debug("Failure count for {0} is {1}, skipping replication: {2}",
					dest, failureInformation.FailureCount, shouldReplicateTo == false);
				return shouldReplicateTo;
			}
			return true;
		}

		private static string EscapeDestinationName(ReplicationStrategy dest)
		{
			return Uri.EscapeDataString(dest.ConnectionStringOptions.Url.Replace("http://", "").Replace("/", "").Replace(":", ""));
		}

		private void WarnIfNoReplicationTargetsWereFound()
		{
			if (firstTimeFoundNoReplicationDocument)
			{
				firstTimeFoundNoReplicationDocument = false;
				log.Warn(
					"Replication bundle is installed, but there is no destination in 'Raven/Replication/Destinations'.\r\nRepliaction results in NO-OP");
			}
		}

		private bool ReplicateTo(ReplicationStrategy destination)
		{
			try
			{
				if (docDb.Disposed)
					return false;
				using (docDb.DisableAllTriggersForCurrentThread())
				{
					SourceReplicationInformation destinationsReplicationInformationForSource;
					try
					{
						destinationsReplicationInformationForSource = GetLastReplicatedEtagFrom(destination);
						if (destinationsReplicationInformationForSource == null)
							return false;
					}
					catch (Exception e)
					{
						log.WarnException("Failed to replicate to: " + destination, e);
						return false;
					}

					bool? replicated = null;
					switch (ReplicateDocuments(destination, destinationsReplicationInformationForSource))
					{
						case true:
							replicated = true;
							break;
						case false:
							return false;
					}

					switch (ReplicateAttachments(destination, destinationsReplicationInformationForSource))
					{
						case true:
							replicated = true;
							break;
						case false:
							return false;
					}

					return replicated ?? false;
				}
			}
			finally
			{
				var holder = activeReplicationTasks.GetOrAdd(destination.ConnectionStringOptions.Url, new IntHolder());
				Thread.VolatileWrite(ref holder.Value, 0);
			}
		}

		private bool? ReplicateAttachments(ReplicationStrategy destination, SourceReplicationInformation destinationsReplicationInformationForSource)
		{
			var tuple = GetAttachments(destinationsReplicationInformationForSource, destination);
			var attachments = tuple.Item1;

			if (attachments == null || attachments.Length == 0)
			{
				if (tuple.Item2 != destinationsReplicationInformationForSource.LastDocumentEtag)
				{
					SetLastReplicatedEtagForDocuments(destination, lastAttachmentEtag: tuple.Item2);
				}
				return null;
			}

			if (TryReplicationAttachments(destination, attachments) == false)// failed to replicate, start error handling strategy
			{
				if (IsFirstFailue(destination))
				{
					log.Info(
						"This is the first failure for {0}, assuming transient failure and trying again",
						destination);
					if (TryReplicationAttachments(destination, attachments))// success on second fail
						return true;
				}
				IncrementFailureCount(destination);
				return false;
			}
			ResetFailureCount(destination);

			return true;
		}

		private bool? ReplicateDocuments(ReplicationStrategy destination, SourceReplicationInformation destinationsReplicationInformationForSource)
		{
			var tuple = GetJsonDocuments(destinationsReplicationInformationForSource, destination);
			var jsonDocuments = tuple.Item1;
			if (jsonDocuments == null || jsonDocuments.Length == 0)
			{
				if(tuple.Item2 != destinationsReplicationInformationForSource.LastDocumentEtag)
				{
					SetLastReplicatedEtagForDocuments(destination, lastDocEtag: tuple.Item2);
				}
				return null;
			}
			if (TryReplicationDocuments(destination, jsonDocuments) == false)// failed to replicate, start error handling strategy
			{
				if (IsFirstFailue(destination))
				{
					log.Info(
						"This is the first failure for {0}, assuming transient failure and trying again",
						destination);
					if (TryReplicationDocuments(destination, jsonDocuments))// success on second fail
						return true;
				}
				IncrementFailureCount(destination);
				return false;
			}
			ResetFailureCount(destination);
			return true;
		}

		private void SetLastReplicatedEtagForDocuments(ReplicationStrategy destination, Guid? lastDocEtag = null, Guid? lastAttachmentEtag = null)
		{
			try
			{
				var url = destination.ConnectionStringOptions.Url + "/replication/lastEtag?from=" + UrlEncodedServerUrl() +
				          "&dbid=" + docDb.TransactionalStorage.Id;
				if(lastDocEtag != null)
					url += "&docEtag=" + lastDocEtag.Value;
				if(lastAttachmentEtag != null)
					url += "&attachmentEtag=" + lastAttachmentEtag.Value;

				var request = httpRavenRequestFactory.Create(url, "PUT", destination.ConnectionStringOptions);
				request.ExecuteRequest();
			}
			catch (WebException e)
			{
				var response = e.Response as HttpWebResponse;
				if (response != null && (response.StatusCode == HttpStatusCode.BadRequest || response.StatusCode == HttpStatusCode.NotFound))
					log.WarnException("Replication is not enabled on: " + destination, e);
				else
					log.WarnException("Failed to contact replication destination: " + destination, e);
			}
			catch (Exception e)
			{
				log.WarnException("Failed to contact replication destination: " + destination, e);
			}
		}

		private void IncrementFailureCount(ReplicationStrategy destination)
		{
			var jsonDocument = docDb.Get(Constants.RavenReplicationDestinationsBasePath + EscapeDestinationName(destination), null);
			var failureInformation = new DestinationFailureInformation { Destination = destination.ConnectionStringOptions.Url };
			if (jsonDocument != null)
			{
				failureInformation = jsonDocument.DataAsJson.JsonDeserialization<DestinationFailureInformation>();
			}
			failureInformation.FailureCount += 1;
			docDb.Put(Constants.RavenReplicationDestinationsBasePath + EscapeDestinationName(destination), null,
					  RavenJObject.FromObject(failureInformation), new RavenJObject(), null);
		}

		private void ResetFailureCount(ReplicationStrategy destination)
		{
			docDb.Delete(Constants.RavenReplicationDestinationsBasePath + EscapeDestinationName(destination), null,
						 null);
		}

		private bool IsFirstFailue(ReplicationStrategy destination)
		{
			var jsonDocument = docDb.Get(Constants.RavenReplicationDestinationsBasePath + EscapeDestinationName(destination), null);
			if (jsonDocument == null)
				return true;
			var failureInformation = jsonDocument.DataAsJson.JsonDeserialization<DestinationFailureInformation>();
			return failureInformation.FailureCount == 0;
		}

		private bool TryReplicationAttachments(ReplicationStrategy destination, RavenJArray jsonAttachments)
		{
			try
			{
				var url = destination.ConnectionStringOptions.Url + "/replication/replicateAttachments?from=" + UrlEncodedServerUrl();

				var sp = Stopwatch.StartNew();
				var request = httpRavenRequestFactory.Create(url, "POST", destination.ConnectionStringOptions);

				request.WebRequest.Headers.Add("Attachment-Ids", string.Join(", ", jsonAttachments.Select(x => x.Value<string>("@id"))));

				request.WriteBson(jsonAttachments);
				request.ExecuteRequest();
				log.Info("Replicated {0} attachments to {1} in {2:#,#;;0} ms", jsonAttachments.Length, destination, sp.ElapsedMilliseconds);
				return true;
			}
			catch (WebException e)
			{
				var response = e.Response as HttpWebResponse;
				if (response != null)
				{
					using (var streamReader = new StreamReader(response.GetResponseStreamWithHttpDecompression()))
					{
						var error = streamReader.ReadToEnd();
						try
						{
							var ravenJObject = RavenJObject.Parse(error);
							log.WarnException("Replication to " + destination + " had failed\r\n" + ravenJObject.Value<string>("Error"), e);
							return false;
						}
						catch (Exception)
						{
						}

						log.WarnException("Replication to " + destination + " had failed\r\n" + error, e);
					}
				}
				else
				{
					log.WarnException("Replication to " + destination + " had failed", e);
				}
				return false;
			}
			catch (Exception e)
			{
				log.WarnException("Replication to " + destination + " had failed", e);
				return false;
			}
		}

		private bool TryReplicationDocuments(ReplicationStrategy destination, RavenJArray jsonDocuments)
		{
			try
			{
				log.Debug("Starting to replicate {0} documents to {1}", jsonDocuments.Length, destination);
				var url = destination.ConnectionStringOptions.Url + "/replication/replicateDocs?from=" + UrlEncodedServerUrl();

				var sp = Stopwatch.StartNew();

				var request = httpRavenRequestFactory.Create(url, "POST", destination.ConnectionStringOptions);
				request.Write(jsonDocuments);
				request.ExecuteRequest();
				log.Info("Replicated {0} documents to {1} in {2:#,#;;0} ms", jsonDocuments.Length, destination, sp.ElapsedMilliseconds);
				return true;
			}
			catch (WebException e)
			{
				var response = e.Response as HttpWebResponse;
				if (response != null)
				{
					Stream responseStream = response.GetResponseStream();
					if (responseStream != null)
					{
						using (var streamReader = new StreamReader(responseStream))
						{
							var error = streamReader.ReadToEnd();
							log.WarnException("Replication to " + destination + " had failed\r\n" + error, e);
						}
					}
					else
					{
						log.WarnException("Replication to " + destination + " had failed", e);
					}
				}
				else
				{
					log.WarnException("Replication to " + destination + " had failed", e);
				}
				return false;
			}
			catch (Exception e)
			{
				log.WarnException("Replication to " + destination + " had failed", e);
				return false;
			}
		}

		private Tuple<RavenJArray, Guid> GetJsonDocuments(SourceReplicationInformation destinationsReplicationInformationForSource, ReplicationStrategy destination)
		{
			RavenJArray jsonDocuments = null;
			Guid lastDocumentEtag = Guid.Empty;
			try
			{
				var destinationId = destinationsReplicationInformationForSource.ServerInstanceId.ToString();

				docDb.TransactionalStorage.Batch(actions =>
				{
					int docsSinceLastReplEtag = 0;
					List<JsonDocument> docsToReplicate;
					List<JsonDocument> filteredDocsToReplicate;
					lastDocumentEtag = destinationsReplicationInformationForSource.LastDocumentEtag;
					while (true)
					{
						docsToReplicate = actions.Documents.GetDocumentsAfter(lastDocumentEtag, 100, 1024 * 1024 * 10)
							.Concat(actions.Lists.Read("Raven/Replication/Docs/Tombstones", lastDocumentEtag, 100)
										.Select(x => new JsonDocument
										{
											Etag = x.Etag,
											Key = x.Key,
											Metadata = x.Data,
											DataAsJson = new RavenJObject()
										}))
							.OrderBy(x => x.Etag)
							.ToList();
						;
						filteredDocsToReplicate = docsToReplicate.Where(document => destination.FilterDocuments(destinationId, document.Key, document.Metadata)).ToList();

						docsSinceLastReplEtag += docsToReplicate.Count;

						if (docsToReplicate.Count == 0 ||
							filteredDocsToReplicate.Count != 0)
						{
							break;
						}

						JsonDocument jsonDocument = docsToReplicate.Last();
						Debug.Assert(jsonDocument.Etag != null);
						Guid documentEtag = jsonDocument.Etag.Value;
						log.Debug("All the docs were filtered, trying another batch from etag [>{0}]", documentEtag);
						lastDocumentEtag = documentEtag;
					}

					log.Debug(() =>
					{
						if (docsSinceLastReplEtag == 0)
							return string.Format("No documents to replicate to {0} - last replicated etag: {1}", destination,
												 destinationsReplicationInformationForSource.LastDocumentEtag);

						if (docsSinceLastReplEtag == filteredDocsToReplicate.Count)
							return string.Format("Replicating {0} docs [>{1}] to {2}.",
											 docsSinceLastReplEtag,
											 destinationsReplicationInformationForSource.LastDocumentEtag,
											 destination);

						var diff = docsToReplicate.Except(filteredDocsToReplicate).Select(x => x.Key);
						return string.Format("Replicating {1} docs (out of {0}) [>{4}] to {2}. [Not replicated: {3}]",
											 docsSinceLastReplEtag,
											 filteredDocsToReplicate.Count,
											 destination,
											 string.Join(", ", diff),
											 destinationsReplicationInformationForSource.LastDocumentEtag);
					});

					jsonDocuments = new RavenJArray(filteredDocsToReplicate
														.Select(x =>
														{
															DocumentRetriever.EnsureIdInMetadata(x);
															return x;
														})
														.Select(x => x.ToJson()));
				});
			}
			catch (Exception e)
			{
				log.WarnException("Could not get documents to replicate after: " + destinationsReplicationInformationForSource.LastDocumentEtag, e);
			}
			return Tuple.Create(jsonDocuments, lastDocumentEtag);
		}


		private Tuple<RavenJArray, Guid> GetAttachments(SourceReplicationInformation destinationsReplicationInformationForSource, ReplicationStrategy destination)
		{
			RavenJArray attachments = null;
			Guid lastAttachmentEtag = Guid.Empty;
			try
			{
				var destinationId = destinationsReplicationInformationForSource.ServerInstanceId.ToString();

				docDb.TransactionalStorage.Batch(actions =>
				{
					int attachmentSinceLastEtag = 0;
					List<AttachmentInformation> attachmentsToReplicate;
					List<AttachmentInformation> filteredAttachmentsToReplicate;
					lastAttachmentEtag = destinationsReplicationInformationForSource.LastAttachmentEtag;
					while (true)
					{
						attachmentsToReplicate = actions.Attachments.GetAttachmentsAfter(lastAttachmentEtag, 100, 1024*1024*10)
							.Concat(actions.Lists.Read(Constants.RavenReplicationAttachmentsTombstones, lastAttachmentEtag, 100)
								        .Select(x => new AttachmentInformation
								        {
									        Key = x.Key,
									        Etag = x.Etag,
									        Metadata = x.Data,
									        Size = 0,
								        }))
							.OrderBy(x => x.Etag)

							.ToList();
						;
						filteredAttachmentsToReplicate = attachmentsToReplicate.Where(attachment => destination.FilterAttachments(attachment, destinationId)).ToList();

						attachmentSinceLastEtag += attachmentsToReplicate.Count;

						if (attachmentsToReplicate.Count == 0 ||
							filteredAttachmentsToReplicate.Count != 0)
						{
							break;
						}

						AttachmentInformation jsonDocument = attachmentsToReplicate.Last();
						Guid attachmentEtag = jsonDocument.Etag;
						log.Debug("All the attachments were filtered, trying another batch from etag [>{0}]", attachmentEtag);
						lastAttachmentEtag = attachmentEtag;
					}

					log.Debug(() =>
					{
						if (attachmentSinceLastEtag == 0)
							return string.Format("No attachments to replicate to {0} - last replicated etag: {1}", destination,
												 destinationsReplicationInformationForSource.LastDocumentEtag);

						if (attachmentSinceLastEtag == filteredAttachmentsToReplicate.Count)
							return string.Format("Replicating {0} attachments [>{1}] to {2}.",
											 attachmentSinceLastEtag,
											 destinationsReplicationInformationForSource.LastDocumentEtag,
											 destination);

						var diff = attachmentsToReplicate.Except(filteredAttachmentsToReplicate).Select(x => x.Key);
						return string.Format("Replicating {1} attachments (out of {0}) [>{4}] to {2}. [Not replicated: {3}]",
											 attachmentSinceLastEtag,
											 filteredAttachmentsToReplicate.Count,
											 destination,
											 string.Join(", ", diff),
											 destinationsReplicationInformationForSource.LastDocumentEtag);
					});

					attachments = new RavenJArray(filteredAttachmentsToReplicate
						                              .Select(x =>
						                              {
							                              var data = new byte[0];
														  if(x.Size> 0)
														  {
															  data = actions.Attachments.GetAttachment(x.Key).Data().ReadData();
														  }
							                              return new RavenJObject
							                                           {
								                                           {"@metadata", x.Metadata},
								                                           {"@id", x.Key},
								                                           {"@etag", x.Etag.ToByteArray()},
								                                           {"data", data}
							                                           };
						                              }));
				});
			}
			catch (Exception e)
			{
				log.WarnException("Could not get attachments to replicate after: " + destinationsReplicationInformationForSource.LastAttachmentEtag, e);
			}
			return Tuple.Create(attachments, lastAttachmentEtag);
		}

		private SourceReplicationInformation GetLastReplicatedEtagFrom(ReplicationStrategy destination)
		{
			try
			{
				var currentEtag = Guid.Empty;
				docDb.TransactionalStorage.Batch(accessor => currentEtag = accessor.Staleness.GetMostRecentDocumentEtag());
				var url = destination.ConnectionStringOptions.Url + "/replication/lastEtag?from=" + UrlEncodedServerUrl() +
						  "&currentEtag=" + currentEtag + "&dbid=" + docDb.TransactionalStorage.Id;
				var request = httpRavenRequestFactory.Create(url, "GET", destination.ConnectionStringOptions);
				return request.ExecuteRequest<SourceReplicationInformation>();
			}
			catch (WebException e)
			{
				var response = e.Response as HttpWebResponse;
				if (response != null && (response.StatusCode == HttpStatusCode.BadRequest || response.StatusCode == HttpStatusCode.NotFound))
					log.WarnException("Replication is not enabled on: " + destination, e);
				else
					log.WarnException("Failed to contact replication destination: " + destination, e);
			}
			catch (Exception e)
			{
				log.WarnException("Failed to contact replication destination: " + destination, e);
			}
			return null;
		}

		private string UrlEncodedServerUrl()
		{
			return Uri.EscapeDataString(docDb.ServerUrl);
		}

		private ReplicationStrategy[] GetReplicationDestinations()
		{
			var document = docDb.Get(Constants.RavenReplicationDestinations, null);
			if (document == null)
			{
				return new ReplicationStrategy[0];
			}
			return document.DataAsJson.JsonDeserialization<ReplicationDocument>()
				.Destinations.Select(GetConnectionOptionsSafe)
				.Where(x => x != null)
				.ToArray();
		}

		private ReplicationStrategy GetConnectionOptionsSafe(ReplicationDestination x)
		{
			try
			{
				return GetConnectionOptions(x);
			}
			catch (Exception e)
			{
				log.ErrorException(
					string.Format("IGNORING BAD REPLICATION CONFIG!{0}Could not figure out connection options for [Url: {1}, ConnectionStringName: {2}]",
					Environment.NewLine, x.Url, x.ConnectionStringName),
					e);

				return null;
			}
		}

		private ReplicationStrategy GetConnectionOptions(ReplicationDestination x)
		{
			var replicationStrategy = new ReplicationStrategy
			{
				ReplicationOptionsBehavior = x.TransitiveReplicationBehavior,
				CurrentDatabaseId = docDb.TransactionalStorage.Id.ToString()
			};
			return string.IsNullOrEmpty(x.ConnectionStringName) ?
				CreateReplicationStrategyFromDocument(x, replicationStrategy) :
				CreateReplicationStrategyFromConnectionString(x, replicationStrategy);
		}

		private static ReplicationStrategy CreateReplicationStrategyFromConnectionString(ReplicationDestination x, ReplicationStrategy replicationStrategy)
		{
			var connectionStringParser = ConnectionStringParser<RavenConnectionStringOptions>.FromConnectionStringName(x.ConnectionStringName);
			connectionStringParser.Parse();
			var options = connectionStringParser.ConnectionStringOptions;
			if (string.IsNullOrEmpty(options.Url))
				throw new InvalidOperationException("Could not figure out what the replication URL is");
			if (string.IsNullOrEmpty(options.DefaultDatabase) == false)
			{
				options.Url += "/databases/" + options.DefaultDatabase;
			}
			replicationStrategy.ConnectionStringOptions = options;
			return replicationStrategy;
		}

		private static ReplicationStrategy CreateReplicationStrategyFromDocument(ReplicationDestination x, ReplicationStrategy replicationStrategy)
		{
			var url = x.Url;
			if (string.IsNullOrEmpty(x.Database) == false)
			{
				url = url + "/databases/" + x.Database;
			}
			replicationStrategy.ConnectionStringOptions = new RavenConnectionStringOptions
			{
				Url = url,
				ApiKey = x.ApiKey,
			};
			if (string.IsNullOrEmpty(x.Username) == false)
			{
				replicationStrategy.ConnectionStringOptions.Credentials = string.IsNullOrEmpty(x.Domain)
					? new NetworkCredential(x.Username, x.Password)
					: new NetworkCredential(x.Username, x.Password, x.Domain);
			}
			return replicationStrategy;
		}
	}

	public class ReplicationStrategy
	{
		public bool FilterDocuments(string destinationId, string key, RavenJObject metadata)
		{
			if (key.StartsWith("Raven/", StringComparison.InvariantCultureIgnoreCase)) // don't replicate system docs
			{
				if (key.StartsWith("Raven/Hilo/", StringComparison.InvariantCultureIgnoreCase) == false) // except for hilo documents
					return false;
			}
			if (metadata.ContainsKey(Constants.NotForReplication) && metadata.Value<bool>(Constants.NotForReplication)) // not explicitly marked to skip
				return false;
			if (metadata[Constants.RavenReplicationConflict] != null) // don't replicate conflicted documents, that just propagate the conflict
				return false;

			if (metadata.Value<string>(Constants.RavenReplicationSource) == destinationId) // prevent replicating back to source
				return false;

			switch (ReplicationOptionsBehavior)
			{
				case TransitiveReplicationOptions.None:
					var value = metadata.Value<string>(Constants.RavenReplicationSource);
					var replicateDoc = value == null || (value == CurrentDatabaseId);
					return replicateDoc;
			}
			return true;

		}

		public bool FilterAttachments(AttachmentInformation attachment, string destinationInstanceId)
		{
			if (attachment.Key.StartsWith("Raven/", StringComparison.InvariantCultureIgnoreCase) || // don't replicate system attachments
				attachment.Key.StartsWith("transactions/recoveryInformation", StringComparison.InvariantCultureIgnoreCase)) // don't replicate transaction recovery information
				return false;

			// explicitly marked to skip
			if (attachment.Metadata.ContainsKey(Constants.NotForReplication) && attachment.Metadata.Value<bool>(Constants.NotForReplication))
				return false;

			if (attachment.Metadata.ContainsKey(Constants.RavenReplicationConflict))// don't replicate conflicted documents, that just propagate the conflict
				return false;

			// we don't replicate stuff that was created there
			if (attachment.Metadata.Value<string>(Constants.RavenReplicationSource) == destinationInstanceId)
				return false;

			switch (ReplicationOptionsBehavior)
			{
				case TransitiveReplicationOptions.None:
					return attachment.Metadata.Value<string>(Constants.RavenReplicationSource) == null ||
						(attachment.Metadata.Value<string>(Constants.RavenReplicationSource) == CurrentDatabaseId);
			}
			return true;

		}

		public string CurrentDatabaseId { get; set; }

		public TransitiveReplicationOptions ReplicationOptionsBehavior { get; set; }
		public RavenConnectionStringOptions ConnectionStringOptions { get; set; }

		public override string ToString()
		{
			return string.Join(" ", new[]
			{
				ConnectionStringOptions.Url,
				ConnectionStringOptions.DefaultDatabase,
				ConnectionStringOptions.ApiKey
			}.Where(x => x != null));
		}
	}
}
