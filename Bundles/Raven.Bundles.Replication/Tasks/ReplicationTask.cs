//-----------------------------------------------------------------------
// <copyright file="ReplicationTask.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Bson;
using NLog;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Replication;
using Raven.Bundles.Replication.Data;
using Raven.Database;
using Raven.Database.Impl;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Bundles.Replication.Tasks
{
	public class ReplicationTask : IStartupTask
	{
		public class IntHolder
		{
			public int Value;
		}

		private DocumentDatabase docDb;
		private readonly Logger log = LogManager.GetCurrentClassLogger();
		private bool firstTimeFoundNoReplicationDocument = true;
		private readonly ConcurrentDictionary<string, IntHolder> activeReplicationTasks = new ConcurrentDictionary<string, IntHolder>();

		private int replicationAttempts;
		private int workCounter;
		private int replicationRequestTimeoutInMs;

		public void Execute(DocumentDatabase database)
		{
			docDb = database;
			replicationRequestTimeoutInMs =
				docDb.Configuration.GetConfigurationValue<int>("Raven/Replication/ReplicationRequestTimeout") ?? 7500;
			
			new Thread(Execute)
			{
				IsBackground = true,
				Name = "Replication Thread"
			}.Start();

		}

		private void Execute()
		{
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
								var holder = activeReplicationTasks.GetOrAdd(destination.Url, new IntHolder());
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

				runningBecauseOfDataModifications = context.WaitForWork(timeToWaitInMinutes, ref workCounter);
				timeToWaitInMinutes = runningBecauseOfDataModifications
				                      	? TimeSpan.FromSeconds(30)
				                      	: TimeSpan.FromMinutes(5);
			}
		}

		private bool IsNotFailing(RavenConnectionStringOptions dest, int currentReplicationAttempts)
		{
			var jsonDocument = docDb.Get(ReplicationConstants.RavenReplicationDestinationsBasePath + EscapeDestinationName(dest), null);
			if (jsonDocument == null)
				return true;
			var failureInformation = jsonDocument.DataAsJson.JsonDeserialization<DestinationFailureInformation>();
			if (failureInformation.FailureCount > 1000)
			{
				var shouldReplicateTo = currentReplicationAttempts%10 == 0;
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

		private static string EscapeDestinationName(RavenConnectionStringOptions dest)
		{
			return Uri.EscapeDataString(dest.Url.Replace("http://", "").Replace("/", "").Replace(":",""));
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

		private bool ReplicateTo(RavenConnectionStringOptions destination)
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
				var holder = activeReplicationTasks.GetOrAdd(destination.Url, new IntHolder());
				Thread.VolatileWrite(ref holder.Value, 0);
			}
		}

		private bool? ReplicateAttachments(RavenConnectionStringOptions destination, SourceReplicationInformation destinationsReplicationInformationForSource)
		{
			var attachments = GetAttachments(destinationsReplicationInformationForSource);

			if (attachments == null || attachments.Length == 0)
				return null;

			if (TryReplicationAttachments(destination, attachments) == false)// failed to replicate, start error handling strategy
			{
				if (IsFirstFailue(destination))
				{
					log.Info(
						"This is the first failure for {0}, assuming transinet failure and trying again",
						destination);
					if (TryReplicationAttachments(destination, attachments))// success on second faile
						return true;
				}
				IncrementFailureCount(destination);
				return false;
			}

			return true;
		}

		private bool? ReplicateDocuments(RavenConnectionStringOptions destination, SourceReplicationInformation destinationsReplicationInformationForSource)
		{
			var jsonDocuments = GetJsonDocuments(destinationsReplicationInformationForSource);
			if (jsonDocuments == null || jsonDocuments.Length == 0)
				return null;
			if (TryReplicationDocuments(destination, jsonDocuments) == false)// failed to replicate, start error handling strategy
			{
				if (IsFirstFailue(destination))
				{
					log.Info(
						"This is the first failure for {0}, assuming transinet failure and trying again",
						destination);
					if (TryReplicationDocuments(destination, jsonDocuments))// success on second faile
						return true;
				}
				IncrementFailureCount(destination);
				return false;
			}
			return true;
		}

		private void IncrementFailureCount(RavenConnectionStringOptions destination)
		{
			var jsonDocument = docDb.Get(ReplicationConstants.RavenReplicationDestinationsBasePath + EscapeDestinationName(destination), null);
			var failureInformation = new DestinationFailureInformation {Destination = destination.Url};
			if (jsonDocument != null)
			{
				failureInformation = jsonDocument.DataAsJson.JsonDeserialization<DestinationFailureInformation>();
			}
			failureInformation.FailureCount += 1;
			docDb.Put(ReplicationConstants.RavenReplicationDestinationsBasePath + EscapeDestinationName(destination), null,
					  RavenJObject.FromObject(failureInformation), new RavenJObject(), null);
		}

		private bool IsFirstFailue(RavenConnectionStringOptions destination)
		{
			var jsonDocument = docDb.Get(ReplicationConstants.RavenReplicationDestinationsBasePath + EscapeDestinationName(destination), null);
			if (jsonDocument == null)
				return true;
			var failureInformation = jsonDocument.DataAsJson.JsonDeserialization<DestinationFailureInformation>();
			return failureInformation.FailureCount == 0;
		}

		private bool TryReplicationAttachments(RavenConnectionStringOptions destination, RavenJArray jsonAttachments)
		{
			try
			{
				var request = (HttpWebRequest)WebRequest.Create(destination.Url + "/replication/replicateAttachments?from=" + UrlEncodedServerUrl());
				request.UseDefaultCredentials = true;
				request.Credentials = destination.Credentials ?? CredentialCache.DefaultNetworkCredentials;
				request.Method = "POST";
				using (var stream = request.GetRequestStream())
				{
					jsonAttachments.WriteTo(new BsonWriter(stream));
					stream.Flush();
				}
				using (request.GetResponse())
				{
					log.Info("Replicated {0} attachments to {1}", jsonAttachments.Length, destination);
				}
				return true;
			}
			catch (WebException e)
			{
				var response = e.Response as HttpWebResponse;
				if (response != null)
				{
					using (var streamReader = new StreamReader(response.GetResponseStream()))
					{
						var error = streamReader.ReadToEnd();
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

		private bool TryReplicationDocuments(RavenConnectionStringOptions destination, RavenJArray jsonDocuments)
		{
			try
			{
				log.Debug("Starting to replicate {0} documents to {1}", jsonDocuments.Length, destination);
				var request = (HttpWebRequest)WebRequest.Create(destination.Url + "/replication/replicateDocs?from=" + UrlEncodedServerUrl());
				request.UseDefaultCredentials = true;
				request.ContentType = "application/json; charset=utf-8";
				request.Credentials = destination.Credentials ?? CredentialCache.DefaultNetworkCredentials;
				request.Method = "POST";
				using (var stream = request.GetRequestStream())
				using (var streamWriter = new StreamWriter(stream, Encoding.UTF8))
				{
					jsonDocuments.WriteTo(new JsonTextWriter(streamWriter));
					streamWriter.Flush();
					stream.Flush();
				}
				using (request.GetResponse())
				{
					log.Info("Replicated {0} documents to {1}", jsonDocuments.Length, destination);
				}
				return true;
			}
			catch (WebException e)
			{
				var response = e.Response as HttpWebResponse;
				if (response != null)
				{
					using (var streamReader = new StreamReader(response.GetResponseStream()))
					{
						var error = streamReader.ReadToEnd();
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

		private RavenJArray GetJsonDocuments(SourceReplicationInformation destinationsReplicationInformationForSource)
		{
			RavenJArray jsonDocuments = null;
			try
			{
				var destinationId = destinationsReplicationInformationForSource.ServerInstanceId.ToString();

				docDb.TransactionalStorage.Batch(actions =>
				{
					jsonDocuments = new RavenJArray(actions.Documents.GetDocumentsAfter(destinationsReplicationInformationForSource.LastDocumentEtag)
						.Where(x => x.Key.StartsWith("Raven/") == false) // don't replicate system docs
						.Where(x => x.Metadata.Value<string>(ReplicationConstants.RavenReplicationSource) != destinationId) // prevent replicating back to source
						.Where(x=> x.Metadata[ReplicationConstants.RavenReplicationConflict] == null) // don't replicate conflicted documents, that just propgate the conflict
						.Select(x=>
						{
							DocumentRetriever.EnsureIdInMetadata(x);
							return x;
						})
						.Take(100)
						.Select(x => x.ToJson()));
				});
			}
			catch (Exception e)
			{
				log.WarnException("Could not get documents to replicate after: " + destinationsReplicationInformationForSource.LastDocumentEtag, e);
			}
			return jsonDocuments;
		}

		private RavenJArray GetAttachments(SourceReplicationInformation destinationsReplicationInformationForSource)
		{
			RavenJArray jsonAttachments = null;
			try
			{
				string destinationInstanceId = destinationsReplicationInformationForSource.ServerInstanceId.ToString();

				docDb.TransactionalStorage.Batch(actions =>
				{
					jsonAttachments = new RavenJArray(actions.Attachments.GetAttachmentsAfter(destinationsReplicationInformationForSource.LastAttachmentEtag)
						.Where(x => x.Key.StartsWith("Raven/") == false) // don't replicate system docs
						.Where(x => x.Metadata.Value<string>(ReplicationConstants.RavenReplicationSource) != destinationInstanceId) // prevent replicating back to source
						.Where(x => x.Metadata[ReplicationConstants.RavenReplicationConflict] == null) // don't replicate conflicted documents, that just propgate the conflict
						.Take(100)
						.Select(x => new RavenJObject
						{
							{"@metadata", x.Metadata},
							{"@id", x.Key},
							{"@etag", x.Etag.ToByteArray()},
							{"data", actions.Attachments.GetAttachment(x.Key).Data}
						}));
				});
			}
			catch (Exception e)
			{
				log.WarnException("Could not get documents to replicate after: " + destinationsReplicationInformationForSource.LastAttachmentEtag, e);
			}
			return jsonAttachments;
		}

		private SourceReplicationInformation GetLastReplicatedEtagFrom(RavenConnectionStringOptions destination)
		{
			try
			{
				var request = (HttpWebRequest)WebRequest.Create(destination.Url + "/replication/lastEtag?from=" + UrlEncodedServerUrl());
				request.Credentials = destination.Credentials ?? CredentialCache.DefaultNetworkCredentials;
				request.UseDefaultCredentials = true;
				request.Timeout = replicationRequestTimeoutInMs;
				using (var response = request.GetResponse())
				using (var stream = response.GetResponseStream())
				{
					var etagFromServer = (SourceReplicationInformation)new JsonSerializer().Deserialize(new StreamReader(stream), typeof(SourceReplicationInformation));
					return etagFromServer;
				}
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
			return Uri.EscapeDataString(docDb.Configuration.ServerUrl);
		}

		private RavenConnectionStringOptions[] GetReplicationDestinations()
		{
			var document = docDb.Get(ReplicationConstants.RavenReplicationDestinations, null);
			if (document == null)
			{
				docDb.Put(ReplicationConstants.RavenReplicationDestinations, null, RavenJObject.FromObject(new ReplicationDocument()), new RavenJObject(), null);
				document = docDb.Get(ReplicationConstants.RavenReplicationDestinations, null);
			}
			return document.DataAsJson.JsonDeserialization<ReplicationDocument>()
				.Destinations.Select(GetConnectionOptionsSafe)
				.Where(x=>x!=null)
				.ToArray();
		}

		private RavenConnectionStringOptions GetConnectionOptionsSafe(ReplicationDestination x)
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

		private static RavenConnectionStringOptions GetConnectionOptions(ReplicationDestination x)
		{
			if (string.IsNullOrEmpty(x.ConnectionStringName))
				return new RavenConnectionStringOptions
				{
					Url = x.Url
				};

			var connectionStringParser = ConnectionStringParser<RavenConnectionStringOptions>.FromConnectionStringName(x.ConnectionStringName);
			connectionStringParser.Parse();
			var options = connectionStringParser.ConnectionStringOptions;
			if (string.IsNullOrEmpty(options.Url))
				throw new InvalidOperationException("Could not figure out what the replication URL is");
			if (string.IsNullOrEmpty(options.DefaultDatabase) == false)
			{
				if (options.Url.EndsWith("/") == false)
					options.Url += "/";
				options.Url += "databases/" + options.DefaultDatabase;
			}
			return options;
		}
	}
}
