using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http;

using Raven.Abstractions;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.Bundles.Replication.Data;
using Raven.Bundles.Replication.Plugins;
using Raven.Bundles.Replication.Responders;
using Raven.Bundles.Replication.Tasks;
using Raven.Client.Connection;
using Raven.Database.Bundles.Replication.Plugins;
using Raven.Database.Bundles.Replication.Utils;
using Raven.Database.Config;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Database.Storage;
using Raven.Json.Linq;

namespace Raven.Database.Bundles.Replication.Controllers
{
	public class ReplicationController : BundlesApiController
	{
		private static readonly ILog log = LogManager.GetCurrentClassLogger();

		public override string BundleName
		{
			get { return "replication"; }
		}

		private ReplicationTask replicationTask;
		public ReplicationTask ReplicationTask
		{
			get { return replicationTask ?? (replicationTask = Database.StartupTasks.OfType<ReplicationTask>().FirstOrDefault()); }
		}

		public IEnumerable<AbstractDocumentReplicationConflictResolver> DocsReplicationConflictResolvers
		{
			get
			{
				var exported = Database.Configuration.Container.GetExportedValues<AbstractDocumentReplicationConflictResolver>();

				var config = GetReplicationConfig();

				if (config == null || config.DocumentConflictResolution == StraightforwardConflictResolution.None)
					return exported;

				var withConfiguredResolvers = exported.ToList();

				switch (config.DocumentConflictResolution)
				{
					case StraightforwardConflictResolution.ResolveToLocal:
						withConfiguredResolvers.Add(LocalDocumentReplicationConflictResolver.Instance);
						break;
					case StraightforwardConflictResolution.ResolveToRemote:
						withConfiguredResolvers.Add(RemoteDocumentReplicationConflictResolver.Instance);
						break;
					case StraightforwardConflictResolution.ResolveToLatest:
						withConfiguredResolvers.Add(LatestDocumentReplicationConflictResolver.Instance);
						break;
					default:
						throw new ArgumentOutOfRangeException("config.DocumentConflictResolution");
				}

				return withConfiguredResolvers;
			}
		}

		[HttpGet]
		[RavenRoute("replication/explain/{*docId}")]
		[RavenRoute("databases/{databaseName}/replication/explain/{*docId}")]
		public HttpResponseMessage ExplainGet(string docId)
		{
			if (string.IsNullOrEmpty(docId)) 
				return GetMessageWithString("Document key is required.", HttpStatusCode.BadRequest);

			var destinationUrl = GetQueryStringValue("destinationUrl");
			if (string.IsNullOrEmpty(destinationUrl))
				return GetMessageWithString("Destination url is required.", HttpStatusCode.BadRequest);

			var databaseName = GetQueryStringValue("databaseName");
			if (string.IsNullOrEmpty(databaseName))
				return GetMessageWithString("Destination database name is required.", HttpStatusCode.BadRequest);

			var result = new ReplicationExplanationForDocument
			{
				Key = docId,
				Destination = new ReplicationExplanationForDocument.DestinationInformation
				{
					Url = destinationUrl,
					DatabaseName = databaseName
				}
			};

			var destinations = ReplicationTask.GetReplicationDestinations(x => string.Equals(x.Url, destinationUrl, StringComparison.OrdinalIgnoreCase) && string.Equals(x.Database, databaseName, StringComparison.OrdinalIgnoreCase));
			if (destinations == null || destinations.Length == 0)
			{
				result.Message = string.Format("Could not find replication destination for a given url ('{0}') and database ('{1}').", destinationUrl, databaseName);
				return GetMessageWithObject(result);
			}

			if (destinations.Length > 1)
			{
				result.Message = string.Format("There is more than one replication destination for a given url ('{0}') and database ('{1}').", destinationUrl, databaseName);
				return GetMessageWithObject(result);
			}

			var destination = destinations[0];
			var destinationsReplicationInformationForSource = ReplicationTask.GetLastReplicatedEtagFrom(destination);
			if (destinationsReplicationInformationForSource == null)
			{
				result.Message = "Could not connect to destination server.";
				return GetMessageWithObject(result);
			}

			var destinationId = destinationsReplicationInformationForSource.ServerInstanceId.ToString();
			result.Destination.ServerInstanceId = destinationId;
			result.Destination.LastDocumentEtag = destinationsReplicationInformationForSource.LastDocumentEtag;

			var document = Database.Documents.Get(docId, null);
			if (document == null)
			{
				result.Message = string.Format("Document with given key ('{0}') does not exist.", docId);
				return GetMessageWithObject(result);
			}

			result.Key = document.Key;
			result.Etag = document.Etag;

			string reason;
			if (destination.FilterDocuments(destinationId, document.Key, document.Metadata, out reason) == false)
			{
				result.Message = reason;
				return GetMessageWithObject(result);
			}

			reason = EtagUtil.IsGreaterThan(document.Etag, destinationsReplicationInformationForSource.LastDocumentEtag) ? "Document will be replicated." : "Document should have been replicated.";
			result.Message = reason;

			return GetMessageWithObject(result);
		}

		[HttpGet]
		[RavenRoute("replication/topology")]
		[RavenRoute("databases/{databaseName}/replication/topology")]
		public HttpResponseMessage TopologyGet()
		{
			var documentsController = new ConfigurationController();
			documentsController.InitializeFrom(this);
			return documentsController.ReplicationConfigurationGet();
		}

		[Obsolete("Use RavenFS instead.")]
		public IEnumerable<AbstractAttachmentReplicationConflictResolver> AttachmentReplicationConflictResolvers
		{
			get
			{
				var exported = Database.Configuration.Container.GetExportedValues<AbstractAttachmentReplicationConflictResolver>();

				var config = GetReplicationConfig();

				if (config == null || config.AttachmentConflictResolution == StraightforwardConflictResolution.None)
					return exported;

				var withConfiguredResolvers = exported.ToList();

				switch (config.AttachmentConflictResolution)
				{
					case StraightforwardConflictResolution.ResolveToLocal:
						withConfiguredResolvers.Add(LocalAttachmentReplicationConflictResolver.Instance);
						break;
					case StraightforwardConflictResolution.ResolveToRemote:
						withConfiguredResolvers.Add(RemoteAttachmentReplicationConflictResolver.Instance);
						break;
					case StraightforwardConflictResolution.ResolveToLatest:
						// ignore this resolver for attachments
						break;
					default:
						throw new ArgumentOutOfRangeException("config.AttachmentConflictResolution");
				}

				return withConfiguredResolvers;
			}
		}

		[HttpPost]
		[RavenRoute("replication/replicateDocs")]
		[RavenRoute("databases/{databaseName}/replication/replicateDocs")]
		public async Task<HttpResponseMessage> DocReplicatePost()
		{
			const int BatchSize = 512;

			var src = GetQueryStringValue("from");
			if (string.IsNullOrEmpty(src))
				return GetEmptyMessage(HttpStatusCode.BadRequest);

			while (src.EndsWith("/"))
				src = src.Substring(0, src.Length - 1);// remove last /, because that has special meaning for Raven

			if (string.IsNullOrEmpty(src))
				return GetEmptyMessage(HttpStatusCode.BadRequest);

			var array = await ReadJsonArrayAsync();
			if (ReplicationTask != null)
				ReplicationTask.HandleHeartbeat(src);

			using (Database.DisableAllTriggersForCurrentThread())
			{
				var conflictResolvers = DocsReplicationConflictResolvers; 

				string lastEtag = Etag.Empty.ToString();

				var docIndex = 0;

				while (docIndex < array.Length)
				{
					using (Database.DocumentLock.Lock())
					{
						Database.TransactionalStorage.Batch(actions =>
						{
							for (var j = 0; j < BatchSize && docIndex < array.Length; j++, docIndex++)
							{
								var document = (RavenJObject) array[docIndex];
								var metadata = document.Value<RavenJObject>("@metadata");
								if (metadata[Constants.RavenReplicationSource] == null)
								{
									// not sure why, old document from when the user didn't have replication
									// that we suddenly decided to replicate, choose the source for that
									metadata[Constants.RavenReplicationSource] = RavenJToken.FromObject(src);
								}

								lastEtag = metadata.Value<string>("@etag");
								var id = metadata.Value<string>("@id");
								document.Remove("@metadata");

								ReplicateDocument(actions, id, metadata, document, src, conflictResolvers);
							}

							SaveReplicationSource(src, lastEtag, array.Length);
						});
					}
				}
			}

			return GetEmptyMessage();
		}

		private void SaveReplicationSource(string src, string lastEtag, int batchSize)
		{
			Guid remoteServerInstanceId = Guid.Parse(GetQueryStringValue("dbid"));

			var replicationDocKey = Constants.RavenReplicationSourcesBasePath + "/" + remoteServerInstanceId;
			var replicationDocument = Database.Documents.Get(replicationDocKey, null);
			var lastAttachmentId = Etag.Empty;
			if (replicationDocument != null)
			{
				lastAttachmentId = replicationDocument.DataAsJson.JsonDeserialization<SourceReplicationInformation>().LastAttachmentEtag;
			}

			Database
				.Documents
				.Put(
					replicationDocKey,
					null,
					RavenJObject.FromObject(
						new SourceReplicationInformation
						{
							Source = src,
							LastDocumentEtag = Etag.Parse(lastEtag),
							LastAttachmentEtag = lastAttachmentId,
							ServerInstanceId = remoteServerInstanceId,
							LastModified = SystemTime.UtcNow,
							LastBatchSize = batchSize
						}),
					new RavenJObject(),
					null);
		}

		[HttpPost]
		[RavenRoute("replication/replicateAttachments")]
		[RavenRoute("databases/{databaseName}/replication/replicateAttachments")]
		[Obsolete("Use RavenFS instead.")]
		public async Task<HttpResponseMessage> AttachmentReplicatePost()
		{
			var src = GetQueryStringValue("from");
			if (string.IsNullOrEmpty(src))
				return GetEmptyMessage(HttpStatusCode.BadRequest);

			while (src.EndsWith("/"))
				src = src.Substring(0, src.Length - 1);// remove last /, because that has special meaning for Raven
			if (string.IsNullOrEmpty(src))
				return GetEmptyMessage(HttpStatusCode.BadRequest);

			var array = await ReadBsonArrayAsync();
			using (Database.DisableAllTriggersForCurrentThread())
			{
				var conflictResolvers = AttachmentReplicationConflictResolvers; 

				Database.TransactionalStorage.Batch(actions =>
				{
					Etag lastEtag = Etag.Empty;
					foreach (RavenJObject attachment in array)
					{
						var metadata = attachment.Value<RavenJObject>("@metadata");
						if (metadata[Constants.RavenReplicationSource] == null)
						{
							// not sure why, old attachment from when the user didn't have replication
							// that we suddenly decided to replicate, choose the source for that
							metadata[Constants.RavenReplicationSource] = RavenJToken.FromObject(src);
						}

						lastEtag = Etag.Parse(attachment.Value<byte[]>("@etag"));
						var id = attachment.Value<string>("@id");

						ReplicateAttachment(actions, id, metadata, attachment.Value<byte[]>("data"), src, conflictResolvers);
					}

					Guid remoteServerInstanceId = Guid.Parse(GetQueryStringValue("dbid"));

					var replicationDocKey = Constants.RavenReplicationSourcesBasePath + "/" + remoteServerInstanceId;
					var replicationDocument = Database.Documents.Get(replicationDocKey, null);
					Etag lastDocId = null;
					if (replicationDocument != null)
					{
						lastDocId =
							replicationDocument.DataAsJson.JsonDeserialization<SourceReplicationInformation>().
								LastDocumentEtag;
					}

					Database.Documents.Put(replicationDocKey, null,
								 RavenJObject.FromObject(new SourceReplicationInformation
								 {
									 Source = src,
									 LastDocumentEtag = lastDocId,
									 LastAttachmentEtag = lastEtag,
									 ServerInstanceId = remoteServerInstanceId,
									 LastModified = SystemTime.UtcNow
								 }),
								 new RavenJObject(), null);
				});
			}

			return GetEmptyMessage();
		}

		[HttpGet]
		[HttpPost]
		[RavenRoute("replication/info")]
		[RavenRoute("databases/{databaseName}/replication/info")]
		public HttpResponseMessage ReplicationInfoGet()
		{
			var replicationStatistics = ReplicationUtils.GetReplicationInformation(Database);
			return GetMessageWithObject(replicationStatistics);
		}

		[HttpGet]
		[RavenRoute("replication/lastEtag")]
		[RavenRoute("databases/{databaseName}/replication/lastEtag")]
		public HttpResponseMessage ReplicationLastEtagGet()
		{
			string src;
			string dbid;
			var result = GetValuesForLastEtag(out src, out dbid);
			if (result != null)
				return result;

			using (Database.DisableAllTriggersForCurrentThread())
			{
				JsonDocument document = null;
				SourceReplicationInformationWithBatchInformation sourceReplicationInformation = null;

				var localServerInstanceId = Database.TransactionalStorage.Id; // this is my id, sent to the remote server

				if (string.IsNullOrEmpty(dbid))
				{
					// backward compatibility for replication behavior
					int nextStart = 0;
					var replicationSources = Database.Documents.GetDocumentsWithIdStartingWith(Constants.RavenReplicationSourcesBasePath, null, null, 0, int.MaxValue, CancellationToken.None, ref nextStart);
					foreach (RavenJObject replicationSource in replicationSources)
					{
						sourceReplicationInformation = replicationSource.JsonDeserialization<SourceReplicationInformationWithBatchInformation>();
						if (string.Equals(sourceReplicationInformation.Source, src, StringComparison.OrdinalIgnoreCase) == false)
							continue;

						document = replicationSource.ToJsonDocument();
						break;
					}
				}
				else
				{
					var remoteServerInstanceId = Guid.Parse(dbid);

					document = Database.Documents.Get(Constants.RavenReplicationSourcesBasePath + "/" + remoteServerInstanceId, null);
					if (document == null)
					{
						// migrate
						document = Database.Documents.Get(Constants.RavenReplicationSourcesBasePath + "/" + src, null);
						if (document != null)
						{
							sourceReplicationInformation = document.DataAsJson.JsonDeserialization<SourceReplicationInformationWithBatchInformation>();
							Database.Documents.Put(Constants.RavenReplicationSourcesBasePath + "/" + sourceReplicationInformation.ServerInstanceId, Etag.Empty, document.DataAsJson, document.Metadata, null);
							Database.Documents.Delete(Constants.RavenReplicationSourcesBasePath + "/" + src, document.Etag, null);

							if (remoteServerInstanceId != sourceReplicationInformation.ServerInstanceId) 
								document = null;
						}
					}
				}

				if (document == null)
				{
					sourceReplicationInformation = new SourceReplicationInformationWithBatchInformation
					{
						Source = src,
						ServerInstanceId = localServerInstanceId,
						LastModified = SystemTime.UtcNow
					};
				}
				else
				{
					if (sourceReplicationInformation == null)
						sourceReplicationInformation = document.DataAsJson.JsonDeserialization<SourceReplicationInformationWithBatchInformation>();

					if (string.Equals(sourceReplicationInformation.Source, src, StringComparison.OrdinalIgnoreCase) == false 
						&& sourceReplicationInformation.LastModified.HasValue 
						&& (SystemTime.UtcNow - sourceReplicationInformation.LastModified.Value).TotalMinutes < 10)
					{
						log.Info(string.Format("Replication source mismatch. Stored: {0}. Remote: {1}.", sourceReplicationInformation.Source, src));

						sourceReplicationInformation.LastAttachmentEtag = Etag.InvalidEtag;
						sourceReplicationInformation.LastDocumentEtag = Etag.InvalidEtag;
					}

					sourceReplicationInformation.ServerInstanceId = localServerInstanceId;
				}

				var maxNumberOfItemsToReceiveInSingleBatch = Database.Configuration.Replication.MaxNumberOfItemsToReceiveInSingleBatch;
				var availableMemory = MemoryStatistics.AvailableMemory;
				var lowMemory = availableMemory < 0.2 * MemoryStatistics.TotalPhysicalMemory && availableMemory < Database.Configuration.AvailableMemoryForRaisingBatchSizeLimit * 2;
				if (lowMemory)
				{
				    int size;
					var lastBatchSize = sourceReplicationInformation.LastBatchSize;
					if (lastBatchSize.HasValue && maxNumberOfItemsToReceiveInSingleBatch.HasValue)
                        size = Math.Min(lastBatchSize.Value, maxNumberOfItemsToReceiveInSingleBatch.Value);
					else if (lastBatchSize.HasValue)
                        size = lastBatchSize.Value;
					else if (maxNumberOfItemsToReceiveInSingleBatch.HasValue)
					    size = maxNumberOfItemsToReceiveInSingleBatch.Value;
					else
					    size = 128;

				    sourceReplicationInformation.MaxNumberOfItemsToReceiveInSingleBatch =
                        Math.Max(size / 2, 64);
				}
				else
				{
					sourceReplicationInformation.MaxNumberOfItemsToReceiveInSingleBatch = Database.Configuration.Replication.MaxNumberOfItemsToReceiveInSingleBatch;
				}

				var currentEtag = GetQueryStringValue("currentEtag");
				Log.Debug(() => string.Format("Got replication last etag request from {0}: [Local: {1} Remote: {2}]. LowMemory: {3}. MaxNumberOfItemsToReceiveInSingleBatch: {4}.", src, sourceReplicationInformation.LastDocumentEtag, currentEtag, lowMemory, sourceReplicationInformation.MaxNumberOfItemsToReceiveInSingleBatch));
				return GetMessageWithObject(sourceReplicationInformation);
			}
		}

		[HttpPut]
		[RavenRoute("replication/lastEtag")]
		[RavenRoute("databases/{databaseName}/replication/lastEtag")]
		public HttpResponseMessage ReplicationLastEtagPut()
		{
			string src;
			string dbid;
			var result = GetValuesForLastEtag(out src, out dbid);
			if (result != null)
				return result;

			using (Database.DisableAllTriggersForCurrentThread())
			{
				var document = Database.Documents.Get(Constants.RavenReplicationSourcesBasePath + "/" + dbid, null);

				SourceReplicationInformation sourceReplicationInformation;

				Etag docEtag = null, attachmentEtag = null;
				try
				{
					docEtag = Etag.Parse(GetQueryStringValue("docEtag"));
				}
				catch
				{
				}

				try
				{
					attachmentEtag = Etag.Parse(GetQueryStringValue("attachmentEtag"));
				}
				catch
				{
				}

				Guid serverInstanceId = Guid.Parse(dbid);

				if (document == null)
				{
					sourceReplicationInformation = new SourceReplicationInformation()
					{
						ServerInstanceId = serverInstanceId,
						LastAttachmentEtag = attachmentEtag ?? Etag.Empty,
						LastDocumentEtag = docEtag ?? Etag.Empty,
						Source = src,
						LastModified = SystemTime.UtcNow
					};
				}
				else
				{
					sourceReplicationInformation = document.DataAsJson.JsonDeserialization<SourceReplicationInformation>();
					sourceReplicationInformation.ServerInstanceId = serverInstanceId;
					sourceReplicationInformation.LastDocumentEtag = docEtag ?? sourceReplicationInformation.LastDocumentEtag;
					sourceReplicationInformation.LastAttachmentEtag = attachmentEtag ?? sourceReplicationInformation.LastAttachmentEtag;
					sourceReplicationInformation.LastModified = SystemTime.UtcNow;
				}

				var etag = document == null ? Etag.Empty : document.Etag;
				var metadata = document == null ? new RavenJObject() : document.Metadata;

				var newDoc = RavenJObject.FromObject(sourceReplicationInformation);
				log.Debug("Updating replication last etags from {0}: [doc: {1} attachment: {2}]", src,
								  sourceReplicationInformation.LastDocumentEtag,
								  sourceReplicationInformation.LastAttachmentEtag);

				Database.Documents.Put(Constants.RavenReplicationSourcesBasePath + "/" + dbid, etag, newDoc, metadata, null);
			}

			return GetEmptyMessage();
		}

		[HttpPost]
		[RavenRoute("replication/heartbeat")]
		[RavenRoute("databases/{databaseName}/replication/heartbeat")]
		public HttpResponseMessage HeartbeatPost()
		{
			var src = GetQueryStringValue("from");

			var replicationTask = Database.StartupTasks.OfType<ReplicationTask>().FirstOrDefault();
			if (replicationTask == null)
			{
				return GetMessageWithObject(new
				{
					Error = "Cannot find replication task setup in the database"
				}, HttpStatusCode.NotFound);

			}

			replicationTask.HandleHeartbeat(src);

			return GetEmptyMessage();
		}

		
		[HttpPost]
		[RavenRoute("replication/replicate-indexes")]
		[RavenRoute("databases/{databaseName}/replication/replicate-indexes")]
		public HttpResponseMessage IndexReplicate([FromBody] ReplicationDestination replicationDestination)
		{
			var op = GetQueryStringValue("op");

			if (string.Equals(op, "replicate-all", StringComparison.InvariantCultureIgnoreCase))
				return ReplicateAllIndexes();

			if (string.Equals(op, "replicate-all-to-destination", StringComparison.InvariantCultureIgnoreCase))
				return ReplicateAllIndexes(dest => dest.IsEqualTo(replicationDestination));

			var indexName = GetQueryStringValue("indexName");
			if(indexName == null)
				throw new InvalidOperationException("indexName query string must be specified if op=replicate-all or op=replicate-all-to-destination isn't specified");

			//check for replication document before doing work on getting index definitions.
			//if there is no replication set up --> no point in doing any other work
			HttpResponseMessage erroResponseMessage;
			var replicationDocument = GetReplicationDocument(out erroResponseMessage);
			if (replicationDocument == null)
				return erroResponseMessage;

			if (indexName.EndsWith("/")) //since id is part of the url, perhaps a trailing forward slash appears there
				indexName = indexName.Substring(0, indexName.Length - 1);
			indexName = HttpUtility.UrlDecode(indexName);

			var indexDefinition = Database.IndexDefinitionStorage.GetIndexDefinition(indexName);
			if (indexDefinition == null)
			{
				return GetMessageWithObject(new
				{
					Message = string.Format("Index with name: {0} not found. Cannot proceed with replication...", indexName)
				}, HttpStatusCode.NotFound);
			}

			var serializedIndexDefinition = RavenJObject.FromObject(indexDefinition);

			var httpRavenRequestFactory = new HttpRavenRequestFactory { RequestTimeoutInMs = Database.Configuration.Replication.ReplicationRequestTimeoutInMilliseconds };

			var failedDestinations = new ConcurrentDictionary<string, Exception>();
			Parallel.ForEach(replicationDocument.Destinations.Where(dest => dest.Disabled == false && dest.SkipIndexReplication == false),
				destination =>
				{
					try
					{
						ReplicateIndex(indexName, destination, serializedIndexDefinition, httpRavenRequestFactory);
					}
					catch (Exception e)
					{
						failedDestinations.TryAdd(destination.Humane ?? "<null?>", e);
						log.WarnException("Could not replicate index " + indexName + " to " + destination.Humane, e);
					}
				});

			return GetMessageWithObject(new
			{
				SuccessfulReplicationCount = (replicationDocument.Destinations.Count - failedDestinations.Count),
				FailedDestinationUrls = failedDestinations.Select(x => new { Server = x.Key, Error = x.Value.ToString() }).ToArray()
			});
		}

		[HttpPost]
		[RavenRoute("replication/replicate-transformers")]
		[RavenRoute("databases/{databaseName}/replication/replicate-transformers")]
		public HttpResponseMessage TransformersReplicate([FromBody] ReplicationDestination replicationDestination)
		{
			var op = GetQueryStringValue("op");

			if (string.Equals(op, "replicate-all", StringComparison.InvariantCultureIgnoreCase))
				return ReplicateAllTransformers();

			if (string.Equals(op, "replicate-all-to-destination", StringComparison.InvariantCultureIgnoreCase))
				return ReplicateAllTransformers(dest => dest.IsEqualTo(replicationDestination));

			var transformerName = GetQueryStringValue("transformerName");
			if (transformerName == null)
				throw new InvalidOperationException("transformerName query string must be specified if op=replicate-all or op=replicate-all-to-destination isn't specified");

			HttpResponseMessage erroResponseMessage;
			var replicationDocument = GetReplicationDocument(out erroResponseMessage);
			if (replicationDocument == null)
				return erroResponseMessage;

			if (string.IsNullOrWhiteSpace(transformerName) || transformerName.StartsWith("/"))
				return GetMessageWithString(String.Format("Invalid transformer name! Received : '{0}'", transformerName), HttpStatusCode.NotFound);

			var transformerDefinition = Database.Transformers.GetTransformerDefinition(transformerName);
			if (transformerDefinition == null)
				return GetEmptyMessage(HttpStatusCode.NotFound);

			var clonedTransformerDefinition = transformerDefinition.Clone();
			clonedTransformerDefinition.TransfomerId = 0;

			var serializedTransformerDefinition = RavenJObject.FromObject(clonedTransformerDefinition);
			var httpRavenRequestFactory = new HttpRavenRequestFactory { RequestTimeoutInMs = Database.Configuration.Replication.ReplicationRequestTimeoutInMilliseconds };

			var failedDestinations = new ConcurrentBag<string>();
			Parallel.ForEach(replicationDocument.Destinations.Where(x => x.Disabled == false && x.SkipIndexReplication == false),
				destination => ReplicateTransformer(transformerName, destination, serializedTransformerDefinition, failedDestinations, httpRavenRequestFactory));

			return GetMessageWithObject(new
			{
				SuccessfulReplicationCount = (replicationDocument.Destinations.Count - failedDestinations.Count),
				FailedDestinationUrls = failedDestinations
			});
		}

		private HttpResponseMessage ReplicateAllTransformers(Func<ReplicationDestination, bool> destinationPredicate = null)
		{
			HttpResponseMessage erroResponseMessage;
			var replicationDocument = GetReplicationDocument(out erroResponseMessage);
			if (replicationDocument == null)
				return erroResponseMessage;

			var httpRavenRequestFactory = new HttpRavenRequestFactory { RequestTimeoutInMs = Database.Configuration.Replication.ReplicationRequestTimeoutInMilliseconds };

			var enabledReplicationDestinations = replicationDocument.Destinations
				.Where(dest => dest.Disabled == false && dest.SkipIndexReplication == false)
				.Select(x => (ReplicationDestination)x)
				.ToList();

			if (destinationPredicate != null)
				enabledReplicationDestinations = enabledReplicationDestinations.Where(destinationPredicate).ToList();

			if (enabledReplicationDestinations.Count == 0)
				return GetMessageWithObject(new { Message = "Replication is configured, but no enabled destinations found." }, HttpStatusCode.NotFound);

			var allTransformerDefinitions = Database.Transformers.Definitions;
			if (allTransformerDefinitions.Length == 0)
				return GetMessageWithObject(new { Message = "No transformers to replicate. Nothing to do.. " });

			var replicationRequestTasks = new List<Task>(enabledReplicationDestinations.Count * allTransformerDefinitions.Length);

			var failedDestinations = new ConcurrentBag<string>();
			foreach (var definition in allTransformerDefinitions)
			{
				var clonedDefinition = definition.Clone();
				clonedDefinition.TransfomerId = 0;
				replicationRequestTasks.AddRange(
					enabledReplicationDestinations
						.Select(destination =>
							Task.Run(() =>
								ReplicateTransformer(definition.Name, destination,
									RavenJObject.FromObject(clonedDefinition),
									failedDestinations,
									httpRavenRequestFactory))).ToList());
			}

			Task.WaitAll(replicationRequestTasks.ToArray());

			return GetMessageWithObject(new
			{
				TransformerCount = allTransformerDefinitions.Length,
				EnabledDestinationsCount = enabledReplicationDestinations.Count,
				SuccessfulReplicationCount = ((enabledReplicationDestinations.Count * allTransformerDefinitions.Length) - failedDestinations.Count),
				FailedDestinationUrls = failedDestinations
			});
		}

		private HttpResponseMessage ReplicateAllIndexes(Func<ReplicationDestination, bool> additionalDestinationPredicate = null)
		{
			//check for replication document before doing work on getting index definitions.
			//if there is no replication set up --> no point in doing any other work
			HttpResponseMessage erroResponseMessage;
			var replicationDocument = GetReplicationDocument(out erroResponseMessage);
			if (replicationDocument == null)
				return erroResponseMessage;

			var indexDefinitions = Database.IndexDefinitionStorage
				.IndexDefinitions
				.Select(x => x.Value)
				.ToList();

			var httpRavenRequestFactory = new HttpRavenRequestFactory { RequestTimeoutInMs = Database.Configuration.Replication.ReplicationRequestTimeoutInMilliseconds };
			var enabledReplicationDestinations = replicationDocument.Destinations
				.Where(dest => dest.Disabled == false && dest.SkipIndexReplication == false)
				.Select(x => (ReplicationDestination)x)
				.ToList();

			if (additionalDestinationPredicate != null)
				enabledReplicationDestinations = enabledReplicationDestinations.Where(additionalDestinationPredicate).ToList();

			if (enabledReplicationDestinations.Count == 0)
				return GetMessageWithObject(new { Message = "Replication is configured, but no enabled destinations found." }, HttpStatusCode.NotFound);

			var replicationRequestTasks = new List<Task>(enabledReplicationDestinations.Count * indexDefinitions.Count);

			var failedDestinations = new ConcurrentDictionary<string, Exception>();
			foreach (var definition in indexDefinitions)
			{
				replicationRequestTasks.AddRange(
					enabledReplicationDestinations
						.Select(destination =>
							Task.Run(() =>
							{
								try
								{
									ReplicateIndex(definition.Name, destination,
										RavenJObject.FromObject(definition),
										httpRavenRequestFactory);
								}
								catch (Exception e)
								{
									failedDestinations.TryAdd(destination.Humane ?? "<null?>", e);
									log.WarnException("Could not replicate " + definition.Name + " to " + destination.Humane, e);
								}
							})));
			}

			Task.WaitAll(replicationRequestTasks.ToArray());

			return GetMessageWithObject(new
			{
				IndexesCount = indexDefinitions.Count,
				EnabledDestinationsCount = enabledReplicationDestinations.Count,
				SuccessfulReplicationCount = ((enabledReplicationDestinations.Count * indexDefinitions.Count) - failedDestinations.Count),
				FailedDestinationUrls = failedDestinations.Select(x=>new{ Server = x.Key, Error = x.Value.ToString()}).ToArray()
			});
		}

		private void ReplicateTransformer(string transformerName, ReplicationDestination destination, RavenJObject transformerDefinition, ConcurrentBag<string> failedDestinations, HttpRavenRequestFactory httpRavenRequestFactory)
		{
			var connectionOptions = new RavenConnectionStringOptions
			{
				ApiKey = destination.ApiKey,
				Url = destination.Url,
				DefaultDatabase = destination.Database
			};

			if (!String.IsNullOrWhiteSpace(destination.Username) &&
				!String.IsNullOrWhiteSpace(destination.Password))
			{
				connectionOptions.Credentials = new NetworkCredential(destination.Username, destination.Password, destination.Domain ?? string.Empty);
			}

			//databases/{databaseName}/transformers/{*id}
			const string urlTemplate = "{0}/databases/{1}/transformers/{2}";
			if (Uri.IsWellFormedUriString(destination.Url, UriKind.RelativeOrAbsolute) == false)
			{
				const string error = "Invalid destination URL";
				failedDestinations.Add(destination.Url);
				Log.Error(error);
				return;
			}

			var operationUrl = string.Format(urlTemplate, destination.Url, destination.Database, Uri.EscapeUriString(transformerName));
			var replicationRequest = httpRavenRequestFactory.Create(operationUrl, "PUT", connectionOptions);
			replicationRequest.Write(transformerDefinition);

			try
			{
				replicationRequest.ExecuteRequest();
			}
			catch (Exception e)
			{
				Log.ErrorException("failed to replicate index to: " + destination.Url, e);
				failedDestinations.Add(destination.Url);
			}
		}

		private void ReplicateIndex(string indexName, ReplicationDestination destination, RavenJObject indexDefinition, HttpRavenRequestFactory httpRavenRequestFactory)
		{
			var connectionOptions = new RavenConnectionStringOptions
			{
				ApiKey = destination.ApiKey,
				Url = destination.Url,
				DefaultDatabase = destination.Database
			};

			if (!String.IsNullOrWhiteSpace(destination.Username) &&
				!String.IsNullOrWhiteSpace(destination.Password))
			{
				connectionOptions.Credentials = new NetworkCredential(destination.Username, destination.Password, destination.Domain ?? string.Empty);
			}

			const string urlTemplate = "{0}/databases/{1}/indexes/{2}";

			var operationUrl = string.Format(urlTemplate, destination.Url, destination.Database, Uri.EscapeUriString(indexName));
			var replicationRequest = httpRavenRequestFactory.Create(operationUrl, "PUT", connectionOptions);
			replicationRequest.Write(indexDefinition);

			replicationRequest.ExecuteRequest();
		}


		private HttpResponseMessage GetValuesForLastEtag(out string src, out string dbid)
		{
			src = GetQueryStringValue("from");
			dbid = GetQueryStringValue("dbid");
			if (dbid == Database.TransactionalStorage.Id.ToString())
				throw new InvalidOperationException("Both source and target databases have database id = " + dbid +
													"\r\nDatabase cannot replicate to itself.");

			if (string.IsNullOrEmpty(src))
				return GetEmptyMessage(HttpStatusCode.BadRequest);

			while (src.EndsWith("/"))
				src = src.Substring(0, src.Length - 1); // remove last /, because that has special meaning for Raven
			if (string.IsNullOrEmpty(src))
				return GetEmptyMessage(HttpStatusCode.BadRequest);
			return null;
		}

		private void ReplicateDocument(IStorageActionsAccessor actions, string id, RavenJObject metadata, RavenJObject document, string src, IEnumerable<AbstractDocumentReplicationConflictResolver> conflictResolvers)
		{
			new DocumentReplicationBehavior
			{
				Actions = actions,
				Database = Database,
				ReplicationConflictResolvers = conflictResolvers,
				Src = src
			}.Replicate(id, metadata, document);
		}

		[Obsolete("Use RavenFS instead.")]
		private void ReplicateAttachment(IStorageActionsAccessor actions, string id, RavenJObject metadata, byte[] data, string src, IEnumerable<AbstractAttachmentReplicationConflictResolver> conflictResolvers)
		{
			new AttachmentReplicationBehavior
			{
				Actions = actions,
				Database = Database,
				ReplicationConflictResolvers = conflictResolvers,
				Src = src
			}.Replicate(id, metadata, data);
		}

		private ReplicationConfig GetReplicationConfig()
		{
			var configDoc = Database.Documents.Get(Constants.RavenReplicationConfig, null);

			if (configDoc == null)
				return null;

			ReplicationConfig config;
			try
			{
				config = configDoc.DataAsJson.JsonDeserialization<ReplicationConfig>();
				return config;
			}
			catch (Exception e)
			{
				Log.Warn("Could not deserialize a replication config", e);
				return null;
			}
		}

		private class ReplicationExplanationForDocument
		{
			public string Key { get; set; }

			public Etag Etag { get; set; }

			public DestinationInformation Destination { get; set; }

			public string Message { get; set; }

			public class DestinationInformation
			{
				public string Url { get; set; }

				public string DatabaseName { get; set; }

				public string ServerInstanceId { get; set; }

				public Etag LastDocumentEtag { get; set; }
			}
		}
	}
}