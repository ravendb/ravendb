using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Replication;
using Raven.Bundles.Replication.Data;
using Raven.Bundles.Replication.Plugins;
using Raven.Bundles.Replication.Responders;
using Raven.Bundles.Replication.Tasks;
using Raven.Database.Server.Controllers;
using Raven.Database.Storage;
using Raven.Json.Linq;

namespace Raven.Database.Bundles.Replication.Controllers
{
	[ExportMetadata("Bundle", "Replication")]
	[InheritedExport(typeof(RavenApiController))]
	public class ReplicationController : RavenApiController
	{
		private ReplicationTask replicationTask;
		public ReplicationTask ReplicationTask
		{
			get { return replicationTask ?? (replicationTask = Database.StartupTasks.OfType<ReplicationTask>().FirstOrDefault()); }
		}

		[ImportMany]
		public IEnumerable<AbstractDocumentReplicationConflictResolver> DocsReplicationConflictResolvers { get; set; }

		[ImportMany]
		public IEnumerable<AbstractAttachmentReplicationConflictResolver> AttachmentReplicationConflictResolvers { get; set; }

		[HttpPost("replication/replicateDoc")]
		public async Task<HttpResponseMessage> DocReplicatePost()
		{
			var src = GetQueryStringValue("from");
			if (string.IsNullOrEmpty(src))
				return new HttpResponseMessage(HttpStatusCode.BadRequest);
		
			while (src.EndsWith("/"))
				src = src.Substring(0, src.Length - 1);// remove last /, because that has special meaning for Raven

			if (string.IsNullOrEmpty(src))
				return new HttpResponseMessage(HttpStatusCode.BadRequest);
			
			var array = await ReadJsonArrayAsync();
			if (ReplicationTask != null)
				ReplicationTask.HandleHeartbeat(src);
		
			using (Database.DisableAllTriggersForCurrentThread())
			{
				Database.TransactionalStorage.Batch(actions =>
				{
					string lastEtag = Etag.Empty.ToString();
					foreach (RavenJObject document in array)
					{
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
						ReplicateDocument(actions, id, metadata, document, src);
					}

					var replicationDocKey = Constants.RavenReplicationSourcesBasePath + "/" + src;
					var replicationDocument = Database.Get(replicationDocKey, null);
					var lastAttachmentId = Etag.Empty;
					if (replicationDocument != null)
					{
						lastAttachmentId =
							replicationDocument.DataAsJson.JsonDeserialization<SourceReplicationInformation>().
								LastAttachmentEtag;
					}

					Guid serverInstanceId;
					if (Guid.TryParse(GetQueryStringValue("dbid"), out serverInstanceId) == false)
						serverInstanceId = Database.TransactionalStorage.Id;
					Database.Put(replicationDocKey, null,
								 RavenJObject.FromObject(new SourceReplicationInformation
								 {
									 Source = src,
									 LastDocumentEtag = Etag.Parse(lastEtag),
									 LastAttachmentEtag = lastAttachmentId,
									 ServerInstanceId = serverInstanceId
								 }),
								 new RavenJObject(), null);
				});
			}

			return new HttpResponseMessage(HttpStatusCode.OK);
		}

		[HttpPost("replication/replicateAttachments")]
		public async Task<HttpResponseMessage> AttachmentReplicatePost()
		{
			var src = GetQueryStringValue("from");
			if (string.IsNullOrEmpty(src))
				return new HttpResponseMessage(HttpStatusCode.BadRequest);
			
			while (src.EndsWith("/"))
				src = src.Substring(0, src.Length - 1);// remove last /, because that has special meaning for Raven
			if (string.IsNullOrEmpty(src))
				return new HttpResponseMessage(HttpStatusCode.BadRequest);
			
			var array = await ReadBsonArrayAsync();
			using (Database.DisableAllTriggersForCurrentThread())
			{
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

						ReplicateAttachment(actions, id, metadata, attachment.Value<byte[]>("data"), src);
					}

					var replicationDocKey = Constants.RavenReplicationSourcesBasePath + "/" + src;
					var replicationDocument = Database.Get(replicationDocKey, null);
					Etag lastDocId = null;
					if (replicationDocument != null)
					{
						lastDocId =
							replicationDocument.DataAsJson.JsonDeserialization<SourceReplicationInformation>().
								LastDocumentEtag;
					}
					
					Guid serverInstanceId;
					if (Guid.TryParse(GetQueryStringValue("dbid"), out serverInstanceId) == false)
						serverInstanceId = Database.TransactionalStorage.Id;
					Database.Put(replicationDocKey, null,
								 RavenJObject.FromObject(new SourceReplicationInformation
								 {
									 Source = src,
									 LastDocumentEtag = lastDocId,
									 LastAttachmentEtag = lastEtag,
									 ServerInstanceId = serverInstanceId
								 }),
								 new RavenJObject(), null);
				});
			}

			return new HttpResponseMessage(HttpStatusCode.OK);
		}

		[HttpGet("replication/info")]
		[HttpPost("replication/info")]
		public HttpResponseMessage ReplicationInfoGet()
		{
			var mostRecentDocumentEtag = Etag.Empty;
			var mostRecentAttachmentEtag = Etag.Empty;
			Database.TransactionalStorage.Batch(accessor =>
			{
				mostRecentDocumentEtag = accessor.Staleness.GetMostRecentDocumentEtag();
				mostRecentAttachmentEtag = accessor.Staleness.GetMostRecentAttachmentEtag();
			});

			var replicationTask = Database.StartupTasks.OfType<ReplicationTask>().FirstOrDefault();
			var replicationStatistics = new ReplicationStatistics
			{
				Self = Database.ServerUrl,
				MostRecentDocumentEtag = mostRecentDocumentEtag,
				MostRecentAttachmentEtag = mostRecentAttachmentEtag,
				Stats = replicationTask == null ? new List<DestinationStats>() : replicationTask.DestinationStats.Values.ToList()
			};
			return GetMessageWithObject(replicationStatistics);
		}

		private void ReplicateDocument(IStorageActionsAccessor actions, string id, RavenJObject metadata, RavenJObject document, string src)
		{
			new DocumentReplicationBehavior
			{
				Actions = actions,
				Database = Database,
				ReplicationConflictResolvers = DocsReplicationConflictResolvers,
				Src = src
			}.Replicate(id, metadata, document);
		}

		private void ReplicateAttachment(IStorageActionsAccessor actions, string id, RavenJObject metadata, byte[] data, string src)
		{
			new AttachmentReplicationBehavior
			{
				Actions = actions,
				Database = Database,
				ReplicationConflictResolvers = AttachmentReplicationConflictResolvers,
				Src = src
			}.Replicate(id, metadata, data);
		}
	}
}