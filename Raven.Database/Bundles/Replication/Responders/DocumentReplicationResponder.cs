//-----------------------------------------------------------------------
// <copyright file="DocumentReplicationResponder.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Bundles.Replication.Data;
using Raven.Bundles.Replication.Plugins;
using Raven.Bundles.Replication.Tasks;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Database.Server.Abstractions;
using Raven.Database.Storage;
using Raven.Json.Linq;

using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;

namespace Raven.Bundles.Replication.Responders
{
	[ExportMetadata("Bundle", "Replication")]
	[InheritedExport(typeof(AbstractRequestResponder))]
	public class DocumentReplicationResponder : AbstractRequestResponder
	{
		private readonly ILog log = LogManager.GetCurrentClassLogger();
		private ReplicationTask replicationTask;
		public ReplicationTask ReplicationTask
		{
			get { return replicationTask ?? (replicationTask = Database.StartupTasks.OfType<ReplicationTask>().FirstOrDefault()); }
		}

		[ImportMany]
		public IEnumerable<AbstractDocumentReplicationConflictResolver> ReplicationConflictResolvers { get; set; }


		public override void Respond(IHttpContext context)
		{
			const int PulseThreshold = 512;

			var src = context.Request.QueryString["from"];
			if (string.IsNullOrEmpty(src))
			{
				context.SetStatusToBadRequest();
				return;
			}
			while (src.EndsWith("/"))
				src = src.Substring(0, src.Length - 1);// remove last /, because that has special meaning for Raven
			if (string.IsNullOrEmpty(src))
			{
				context.SetStatusToBadRequest();
				return;
			}
			var array = context.ReadJsonArray();
			if (ReplicationTask != null)
				ReplicationTask.HandleHeartbeat(src);

			using (Database.DisableAllTriggersForCurrentThread())
			{
				IDisposable documentLock = null;

				try
				{
					Database.TransactionalStorage.Batch(actions =>
					{
						string lastEtag = Etag.Empty.ToString();
						for (var i = 0; i < array.Length; i++)
						{
							if (documentLock == null)
								documentLock = Database.DocumentLock.Lock();

							var document = (RavenJObject)array[i];
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

							if (i <= 0 || i % PulseThreshold != 0)
								continue;

							SaveReplicationSource(context, src, lastEtag);
							actions.General.PulseTransaction();

							documentLock.Dispose();
							documentLock = null;
						}

						SaveReplicationSource(context, src, lastEtag);
					});
				}
				finally
				{
					if (documentLock != null)
						documentLock.Dispose();
				}
			}
		}

		private void SaveReplicationSource(IHttpContext context, string src, string lastEtag)
		{
			var replicationDocKey = Constants.RavenReplicationSourcesBasePath + "/" + src;
			var replicationDocument = Database.Get(replicationDocKey, null);
			var lastAttachmentId = Etag.Empty;
			if (replicationDocument != null)
				lastAttachmentId = replicationDocument.DataAsJson.JsonDeserialization<SourceReplicationInformation>().LastAttachmentEtag;

			Guid serverInstanceId;
			if (Guid.TryParse(context.Request.QueryString["dbid"], out serverInstanceId) == false)
				serverInstanceId = Database.TransactionalStorage.Id;

			Database.Put(
				replicationDocKey,
				null,
				RavenJObject.FromObject(
					new SourceReplicationInformation
					{
						Source = src,
						LastDocumentEtag = Etag.Parse(lastEtag),
						LastAttachmentEtag = lastAttachmentId,
						ServerInstanceId = serverInstanceId
					}),
				new RavenJObject(),
				null);
		}

		private void ReplicateDocument(IStorageActionsAccessor actions, string id, RavenJObject metadata, RavenJObject document, string src)
		{
			try
			{
				new DocumentReplicationBehavior
				{
					Actions = actions,
					Database = Database,
					ReplicationConflictResolvers = ReplicationConflictResolvers,
					Src = src
				}.Replicate(id, metadata, document);
			}
			catch (Exception ex)
			{
				log.ErrorException(
					string.Format("Exception occurred during the replication of the document {0} from the server {1}", id, src), ex);
				throw;
			}
		}

		public override string UrlPattern
		{
			get { return "^/replication/replicateDocs$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "POST" }; }
		}
	}
}
