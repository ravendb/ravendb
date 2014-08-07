//-----------------------------------------------------------------------
// <copyright file="AttachmentReplicationResponder.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util.Encryptors;
using Raven.Bundles.Replication.Data;
using Raven.Bundles.Replication.Plugins;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Database.Server.Abstractions;
using Raven.Database.Storage;
using Raven.Json.Linq;

using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;

namespace Raven.Bundles.Replication.Responders
{
	[ExportMetadata("Bundle", "Replication")]
	[InheritedExport(typeof(AbstractRequestResponder))]
	public class AttachmentReplicationResponder : AbstractRequestResponder
	{
		private ILog log = LogManager.GetCurrentClassLogger();

		[ImportMany]
		public IEnumerable<AbstractAttachmentReplicationConflictResolver> ReplicationConflictResolvers { get; set; }

		public override void Respond(IHttpContext context)
		{
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
			var array = context.ReadBsonArray();
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

						ReplicateAttachment(actions, id, metadata, attachment.Value<byte[]>("data"), lastEtag, src);
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
					if (Guid.TryParse(context.Request.QueryString["dbid"], out serverInstanceId) == false)
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
		}

		private void ReplicateAttachment(IStorageActionsAccessor actions, string id, RavenJObject metadata, byte[] data, Etag lastEtag, string src)
		{
			try
			{
				new AttachmentReplicationBehavior
				{
					Actions = actions,
					Database = Database,
					ReplicationConflictResolvers = ReplicationConflictResolvers,
					Src = src
				}.Replicate(id, metadata, data);
			}
			catch (Exception ex)
			{
				log.ErrorException(
					string.Format("Exception occurred during the replication of the attachment {0} from the server {1}", id, src), ex);
				throw;
			}
		}

		public override string UrlPattern
		{
			get { return "^/replication/replicateAttachments$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "POST" }; }
		}
	}
}
