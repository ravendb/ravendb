//-----------------------------------------------------------------------
// <copyright file="AttachmentReplicationResponder.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Security.Cryptography;
using System.Text;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Bundles.Replication.Data;
using Raven.Bundles.Replication.Plugins;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Database.Server.Abstractions;
using Raven.Database.Storage;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;
using System.Linq;

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
					byte[] lastEtag = Guid.Empty.ToByteArray();
					foreach (RavenJObject attachment in array)
					{
						var metadata = attachment.Value<RavenJObject>("@metadata");
						if(metadata[Constants.RavenReplicationSource] == null)
						{
							// not sure why, old attachment from when the user didn't have replciation
							// that we suddenly decided to replicate, choose the source for that
							metadata[Constants.RavenReplicationSource] = RavenJToken.FromObject(src);
						}
						lastEtag = attachment.Value<byte[]>("@etag");
						var id = attachment.Value<string>("@id");
						ReplicateAttachment(actions, id, metadata, attachment.Value<byte[]>("data"), new Guid(lastEtag), src);
					}


					var replicationDocKey = Constants.RavenReplicationSourcesBasePath + "/" + src;
					var replicationDocument = Database.Get(replicationDocKey,null);
					var lastDocId = Guid.Empty;
					if(replicationDocument != null)
					{
						lastDocId =
							replicationDocument.DataAsJson.JsonDeserialization<SourceReplicationInformation>().
								LastDocumentEtag;
					}
					Database.Put(replicationDocKey, null,
								 RavenJObject.FromObject(new SourceReplicationInformation
								 {
									 LastDocumentEtag = lastDocId,
									 LastAttachmentEtag = new Guid(lastEtag),
									 ServerInstanceId = Database.TransactionalStorage.Id
								 }),
								 new RavenJObject(), null);
				});
			}
		}

		private void ReplicateAttachment(IStorageActionsAccessor actions, string id, RavenJObject metadata, byte[] data, Guid lastEtag ,string src)
		{
			new AttachmentReplicationBehavior
			{
				Actions = actions,
				Database = Database,
				ReplicationConflictResolvers = ReplicationConflictResolvers,
				Src = src
			}.Replicate(id, metadata, data);
		}

		private static string HashReplicationIdentifier(RavenJObject metadata, Guid lastEtag)
		{
			using (var md5 = MD5.Create())
			{
				var bytes = Encoding.UTF8.GetBytes(metadata.Value<string>(Constants.RavenReplicationSource) + "/" + lastEtag);
				return new Guid(md5.ComputeHash(bytes)).ToString();
			}
		}

		private string HashReplicationIdentifier(Guid existingEtag)
		{
			using (var md5 = MD5.Create())
			{
				var bytes = Encoding.UTF8.GetBytes(Database.TransactionalStorage.Id + "/" + existingEtag);
				return new Guid(md5.ComputeHash(bytes)).ToString();
			}
		}

		private static bool IsDirectChildOfCurrentAttachment(Attachment existingAttachment, RavenJObject metadata)
		{
			var version = new RavenJObject
			{
				{Constants.RavenReplicationSource, existingAttachment.Metadata[Constants.RavenReplicationSource]},
				{Constants.RavenReplicationVersion, existingAttachment.Metadata[Constants.RavenReplicationVersion]},
			};

			var history = metadata[Constants.RavenReplicationHistory];
			if (history == null) // no history, not a parent
				return false;

			if (history.Type != JTokenType.Array)
				return false;

			return history.Values().Contains(version, new RavenJTokenEqualityComparer());
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
