//-----------------------------------------------------------------------
// <copyright file="AttachmentReplicationResponder.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using NLog;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Bundles.Replication.Data;
using Raven.Bundles.Replication.Plugins;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Responders;
using Raven.Database.Storage;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;
using System.Linq;

namespace Raven.Bundles.Replication.Responders
{
	[ExportMetadata("Bundle", "Replication")]
	[InheritedExport(typeof(AbstractRequestResponder))]
	public class AttachmentReplicationResponder : RequestResponder
	{
		private Logger log = LogManager.GetCurrentClassLogger();

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
						if(metadata[ReplicationConstants.RavenReplicationSource] == null)
						{
							// not sure why, old attachment from when the user didn't have replciation
							// that we suddenly decided to replicate, choose the source for that
							metadata[ReplicationConstants.RavenReplicationSource] = RavenJToken.FromObject(src);
						}
						lastEtag = attachment.Value<byte[]>("@etag");
						var id = attachment.Value<string>("@id");
						ReplicateAttachment(actions, id, metadata, attachment.Value<byte[]>("data"), new Guid(lastEtag), src);
					}


					var replicationDocKey = ReplicationConstants.RavenReplicationSourcesBasePath + "/" + src;
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
			var existingAttachment = actions.Attachments.GetAttachment(id);
			if (existingAttachment == null)
			{
				log.Debug("New attachment {0} replicated successfully from {1}", id, src);
				actions.Attachments.AddAttachment(id, Guid.Empty, new MemoryStream(data), metadata);
				return;
			}

			// we just got the same version from the same source - request playback again?
			// at any rate, not an error, moving on
			if(existingAttachment.Metadata.Value<string>(ReplicationConstants.RavenReplicationSource) == metadata.Value<string>(ReplicationConstants.RavenReplicationSource)  
				&& existingAttachment.Metadata.Value<int>(ReplicationConstants.RavenReplicationVersion) == metadata.Value<int>(ReplicationConstants.RavenReplicationVersion))
			{
				return;
			}
			
			var existingDocumentIsInConflict = existingAttachment.Metadata[ReplicationConstants.RavenReplicationConflict] != null;
			if (existingDocumentIsInConflict == false &&                    // if the current document is not in conflict, we can continue without having to keep conflict semantics
				(IsDirectChildOfCurrentAttachment(existingAttachment, metadata))) // this update is direct child of the existing doc, so we are fine with overwriting this
			{
				log.Debug("Existing document {0} replicated successfully from {1}", id, src);
				actions.Attachments.AddAttachment(id, null, new MemoryStream(data), metadata);
				return;
			}

			if (ReplicationConflictResolvers.Any(replicationConflictResolver => replicationConflictResolver.TryResolve(id, metadata, data, existingAttachment)))
			{
				actions.Attachments.AddAttachment(id, null, new MemoryStream(data), metadata);
				return;
			}

			var newDocumentConflictId = id + "/conflicts/" + HashReplicationIdentifier(metadata, lastEtag);
			metadata.Add(ReplicationConstants.RavenReplicationConflict, RavenJToken.FromObject(true));
			actions.Attachments.AddAttachment(newDocumentConflictId, null, new MemoryStream(data), metadata);

			if (existingDocumentIsInConflict) // the existing document is in conflict
			{
				log.Debug("Conflicted document {0} has a new version from {1}, adding to conflicted documents", id, src);
				
				// just update the current doc with the new conflict document
				var conflictArray = existingAttachment.Metadata.Value<RavenJArray>("Conflicts");
				if (conflictArray == null)
					existingAttachment.Metadata["Conflicts"] = conflictArray = new RavenJArray();

				conflictArray.Add(RavenJToken.FromObject(newDocumentConflictId));
				actions.Attachments.AddAttachment(id, existingAttachment.Etag, existingAttachment.Data(), existingAttachment.Metadata);
				return;
			}
			log.Debug("Existing document {0} is in conflict with replicated version from {1}, marking document as conflicted", id, src);
				
			// we have a new conflict
			// move the existing doc to a conflict and create a conflict document
			var existingDocumentConflictId = id + "/conflicts/" + HashReplicationIdentifier(existingAttachment.Etag);
			
			existingAttachment.Metadata.Add(ReplicationConstants.RavenReplicationConflict, RavenJToken.FromObject(true));
			actions.Attachments.AddAttachment(existingDocumentConflictId, null, existingAttachment.Data(), existingAttachment.Metadata);
			var conflictAttachment = new RavenJObject
			{
				{"Conflicts", new RavenJArray(existingDocumentConflictId, newDocumentConflictId)}
			};
			var memoryStream = new MemoryStream();
			conflictAttachment.WriteTo(memoryStream);
			memoryStream.Position = 0;
			actions.Attachments.AddAttachment(id, null,
								memoryStream,
								new RavenJObject
								{
									{ReplicationConstants.RavenReplicationConflict, true},
									{"@Http-Status-Code", 409},
									{"@Http-Status-Description", "Conflict"}
								});
		}

		private static string HashReplicationIdentifier(RavenJObject metadata, Guid lastEtag)
		{
			using (var md5 = MD5.Create())
			{
				var bytes = Encoding.UTF8.GetBytes(metadata.Value<string>(ReplicationConstants.RavenReplicationSource) + "/" + lastEtag);
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
				{ReplicationConstants.RavenReplicationSource, existingAttachment.Metadata[ReplicationConstants.RavenReplicationSource]},
				{ReplicationConstants.RavenReplicationVersion, existingAttachment.Metadata[ReplicationConstants.RavenReplicationVersion]},
			};

			var history = metadata[ReplicationConstants.RavenReplicationHistory];
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
