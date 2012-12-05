using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Bundles.Replication.Plugins;
using Raven.Json.Linq;

namespace Raven.Bundles.Replication.Responders
{
	public class AttachmentReplicationBehavior : SingleItemReplicationBehavior<Attachment, byte[]>
	{
		public IEnumerable<AbstractAttachmentReplicationConflictResolver> ReplicationConflictResolvers { get; set; }

		protected override DocumentChangeTypes ReplicationConflict
		{
			get { return DocumentChangeTypes.AttachmentReplicationConflict; }
		}

		protected override void DeleteItem(string id, Guid etag)
		{
			Database.DeleteStatic(id, etag);
		}

		protected override void MarkAsDeleted(string id, RavenJObject metadata)
		{
			Actions.Lists.Set(Constants.RavenReplicationAttachmentsTombstones, id, metadata);
		}

		protected override void AddWithoutConflict(string id, Guid? etag, RavenJObject metadata, byte[] incoming)
		{
			Database.PutStatic(id, etag, new MemoryStream(incoming), metadata);
		}

		protected override void CreateConflict(string id, string newDocumentConflictId, string existingDocumentConflictId, Attachment existingItem, RavenJObject existingMetadata)
		{
			existingItem.Metadata.Add(Constants.RavenReplicationConflict, RavenJToken.FromObject(true));
			Actions.Attachments.AddAttachment(existingDocumentConflictId, null, existingItem.Data(), existingItem.Metadata);
			Actions.Lists.Remove(Constants.RavenReplicationDocsTombstones, id);
			var conflictAttachment = new RavenJObject
			{
				{"Conflicts", new RavenJArray(existingDocumentConflictId, newDocumentConflictId)}
			};
			var memoryStream = new MemoryStream();
			conflictAttachment.WriteTo(memoryStream);
			memoryStream.Position = 0;
			var etag = existingMetadata.Value<bool>(Constants.RavenDeleteMarker) ? Guid.Empty : existingItem.Etag;
			Actions.Attachments.AddAttachment(id, etag,
								memoryStream,
								new RavenJObject
								{
									{Constants.RavenReplicationConflict, true},
									{"@Http-Status-Code", 409},
									{"@Http-Status-Description", "Conflict"}
								});
		}

		protected override void AppendToCurrentItemConflicts(string id, string newConflictId, RavenJObject existingMetadata, Attachment existingItem)
		{
			var existingConflict = existingItem.Data().ToJObject();

			// just update the current attachment with the new conflict document
			var conflictArray = existingConflict.Value<RavenJArray>("Conflicts");
			if (conflictArray == null)
				existingConflict["Conflicts"] = conflictArray = new RavenJArray();

			conflictArray.Add(newConflictId);

			var memoryStream = new MemoryStream();
			existingConflict.WriteTo(memoryStream);
			memoryStream.Position = 0;

			Actions.Attachments.AddAttachment(id, existingItem.Etag, memoryStream, existingItem.Metadata);
		}

		protected override RavenJObject TryGetExisting(string id, out Attachment existingItem, out Guid existingEtag)
		{
			var existingAttachment = Actions.Attachments.GetAttachment(id);
			if (existingAttachment != null)
			{
				existingItem = existingAttachment;
				existingEtag = existingAttachment.Etag;
				return existingAttachment.Metadata;
			}

			var listItem = Actions.Lists.Read(Constants.RavenReplicationAttachmentsTombstones, id);
			if (listItem != null)
			{
				existingEtag = listItem.Etag;
				existingItem = new Attachment
				{
					Etag = listItem.Etag,
					Key = listItem.Key,
					Metadata = listItem.Data,
					Data = () => new MemoryStream()
				};
				return listItem.Data;
			}
			existingEtag = Guid.Empty;
			existingItem = null;
			return null;

		}

		protected override bool TryResolveConflict(string id, RavenJObject metadata, byte[] data, Attachment existing)
		{
			return ReplicationConflictResolvers.Any(replicationConflictResolver =>
			                                        replicationConflictResolver.TryResolve(id, metadata, data, existing, Actions.Attachments.GetAttachment));
		}
	}
}