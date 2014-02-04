namespace Raven.Database.Bundles.Replication.Plugins
{
	using System;
	using System.Linq;

	using Raven.Abstractions.Data;
	using Raven.Abstractions.Extensions;
	using Raven.Bundles.Replication.Plugins;
	using Raven.Database.Bundles.Replication.Impl;
	using Raven.Json.Linq;

	public class DefaultAttachmentReplicationConflictResolver : AbstractAttachmentReplicationConflictResolver
	{
		public override bool TryResolve(string id, RavenJObject metadata, byte[] data, Attachment existingAttachment,
		                                Func<string, Attachment> getAttachment, out RavenJObject metadataToSave,
		                                out byte[] dataToSave)
		{
			var existingAttachmentIsInConflict = existingAttachment.Metadata[Constants.RavenReplicationConflict] != null;
			var existingAttachmentIsDeleted = existingAttachment.Metadata[Constants.RavenDeleteMarker] != null
			                                  && existingAttachment.Metadata[Constants.RavenDeleteMarker].Value<bool>();

			metadataToSave = null;
			dataToSave = null;

			if (existingAttachmentIsInConflict && existingAttachmentIsDeleted == false)
			{
				var conflictIds =
					existingAttachment.Data().ToJObject().Value<RavenJArray>("Conflicts")
					                  .Select(x => x.Value<string>())
					                  .ToArray();

				if (conflictIds.Length == 0) return false;

				if (conflictIds
					    .Select(getAttachment)
					    .All(doc => Historian.IsDirectChildOfCurrent(metadata, doc.Metadata)) == false)
					return false;

				metadataToSave = metadata;
				dataToSave = data;

				return true;
			}

			return false;
		}
	}
}