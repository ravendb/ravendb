namespace Raven.Database.Bundles.Replication.Plugins
{
	using System;
	using System.ComponentModel.Composition;
	using System.Linq;

	using Raven.Abstractions.Data;
	using Raven.Bundles.Replication.Plugins;
	using Raven.Database.Bundles.Replication.Impl;
	using Raven.Json.Linq;

	[ExportMetadata("Bundle", "Replication")]
	[InheritedExport(typeof(AbstractDocumentReplicationConflictResolver))]
	public class DefaultDocumentReplicationConflictResolver : AbstractDocumentReplicationConflictResolver
	{
		public override bool TryResolve(string id, RavenJObject metadata, RavenJObject document, JsonDocument existingDoc, Func<string, JsonDocument> getDocument)
		{
			var existingDocumentIsInConflict = existingDoc.Metadata[Constants.RavenReplicationConflict] != null;
			var existingDocumentIsDeleted = existingDoc.Metadata[Constants.RavenDeleteMarker] != null
											&& existingDoc.Metadata[Constants.RavenDeleteMarker].Value<bool>();

			if (existingDocumentIsInConflict && existingDocumentIsDeleted == false)
			{
				var conflictIds =
					existingDoc.DataAsJson.Value<RavenJArray>("Conflicts")
					.Select(x => x.Value<string>())
					.ToArray();

				if (conflictIds.Length == 0) return false;

				return
					conflictIds
					.Select(getDocument)
					.All(doc => Historian.IsDirectChildOfCurrent(metadata, doc.Metadata));
			}

			return false;
		}
	}
}
