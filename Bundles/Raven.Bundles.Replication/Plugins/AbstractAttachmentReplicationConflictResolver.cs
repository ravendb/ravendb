using System.ComponentModel.Composition;
using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Bundles.Replication.Plugins
{
	[InheritedExport]
	public abstract class AbstractAttachmentReplicationConflictResolver
	{
		public abstract bool TryResolve(string id, RavenJObject metadata, byte [] data, Attachment existingAttachment);
	}
}