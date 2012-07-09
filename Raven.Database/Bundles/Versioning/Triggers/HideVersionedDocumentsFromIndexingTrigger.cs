using System.ComponentModel.Composition;
using Raven.Abstractions.Data;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Bundles.Versioning.Triggers
{
	[InheritedExport(typeof(AbstractReadTrigger))]
	[ExportMetadata("Bundle", "Versioning")]
	public class HideVersionedDocumentsFromIndexingTrigger : AbstractReadTrigger
	{
		public override ReadVetoResult AllowRead(string key, RavenJObject metadata, ReadOperation operation, TransactionInformation transactionInformation)
		{
			if (operation != ReadOperation.Index)
				return ReadVetoResult.Allowed;

			if (metadata.Value<string>(VersioningUtil.RavenDocumentRevisionStatus) == "Historical" && Database.IsVersioningActive(metadata))
				return ReadVetoResult.Ignore;

			return ReadVetoResult.Allowed;
		}
	}
}