using System.ComponentModel.Composition;
using Raven.Abstractions.Data;
using Raven.Database.Plugins;

namespace Raven.Bundles.Replication.Triggers
{
	[ExportMetadata("Bundle", "Replication")]
	[ExportMetadata("Order", 10000)]
	[InheritedExport(typeof(AbstractReadTrigger))]
	public class PreventIndexingConflictDocumentsReadTrigger : AbstractReadTrigger
	{
		public override ReadVetoResult AllowRead(string key, Json.Linq.RavenJObject metadata, ReadOperation operation, Abstractions.Data.TransactionInformation transactionInformation)
		{
			if (operation == ReadOperation.Index  && metadata.ContainsKey(Constants.RavenReplicationConflictDocument))
			{
				return ReadVetoResult.Ignore;
			}
			return ReadVetoResult.Allowed;
		}
	}
}