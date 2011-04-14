using Raven.Abstractions.Data;
using Raven.Database.Plugins;
using Raven.Http;
using Raven.Json.Linq;

namespace Raven.Bundles.Versioning.Triggers
{
	public class HideVersionedDocumentsFromIndexingTrigger : AbstractReadTrigger
	{
		public override ReadVetoResult AllowRead(string key, RavenJObject metadata, ReadOperation operation,
		                                         TransactionInformation transactionInformation)
		{
			if (operation != ReadOperation.Index)
				return ReadVetoResult.Allowed;

			if (metadata.Value<string>(VersioningPutTrigger.RavenDocumentRevisionStatus) == "Historical")
				return ReadVetoResult.Ignore;

			return ReadVetoResult.Allowed;
		}
	}
}