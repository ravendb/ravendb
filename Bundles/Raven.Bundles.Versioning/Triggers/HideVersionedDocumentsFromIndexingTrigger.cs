using Newtonsoft.Json.Linq;
using Raven.Database.Plugins;
using Raven.Http;

namespace Raven.Bundles.Versioning.Triggers
{
	public class HideVersionedDocumentsFromIndexingTrigger : AbstractReadTrigger
	{
		public override ReadVetoResult AllowRead(string key, JObject metadata, ReadOperation operation,
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