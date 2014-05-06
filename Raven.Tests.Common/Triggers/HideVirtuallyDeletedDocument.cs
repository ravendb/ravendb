using Raven.Abstractions.Data;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Tests.Common.Triggers
{
	public class HideVirtuallyDeletedDocument : AbstractReadTrigger
	{
		public override ReadVetoResult AllowRead(string key, RavenJObject metadata, ReadOperation operation, TransactionInformation transactionInformation)
		{
			if (operation != ReadOperation.Index)
				return ReadVetoResult.Allowed;
			if (metadata.ContainsKey("Deleted") == false)
				return ReadVetoResult.Allowed;
			return ReadVetoResult.Ignore;
		}
	}
}