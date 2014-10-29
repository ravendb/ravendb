using Raven.Abstractions.Data;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Tests.Common.Triggers
{
	public class HiddenDocumentsTrigger : AbstractReadTrigger
	{
		public override ReadVetoResult AllowRead(string key, RavenJObject metadata, ReadOperation operation, TransactionInformation transactionInformation)
		{
			if (operation == ReadOperation.Index)
				return ReadVetoResult.Allowed;
			var name = metadata["hidden"];
			if (name != null && name.Value<bool>())
			{
				return ReadVetoResult.Ignore;
			}
			return ReadVetoResult.Allowed;
		}
	}
}