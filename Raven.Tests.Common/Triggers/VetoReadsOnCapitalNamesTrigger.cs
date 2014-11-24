using System.Linq;

using Raven.Abstractions.Data;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Tests.Common.Triggers
{
	public class VetoReadsOnCapitalNamesTrigger : AbstractReadTrigger
	{
		public override ReadVetoResult AllowRead(string key, RavenJObject metadata, ReadOperation operation, TransactionInformation transactionInformation)
		{
			if (operation == ReadOperation.Index)
				return ReadVetoResult.Allowed;
			var name = metadata["name"];
			if (name != null && name.Value<string>().Any(char.IsUpper))
			{
				return ReadVetoResult.Deny("Upper case characters in the 'name' property means the document is a secret!");
			}
			return ReadVetoResult.Allowed;
		}
	}
}