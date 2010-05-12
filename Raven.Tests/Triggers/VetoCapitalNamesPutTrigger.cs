using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Plugins;
using System.Linq;

namespace Raven.Tests.Triggers
{
	public class VetoCapitalNamesPutTrigger : IPutTrigger
	{
        public VetoResult AllowPut(string key, JObject document, JObject metadata, TransactionInformation transactionInformation)
		{
			var name = document["name"];
			if(name != null && name.Value<string>().Any(char.IsUpper))
			{
				return VetoResult.Deny("Can't use upper case characters in the 'name' property");
			}
			return VetoResult.Allowed;
		}

		public void OnPut(string key, JObject document, JObject metadata, TransactionInformation transactionInformation)
		{
		}

		public void AfterCommit(string key, JObject document, JObject metadata)
		{
		}
	}
}