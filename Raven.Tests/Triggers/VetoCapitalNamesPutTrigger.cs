using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Plugins;
using System.Linq;
using Raven.Http;

namespace Raven.Tests.Triggers
{
	public class VetoCapitalNamesPutTrigger : AbstractPutTrigger
	{
        public override VetoResult AllowPut(string key, JObject document, JObject metadata, TransactionInformation transactionInformation)
		{
			var name = document["name"];
			if(name != null && name.Value<string>().Any(char.IsUpper))
			{
				return VetoResult.Deny("Can't use upper case characters in the 'name' property");
			}
			return VetoResult.Allowed;
		}
	}
}
