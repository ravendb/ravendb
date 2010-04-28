using System.ComponentModel.Composition;
using Newtonsoft.Json.Linq;
using Raven.Database.Plugins;
using System.Linq;

namespace Raven.Tests.Triggers
{
	[Export(typeof(IPutTrigger))]
	public class VetoCapitalNamesPutTrigger : IPutTrigger
	{
		public VetoResult AllowPut(string key, JObject document, JObject metadata)
		{
			var name = document["name"];
			if(name != null && name.Value<string>().Any(char.IsUpper))
			{
				return VetoResult.Deny("Can't use upper case characters in the 'name' property");
			}
			return VetoResult.Allowed;
		}

		public void OnPut(string key, JObject document, JObject metadata)
		{
		}

		public void AfterCommit(string key, JObject document, JObject metadata)
		{
		}
	}
}