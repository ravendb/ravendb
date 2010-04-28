using System;
using System.ComponentModel.Composition;
using Newtonsoft.Json.Linq;
using Raven.Database.Plugins;

namespace Raven.Tests.Triggers
{
	[Export(typeof(IPutTrigger))]
	public class AuditPutTrigger : IPutTrigger
	{
		public VetoResult AllowPut(string key, JObject document, JObject metadata)
		{
			return VetoResult.Allowed;
		}

		public void OnPut(string key, JObject document, JObject metadata)
		{
			document["created_at"] = new JValue(new DateTime(2000, 1, 1));
		}

		public void AfterCommit(string key, JObject document, JObject metadata)
		{
		}
	}
}