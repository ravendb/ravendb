using System;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Plugins;

namespace Raven.Tests.Triggers
{
	public class AuditPutTrigger : AbstractPutTrigger
	{
        public override VetoResult AllowPut(string key, JObject document, JObject metadata, TransactionInformation transactionInformation)
		{
			return VetoResult.Allowed;
		}

		public override void OnPut(string key, JObject document, JObject metadata, TransactionInformation transactionInformation)
		{
			document["created_at"] = new JValue(new DateTime(2000, 1, 1,0,0,0,DateTimeKind.Utc));
		}
	}
}
