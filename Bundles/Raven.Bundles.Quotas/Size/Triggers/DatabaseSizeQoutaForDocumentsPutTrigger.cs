using System;
using Raven.Abstractions.Data;
using Raven.Database.Commercial;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Bundles.Quotas.Triggers
{
	public class DatabaseSizeQoutaForDocumentsPutTrigger : AbstractPutTrigger
	{
		
		
		public override VetoResult AllowPut(string key, RavenJObject document, RavenJObject metadata,
		                                    TransactionInformation transactionInformation)
		{
			return SizeQuotaConfiguration.GetConfiguration(Database).AllowPut();
		}

	}
}