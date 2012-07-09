using System.ComponentModel.Composition;
using Raven.Abstractions.Data;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Bundles.Quotas.Size.Triggers
{

	[InheritedExport(typeof(AbstractPutTrigger))]
	[ExportMetadata("Bundle", "Quotas")]
	public class DatabaseSizeQoutaForDocumentsPutTrigger : AbstractPutTrigger
	{
		public override VetoResult AllowPut(string key, RavenJObject document, RavenJObject metadata,
		                                    TransactionInformation transactionInformation)
		{
			return SizeQuotaConfiguration.GetConfiguration(Database).AllowPut();
		}

	}
}