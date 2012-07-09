using System.ComponentModel.Composition;
using Raven.Abstractions.Data;
using Raven.Bundles.Quotas.Size;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Bundles.Quotas.Documents.Triggers
{
	[InheritedExport(typeof(AbstractPutTrigger))]
	[ExportMetadata("Bundle", "Quotas")]
	public class DatabaseCountQoutaForDocumentsPutTrigger : AbstractPutTrigger
	{
		public override VetoResult AllowPut(string key, RavenJObject document, RavenJObject metadata,
		                                    TransactionInformation transactionInformation)
		{
			return SizeQuotaConfiguration.GetConfiguration(Database).AllowPut();
		}

	}
}