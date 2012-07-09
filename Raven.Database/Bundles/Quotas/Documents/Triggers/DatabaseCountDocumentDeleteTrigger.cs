using System.ComponentModel.Composition;
using Raven.Bundles.Quotas.Size;
using Raven.Database.Plugins;

namespace Raven.Bundles.Quotas.Documents.Triggers
{

	[InheritedExport(typeof(AbstractDeleteTrigger))]
	[ExportMetadata("Bundle", "Quotas")]
	public class DatabaseCountDocumentDeleteTrigger : AbstractDeleteTrigger
	{
		public override void AfterDelete(string key, Abstractions.Data.TransactionInformation transactionInformation)
		{
			SizeQuotaConfiguration.GetConfiguration(Database).AfterDelete();
		}
	}
}