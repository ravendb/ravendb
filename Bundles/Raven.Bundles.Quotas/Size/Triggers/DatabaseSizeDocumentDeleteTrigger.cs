using System.ComponentModel.Composition;
using Raven.Database.Plugins;

namespace Raven.Bundles.Quotas.Size.Triggers
{
	[InheritedExport(typeof(AbstractDeleteTrigger))]
	[ExportMetadata("Bundle", "Quotas")]
	public class DatabaseSizeDocumentDeleteTrigger : AbstractDeleteTrigger
	{
		public override void AfterDelete(string key, Abstractions.Data.TransactionInformation transactionInformation)
		{
			SizeQuotaConfiguration.GetConfiguration(Database).AfterDelete();
		}
	}
}