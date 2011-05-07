using Raven.Database.Plugins;

namespace Raven.Bundles.Quotas.Triggers
{
	public class DatabaseSizeAttachmentDeleteTrigger : AbstractDeleteTrigger
	{
		public override void AfterDelete(string key, Abstractions.Data.TransactionInformation transactionInformation)
		{
			SizeQuotaConfiguration.GetConfiguration(Database).AfterDelete();
		}
	}
}