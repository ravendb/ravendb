using System.ComponentModel.Composition;
using Raven.Abstractions.Data;
using Raven.Database.Plugins;

namespace Raven.Bundles.Encryption.Plugin
{
	[InheritedExport(typeof(AbstractDeleteTrigger))]
	[ExportMetadata("Order", 10000)]
	[ExportMetadata("Bundle", "Encryption")]
	public class EncryptionSettingsDeleteTrigger : AbstractDeleteTrigger
	{
		public override VetoResult AllowDelete(string key, TransactionInformation transactionInformation)
		{
			if (key == Constants.InDatabaseKeyVerificationDocumentName)
				return VetoResult.Deny("Cannot delete the encryption verification document.");

			return base.AllowDelete(key, transactionInformation);
		}
	}
}