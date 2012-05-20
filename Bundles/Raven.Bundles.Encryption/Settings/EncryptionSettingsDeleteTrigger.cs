using Raven.Abstractions.Data;
using Raven.Database.Plugins;

namespace Raven.Bundles.Encryption.Settings
{
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