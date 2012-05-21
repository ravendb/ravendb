using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Bundles.Encryption.Plugin
{
	public class EncryptionSettingsPutTrigger : AbstractPutTrigger
	{
		public override VetoResult AllowPut(string key, RavenJObject document, RavenJObject metadata, TransactionInformation transactionInformation)
		{
			if (key == Constants.InDatabaseKeyVerificationDocumentName)
			{
				if (Database.Get(key, null) != null)
					return VetoResult.Deny("The encryption verification document already exists and cannot be overwritten.");
			}

			return VetoResult.Allowed;
		}
	}
}
