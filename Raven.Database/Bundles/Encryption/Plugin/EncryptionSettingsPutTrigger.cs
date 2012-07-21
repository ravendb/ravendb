using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Bundles.Encryption.Settings;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Bundles.Encryption.Plugin
{
	[InheritedExport(typeof(AbstractPutTrigger))]
	[ExportMetadata("Order", 10000)]
	[ExportMetadata("Bundle", "Encryption")]
	public class EncryptionSettingsPutTrigger : AbstractPutTrigger
	{
		public override VetoResult AllowPut(string key, RavenJObject document, RavenJObject metadata, TransactionInformation transactionInformation)
		{
			if (key == Constants.InDatabaseKeyVerificationDocumentName)
			{
				if (Database == null) // we haven't been intialized yet
					return VetoResult.Allowed;

				if (Database.Get(key, null) != null)
					return VetoResult.Deny("The encryption verification document already exists and cannot be overwritten.");
			}

			return VetoResult.Allowed;
		}
	}
}
