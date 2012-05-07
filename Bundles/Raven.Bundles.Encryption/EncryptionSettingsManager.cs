using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Bundles.Encryption
{
	internal class EncryptionSettingsManager : AbstractPutTrigger
	{
		public static EncryptionSettings EncryptionSettings { get; private set; }

		public override void Initialize()
		{
			var settingsDocument = Database.Get(Constants.EncryptionSettingsDocumentKey, null);
			RavenJObject json;
			if (settingsDocument != null)
			{
				json = settingsDocument.DataAsJson;
			}
			else
			{
				json = JsonExtensions.ToJObject(new EncryptionSettings());
				Database.Put(Constants.EncryptionSettingsDocumentKey, null, json, new RavenJObject(), null);
			}

			EncryptionSettings = json.JsonDeserialization<EncryptionSettings>();
		}

		public override VetoResult AllowPut(string key, RavenJObject document, RavenJObject metadata, TransactionInformation transactionInformation)
		{
			if (key == Constants.EncryptionSettingsDocumentKey)
			{
				if (EncryptedDocumentsExist())
					return VetoResult.Deny("The encryption settings may not be changed when the database already contains encrypted data. Use a migration procedure instead.");
			}

			return VetoResult.Allowed;
		}

		private bool EncryptedDocumentsExist()
		{
			const int pageSize = 10;
			int index = 0;
			while (true)
			{
				var array = Database.GetDocuments(index, index + pageSize, null);
				if (array.Length == 0)
				{
					// We've gone over all the documents in the database, and none of them are encrypted.
					return false;
				}

				if (array.All(x => EncryptionSettings.DontEncrypt(x.Value<RavenJObject>("@metadata").Value<string>("@id"))))
				{
					index += array.Length;
					continue;
				}
				else
				{
					// Found a document which is encrypted
					return true;
				}
			}
		}
	}
}
