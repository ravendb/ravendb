using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Security.Cryptography;
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
			var type = GetTypeFromName(Database.Configuration.Settings[Constants.AlgorithmTypeSetting]);
			var key = GetKeyFromBase64(Database.Configuration.Settings[Constants.EncryptionKeySetting]);

			EncryptionSettings = new EncryptionSettings(key, type);

			VerifyEncryptionKey();
		}

		/// <summary>
		/// A wrapper around Convert.FromBase64String, with extra validation and relevant exception messages.
		/// </summary>
		private byte[] GetKeyFromBase64(string base64)
		{
			if (string.IsNullOrWhiteSpace(base64))
				throw new ConfigurationException("The " + Constants.EncryptionKeySetting + " setting must be set to an encryption key. "
					+ "The key should be in base 64, and should be at least " + Constants.MinimumAcceptableEncryptionKeyLength
					+ " bytes long. You may use EncryptionSettings.GenerateRandomEncryptionKey() to generate a key.\n"
					+ "If you'd like, here's a key that was randomly generated:\n"
					+ Convert.ToBase64String(EncryptionSettings.GenerateRandomEncryptionKey()));

			try
			{
				var result = Convert.FromBase64String(base64);
				if (result.Length < Constants.MinimumAcceptableEncryptionKeyLength)
					throw new ConfigurationException("The " + Constants.EncryptionKeySetting + " setting must be at least "
						+ Constants.MinimumAcceptableEncryptionKeyLength + " bytes long.");

				return result;
			}
			catch (FormatException e)
			{
				throw new ConfigurationException("The " + Constants.EncryptionKeySetting + " setting has an invalid base 64 value.", e);
			}
		}

		/// <summary>
		/// A wrapper around Type.GetType, with extra validation and a default value.
		/// </summary>
		private Type GetTypeFromName(string typeName)
		{
			if (string.IsNullOrWhiteSpace(typeName))
				return Constants.DefaultCryptoServiceProvider;

			var result = Type.GetType(typeName);

			if (result == null)
				throw new ConfigurationException("Unknown type for encryption: " + typeName);

			if (!result.IsSubclassOf(typeof(System.Security.Cryptography.SymmetricAlgorithm)))
				throw new ConfigurationException("The encryption algorithm type must be a subclass of System.Security.Cryptography.SymmetricAlgorithm.");
			if (result.IsAbstract)
				throw new ConfigurationException("Cannot use an abstract type for an encryption algorithm.");

			return result;
		}

		/// <summary>
		/// Uses an encrypted document to verify that the encryption key is correct and decodes it to the right value.
		/// </summary>
		private void VerifyEncryptionKey()
		{
			JsonDocument doc;
			try
			{
				doc = Database.Get(Constants.InDatabaseKeyVerificationDocumentName, null);
			}
			catch (CryptographicException e)
			{
				throw new ConfigurationException("The database is encrypted with a different key and/or algorithm than the ones "
					+ "currently in the configuration file.", e);
			}

			if (doc != null)
			{
				if (!doc.DataAsJson.SequenceEqual(Constants.InDatabaseKeyVerificationDocumentContents))
					throw new ConfigurationException("The database is encrypted with a different key and/or algorithm than the ones "
						+ "currently in the configuration file.");
			}
			else
			{
				// This is the first time the database is loaded.
				if (EncryptedDocumentsExist())
					throw new InvalidOperationException("The database already has existing documents, you cannot start using encryption now.");

				Database.Put(Constants.InDatabaseKeyVerificationDocumentName, null, Constants.InDatabaseKeyVerificationDocumentContents, new RavenJObject(), null);
			}
		}

		public override VetoResult AllowPut(string key, RavenJObject document, RavenJObject metadata, TransactionInformation transactionInformation)
		{
			if (key == Constants.InDatabaseKeyVerificationDocumentName)
			{
				if (Database.Get(key, null) != null)
					return VetoResult.Deny("The encryption verification document already exists and cannot be overwritten.");
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
