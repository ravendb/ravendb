using System;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Json.Linq;
using Raven.Abstractions.Logging;
using Raven.Bundles.Encryption.Settings;
using Raven.Database.FileSystem;
using Raven.Database.Server.Abstractions;
using Raven.Json.Linq;

namespace Raven.Database.Bundles.Encryption.Settings
{
	internal static class EncryptionSettingsManager
	{
		private static readonly string EncryptionSettingsKeyInExtensionsState = Guid.NewGuid().ToString();
		private static readonly ILog log = LogManager.GetCurrentClassLogger();

		public static EncryptionSettings GetEncryptionSettingsForResource(IResourceStore resource)
		{
			var result = (EncryptionSettings)resource.ExtensionsState.GetOrAdd(EncryptionSettingsKeyInExtensionsState, _ =>
			{
				var type = GetTypeFromName(resource.Configuration.Settings[Constants.AlgorithmTypeSetting]);
				var key = GetKeyFromBase64(resource.Configuration.Settings[Constants.EncryptionKeySetting], resource.Configuration.Encryption.EncryptionKeyBitsPreference);
				var encryptIndexes = GetEncryptIndexesFromString(resource.Configuration.Settings[Constants.EncryptIndexes], true);

				return new EncryptionSettings(key, type, encryptIndexes, resource.Configuration.Encryption.EncryptionKeyBitsPreference);
			});

			return result;
		}

		/// <summary>
		/// A wrapper around Convert.FromBase64String, with extra validation and relevant exception messages.
		/// </summary>
		private static byte[] GetKeyFromBase64(string base64, int defaultEncryptionKeySize)
		{
			if (string.IsNullOrWhiteSpace(base64))
				throw new ConfigurationErrorsException("The " + Constants.EncryptionKeySetting + " setting must be set to an encryption key. "
					+ "The key should be in base 64, and should be at least " + Constants.MinimumAcceptableEncryptionKeyLength
					+ " bytes long. You may use EncryptionSettings.GenerateRandomEncryptionKey() to generate a key.\n"
					+ "If you'd like, here's a key that was randomly generated:\n"
					+ "<add key=\"Raven/Encryption/Key\" value=\""
					+ Convert.ToBase64String(EncryptionSettings.GenerateRandomEncryptionKey(defaultEncryptionKeySize))
					+ "\" />");

			try
			{
				var result = Convert.FromBase64String(base64);
				if (result.Length < Constants.MinimumAcceptableEncryptionKeyLength)
					throw new ConfigurationErrorsException("The " + Constants.EncryptionKeySetting + " setting must be at least "
						+ Constants.MinimumAcceptableEncryptionKeyLength + " bytes long.");

				return result;
			}
			catch (FormatException e)
			{
				throw new ConfigurationErrorsException("The " + Constants.EncryptionKeySetting + " setting has an invalid base 64 value.", e);
			}
		}

		/// <summary>
		/// A wrapper around Type.GetType, with extra validation and a default value.
		/// </summary>
		private static Type GetTypeFromName(string typeName)
		{
			if (string.IsNullOrWhiteSpace(typeName))
				return Constants.DefaultCryptoServiceProvider;

			var result = Type.GetType(typeName);

			if (result == null)
				throw new ConfigurationErrorsException("Unknown type for encryption: " + typeName);

			if (!result.IsSubclassOf(typeof(System.Security.Cryptography.SymmetricAlgorithm)))
				throw new ConfigurationErrorsException("The encryption algorithm type must be a subclass of System.Security.Cryptography.SymmetricAlgorithm.");
			if (result.IsAbstract)
				throw new ConfigurationErrorsException("Cannot use an abstract type for an encryption algorithm.");

			return result;
		}

		/// <summary>
		/// Uses an encrypted document to verify that the encryption key is correct and decodes it to the right value.
		/// </summary>
		public static void VerifyEncryptionKey(RavenFileSystem fileSystem, EncryptionSettings settings)
		{
			RavenJObject config = null;
			try
			{
				fileSystem.Storage.Batch(accessor =>
				{
					try
					{
						config = accessor.GetConfig(Constants.InResourceKeyVerificationDocumentName);
					}
					catch (FileNotFoundException)
					{
					}
				});
			}
			catch (CryptographicException e)
			{
				throw new ConfigurationErrorsException("The file system is encrypted with a different key and/or algorithm than the ones "
					+ "currently in the configuration file.", e);
			}

			if (config != null)
			{
				if (!RavenJTokenEqualityComparer.Default.Equals(config, Constants.InResourceKeyVerificationDocumentContents))
					throw new ConfigurationErrorsException("The file system is encrypted with a different key and/or algorithm than the ones is currently configured");
			}
			else
			{
				// This is the first time the file system is loaded.
				if (EncryptedFileExist(fileSystem))
					throw new InvalidOperationException("The file system already has existing files, you cannot start using encryption now.");

				var clonedDoc = (RavenJObject)Constants.InResourceKeyVerificationDocumentContents.CreateSnapshot();
				fileSystem.Storage.Batch(accessor => accessor.SetConfig(Constants.InResourceKeyVerificationDocumentName, clonedDoc));
			}
		}

		/// <summary>
		/// Uses an encrypted document to verify that the encryption key is correct and decodes it to the right value.
		/// </summary>
		public static void VerifyEncryptionKey(DocumentDatabase database, EncryptionSettings settings)
		{
			JsonDocument doc;
			try
			{
				doc = database.Documents.Get(Constants.InResourceKeyVerificationDocumentName, null);
			}
			catch (Exception e)
			{
				if (e is CryptographicException)
				{
					throw new ConfigurationErrorsException("The database is encrypted with a different key and/or algorithm than the ones "
													   + "currently in the configuration file.", e);
				}
				if (settings.Codec.UsingSha1)
					throw;
				
				log.Debug("Couldn't decrypt the database using MD5. Trying with SHA1.");
				settings.Codec.UseSha1();
				VerifyEncryptionKey(database, settings);
				return;
			}

			if (doc != null)
			{
				if (!RavenJTokenEqualityComparer.Default.Equals(doc.DataAsJson, Constants.InResourceKeyVerificationDocumentContents))
					throw new ConfigurationErrorsException("The database is encrypted with a different key and/or algorithm than the ones "
						+ "currently in the configuration file.");
			}
			else
			{
				// This is the first time the database is loaded.
				if (EncryptedDocumentsExist(database))
					throw new InvalidOperationException("The database already has existing documents, you cannot start using encryption now.");

				var clonedDoc = (RavenJObject)Constants.InResourceKeyVerificationDocumentContents.CreateSnapshot();
				database.Documents.Put(Constants.InResourceKeyVerificationDocumentName, null, clonedDoc, new RavenJObject(), null);
			}
		}

		private static bool EncryptedDocumentsExist(DocumentDatabase database)
		{
			const int pageSize = 10;
			int index = 0;
			while (true)
			{
				var array = database.Documents.GetDocuments(index, index + pageSize, null, CancellationToken.None);
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
				// Found a document which is encrypted
				return true;
			}
		}

		private static bool EncryptedFileExist(RavenFileSystem fileSystem)
		{
			const int pageSize = 10;
			var start = Guid.Empty;

			bool foundEncryptedDoc = false;

			while (true)
			{
				var foundMoreDocs = false;

				fileSystem.Storage.Batch(accessor =>
				{
					var fileHeaders = accessor.GetFilesAfter(start, pageSize);

					foreach (var fileHeader in fileHeaders)
					{
						foundMoreDocs = true;

						if (EncryptionSettings.DontEncrypt(fileHeader.Name) == false)
						{
							foundEncryptedDoc = true;
							break;
						}

						start = fileHeader.Etag;
					}
				});

				if (foundEncryptedDoc || foundMoreDocs == false)
					break;
			}

			return foundEncryptedDoc;
		}

		private static bool GetEncryptIndexesFromString(string value, bool defaultValue)
		{
			if (string.IsNullOrWhiteSpace(value))
				return defaultValue;

			try
			{
				return Convert.ToBoolean(value);
			}
			catch (Exception e)
			{
				throw new ConfigurationErrorsException("Invalid boolean value for setting EncryptIndexes: " + value, e);
			}
		}
	}
}
