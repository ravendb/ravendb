using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Bundles.Encryption
{
	internal static class Codec
	{
		private static readonly ThreadLocal<MD5> LocalMD5 = new ThreadLocal<MD5>(() => MD5.Create());
		private static int? encryptionKeySize = null;
		private static int? encryptionIVSize = null;
		
		internal static EncryptionSettings EncryptionSettings
		{
			get { return EncryptionSettingsManager.EncryptionSettings; }
		}

		public static Stream Encode(string key, Stream dataStream)
		{
			if (EncryptionSettings == null)
				return dataStream;

			return new CryptoStream(dataStream, GetCryptoProvider(key).CreateEncryptor(), CryptoStreamMode.Write);
		}

		public static Stream Decode(string key, Stream dataStream)
		{
			if (EncryptionSettings == null || EncryptionSettings.DontEncrypt(key))
				return dataStream;

			return new CryptoStream(dataStream, GetCryptoProvider(key).CreateDecryptor(), CryptoStreamMode.Read);
		}

		private static SymmetricAlgorithm GetCryptoProvider(string key)
		{
			var result = EncryptionSettings.GenerateAlgorithm();
			encryptionKeySize = encryptionKeySize ?? GetKeySizeForEncryption(result);
			encryptionIVSize = encryptionIVSize ?? GetIVSizeForEncryption(result);

			var passwordBytes = new Rfc2898DeriveBytes(EncryptionSettings.EncryptionKey, GetSaltFromDocumentKey(key), Constants.Rfc2898Iterations);

			result.Key = passwordBytes.GetBytes(encryptionKeySize.Value);
			result.IV = passwordBytes.GetBytes(encryptionIVSize.Value);
			return result;
		}

		private static byte[] GetSaltFromDocumentKey(string key)
		{
			return LocalMD5.Value.ComputeHash(Encoding.UTF8.GetBytes(key));
		}

		private static int GetKeySizeForEncryption(SymmetricAlgorithm algorithm)
		{
			int bits;
			if (algorithm.ValidKeySize(Constants.DefaultKeySizeToUseInActualEncryptionInBits))
				bits = Constants.DefaultKeySizeToUseInActualEncryptionInBits;
			else
				bits = algorithm.LegalKeySizes[0].MaxSize;

			return bits / 8;
		}

		private static int GetIVSizeForEncryption(SymmetricAlgorithm result)
		{
			return result.IV.Length;
		}
	}
}
