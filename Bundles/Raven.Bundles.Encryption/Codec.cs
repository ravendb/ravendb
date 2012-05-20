using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Bundles.Encryption.Settings;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Bundles.Encryption
{
	internal static class Codec
	{
		private static readonly ThreadLocal<MD5> LocalMD5 = new ThreadLocal<MD5>(() => MD5.Create());
		private static int? encryptionKeySize = null;
		private static int? encryptionIVSize = null;

		public static EncryptionSettings EncryptionSettings
		{
			get { return EncryptionSettingsManager.EncryptionSettings; }
		}

		public static Stream Encode(string key, Stream dataStream)
		{
			return new CryptoStream(dataStream, GetCryptoProvider(key).CreateEncryptor(), CryptoStreamMode.Write);
		}

		public static Stream Decode(string key, Stream dataStream)
		{
			return new CryptoStream(dataStream, GetCryptoProvider(key).CreateDecryptor(), CryptoStreamMode.Read);
		}

		public static EncodedBlock EncodeBlock(string key, byte[] data)
		{
			byte[] iv;
			var transform = GetCryptoProviderWithRandomIV(key, out iv).CreateEncryptor();

			return new EncodedBlock(iv, transform.TransformEntireBlock(data));
		}

		public static byte[] DecodeBlock(string key, EncodedBlock block)
		{
			var transform = GetCryptoProvider(key, block.IV).CreateDecryptor();

			return transform.TransformEntireBlock(block.Data);
		}

		public static int GetIVLength()
		{
			if (encryptionIVSize == null)
			{
				// This will force detection of the iv size
				GetCryptoProvider("");
			}

			return encryptionIVSize.Value;
		}

		private static SymmetricAlgorithm GetCryptoProvider(string key)
		{
			return GetCryptoProvider(key, null);
		}
		
		private static SymmetricAlgorithm GetCryptoProvider(string key, byte[] iv)
		{
			var result = EncryptionSettings.GenerateAlgorithm();
			encryptionKeySize = encryptionKeySize ?? GetKeySizeForEncryption(result);
			encryptionIVSize = encryptionIVSize ?? GetIVSizeForEncryption(result);

			if (iv != null && iv.Length != encryptionIVSize)
				throw new ArgumentException("GetCryptoProvider: IV has wrong length. Given length: " + iv.Length + ", expectd length: " + encryptionIVSize);

			var passwordBytes = new Rfc2898DeriveBytes(EncryptionSettings.EncryptionKey, GetSaltFromDocumentKey(key), Constants.Rfc2898Iterations);

			result.Key = passwordBytes.GetBytes(encryptionKeySize.Value);

			if (iv != null)
				iv = passwordBytes.GetBytes(encryptionIVSize.Value);
			result.IV = iv;
			return result;
		}

		private static SymmetricAlgorithm GetCryptoProviderWithRandomIV(string key, out byte[] iv)
		{
			var rng = new RNGCryptoServiceProvider();
			iv = new byte[GetIVLength()];
			rng.GetBytes(iv);

			return GetCryptoProvider(key, iv);
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

		public struct EncodedBlock
		{
			public EncodedBlock(byte[] iv, byte[] data)
			{
				this.IV = iv;
				this.Data = data;
			}

			public readonly byte[] IV;
			public readonly byte[] Data;
		}
	}
}
