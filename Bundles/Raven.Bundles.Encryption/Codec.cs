using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Extensions;
using Raven.Bundles.Encryption.Settings;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Bundles.Encryption
{
	internal static class Codec
	{
		private static readonly ThreadLocal<SHA1> LocalSHA1 = new ThreadLocal<SHA1>(() => SHA1.Create());
		private static readonly ThreadLocal<RNGCryptoServiceProvider> LocalRNG = new ThreadLocal<RNGCryptoServiceProvider>(() => new RNGCryptoServiceProvider());
		private static Tuple<byte[], byte[]> encryptionStartingKeyAndIV = null;
		private static int? encryptionKeySize = null;
		private static int? encryptionIVSize = null;

		public static EncryptionSettings EncryptionSettings
		{
			get { return EncryptionSettingsManager.EncryptionSettings; }
		}

		public static Stream Encode(string key, Stream dataStream)
		{
			return new CryptoStream(dataStream, GetCryptoProvider(null).CreateEncryptor(), CryptoStreamMode.Write).WriteSalt(key);
		}

		public static Stream Decode(string key, Stream dataStream)
		{
			return new CryptoStream(dataStream, GetCryptoProvider(null).CreateDecryptor(), CryptoStreamMode.Read).ReadSalt(key);
		}

		public static EncodedBlock EncodeBlock(string key, byte[] data)
		{
			byte[] iv;
			var transform = GetCryptoProviderWithRandomIV(out iv).CreateEncryptor();

			return new EncodedBlock(iv, transform.TransformEntireBlock(data));
		}

		public static byte[] DecodeBlock(string key, EncodedBlock block)
		{
			var transform = GetCryptoProvider(block.IV).CreateDecryptor();

			return transform.TransformEntireBlock(block.Data);
		}

		private static int GetIVLength()
		{
			if (encryptionIVSize == null)
			{
				// This will force detection of the iv size
				GetCryptoProvider(null);
			}

			return encryptionIVSize.Value;
		}

		private static SymmetricAlgorithm GetCryptoProvider(byte[] iv)
		{
			var result = EncryptionSettings.GenerateAlgorithm();
			encryptionStartingKeyAndIV = encryptionStartingKeyAndIV ?? GetStartingKeyAndIVForEncryption(result);

			if (iv != null && iv.Length != encryptionIVSize)
				throw new ArgumentException("GetCryptoProvider: IV has wrong length. Given length: " + iv.Length + ", expectd length: " + encryptionIVSize);

			result.Key = encryptionStartingKeyAndIV.Item1;
			result.IV = iv ?? encryptionStartingKeyAndIV.Item2;
			return result;
		}

		private static SymmetricAlgorithm GetCryptoProviderWithRandomIV(out byte[] iv)
		{
			iv = new byte[GetIVLength()];
			LocalRNG.Value.GetBytes(iv);

			return GetCryptoProvider(iv);
		}

		private static Tuple<byte[], byte[]> GetStartingKeyAndIVForEncryption(SymmetricAlgorithm algorithm)
		{
			int bits;
			if (algorithm.ValidKeySize(Constants.DefaultKeySizeToUseInActualEncryptionInBits))
				bits = Constants.DefaultKeySizeToUseInActualEncryptionInBits;
			else
				bits = algorithm.LegalKeySizes[0].MaxSize;

			encryptionKeySize = bits / 8;
			encryptionIVSize = algorithm.IV.Length;

			var deriveBytes = new Rfc2898DeriveBytes(EncryptionSettings.EncryptionKey, GetSaltFromEncryptionKey(EncryptionSettings.EncryptionKey), Constants.Rfc2898Iterations);
			return Tuple.Create(deriveBytes.GetBytes(encryptionKeySize.Value), deriveBytes.GetBytes(encryptionIVSize.Value));
		}

		private static byte[] GetSaltFromEncryptionKey(byte[] key)
		{
			return LocalSHA1.Value.ComputeHash(key);
		}

		private static Stream WriteSalt(this Stream stream, string key)
		{
			byte[] keyBytes = Encoding.UTF8.GetBytes(key);
			stream.Write(keyBytes, 0, keyBytes.Length);
			return stream;
		}

		private static Stream ReadSalt(this Stream stream, string key)
		{
			try
			{
				byte[] keyBytes = Encoding.UTF8.GetBytes(key);
				byte[] readBytes = stream.ReadEntireBlock(keyBytes.Length);

				if (!readBytes.SequenceEqual(keyBytes))
				{
					throw new InvalidDataException("The encrypted stream's salt was different than the expected salt.");
				}
				return stream;

			}
			catch (Exception ex)
			{
				throw new IOException("Encrypted stream is not correctly salted with the document key.", ex);
			}
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
