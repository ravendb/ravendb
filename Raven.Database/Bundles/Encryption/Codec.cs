using System;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using Raven.Abstractions.Extensions;
using Raven.Bundles.Encryption.Settings;
using Raven.Abstractions.Data;

namespace Raven.Bundles.Encryption
{
	public class Codec
	{
		private static readonly ThreadLocal<RNGCryptoServiceProvider> LocalRNG = new ThreadLocal<RNGCryptoServiceProvider>(() => new RNGCryptoServiceProvider());

		public readonly EncryptionSettings EncryptionSettings;
		private Tuple<byte[], byte[]> encryptionStartingKeyAndIV;
		private int? encryptionKeySize;
		private int? encryptionIVSize;

		public Codec(EncryptionSettings settings)
		{
			this.EncryptionSettings = settings;
		}

		public Stream Encode(string key, Stream dataStream)
		{
			SymmetricAlgorithm provider = null;
			ICryptoTransform encryptor = null;
			Stream stream = null;
			try
			{
				provider = GetCryptoProvider(null);
				encryptor = provider.CreateEncryptor();
				stream = new CryptoStream(dataStream, encryptor, CryptoStreamMode.Write);
				return stream.WriteSalt(key).DisposeTogetherWith(provider, encryptor);
			}
			catch
			{
				try
				{
					if (provider != null)
						provider.Dispose();
				}
				catch { }
				try
				{
					if (encryptor != null)
						encryptor.Dispose();
				}
				catch { }
				try
				{
					if (stream != null)
						stream.Dispose();
				}
				catch { }
				throw;
			}
		}

		public Stream Decode(string key, Stream dataStream)
		{
			SymmetricAlgorithm provider = null;
			ICryptoTransform decryptor = null;
			Stream stream = null;
			try
			{
				provider = GetCryptoProvider(null);
				decryptor = provider.CreateDecryptor();
				stream = new CryptoStream(dataStream, decryptor, CryptoStreamMode.Read);
				return stream.ReadSalt(key).DisposeTogetherWith(provider, decryptor);
			}
			catch
			{
				try
				{
					if (provider != null)
						provider.Dispose();
				}
				catch { }
				try
				{
					if (decryptor != null)
						decryptor.Dispose();
				}
				catch { }
				try
				{
					if (stream != null)
						stream.Dispose();
				}
				catch { }
				throw;
			}
		}

		public EncodedBlock EncodeBlock(string key, byte[] data)
		{
			byte[] iv;
			var transform = GetCryptoProviderWithRandomIV(out iv).CreateEncryptor();

			return new EncodedBlock(iv, transform.TransformEntireBlock(data));
		}

		public byte[] DecodeBlock(string key, EncodedBlock block)
		{
			var transform = GetCryptoProvider(block.IV).CreateDecryptor();

			return transform.TransformEntireBlock(block.Data);
		}

		private int GetIVLength()
		{
			if (encryptionIVSize == null)
			{
				// This will force detection of the iv size
				GetCryptoProvider(null);
			}

			return encryptionIVSize.Value;
		}

		private SymmetricAlgorithm GetCryptoProvider(byte[] iv)
		{
			var result = EncryptionSettings.GenerateAlgorithm();
			encryptionStartingKeyAndIV = encryptionStartingKeyAndIV ?? GetStartingKeyAndIVForEncryption(result);

			if (iv != null && iv.Length != encryptionIVSize)
				throw new ArgumentException("GetCryptoProvider: IV has wrong length. Given length: " + iv.Length + ", expected length: " + encryptionIVSize);

			result.Key = encryptionStartingKeyAndIV.Item1;
			result.IV = iv ?? encryptionStartingKeyAndIV.Item2;
			return result;
		}

		private SymmetricAlgorithm GetCryptoProviderWithRandomIV(out byte[] iv)
		{
			iv = new byte[GetIVLength()];
			LocalRNG.Value.GetBytes(iv);

			return GetCryptoProvider(iv);
		}

		private Tuple<byte[], byte[]> GetStartingKeyAndIVForEncryption(SymmetricAlgorithm algorithm)
		{
			int bits = algorithm.ValidKeySize(Constants.DefaultKeySizeToUseInActualEncryptionInBits) ? 
				Constants.DefaultKeySizeToUseInActualEncryptionInBits : 
				algorithm.LegalKeySizes[0].MaxSize;

			encryptionKeySize = bits / 8;
			encryptionIVSize = algorithm.IV.Length;

			var deriveBytes = new Rfc2898DeriveBytes(EncryptionSettings.EncryptionKey, GetSaltFromEncryptionKey(EncryptionSettings.EncryptionKey), Constants.Rfc2898Iterations);
			return Tuple.Create(deriveBytes.GetBytes(encryptionKeySize.Value), deriveBytes.GetBytes(encryptionIVSize.Value));
		}

		private static byte[] GetSaltFromEncryptionKey(byte[] key)
		{
			return SHA1.Create().ComputeHash(key);
		}

		public struct EncodedBlock
		{
			public EncodedBlock(byte[] iv, byte[] data)
			{
				IV = iv;
				Data = data;
			}

			public readonly byte[] IV;
			public readonly byte[] Data;
		}
	}

	internal static class CodecSaltExtensions
	{
		public static Stream WriteSalt(this Stream stream, string key)
		{
			byte[] keyBytes = Encoding.UTF8.GetBytes(key);
			stream.Write(keyBytes, 0, keyBytes.Length);
			return stream;
		}

		public static Stream ReadSalt(this Stream stream, string key)
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
	}
}
