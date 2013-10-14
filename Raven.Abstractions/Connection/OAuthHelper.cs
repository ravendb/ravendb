using System;
using System.Collections.Generic;
using System.Linq;
#if NETFX_CORE
using Windows.Security.Cryptography.Core;
using Windows.Security.Cryptography;
using Windows.Storage.Streams;
#else
using System.Security.Cryptography;
#endif
using System.Text;
using System.Threading;
using Raven.Abstractions.Extensions;

namespace Raven.Abstractions.Connection
{
	using Raven.Abstractions.Util.Encryptors;

	public static class OAuthHelper
	{
		public static class Keys
		{
			public const string EncryptedData = "data";
			public const string APIKeyName = "api key name";
			public const string Challenge = "challenge";
			public const string Response = "response";

			public const string RSAExponent = "exponent";
			public const string RSAModulus = "modulus";

			public const string ChallengeTimestamp = "pepper";
			public const string ChallengeSalt = "salt";
			public const int ChallengeSaltLength = 64;

			public const string ResponseFormat = "{0};{1}";
			public const string WWWAuthenticateHeaderKey = "Raven ";
		}

#if NETFX_CORE
		[ThreadStatic]
		private static KeyDerivationAlgorithmProvider sha1;
#else
		[ThreadStatic]
		private static IHashEncryptor sha1;
#endif

		/**** Cryptography *****/

		public static string Hash(string data)
		{
			var bytes = Encoding.UTF8.GetBytes(data);
#if NETFX_CORE
			/*if (sha1 == null)
				sha1 = KeyDerivationAlgorithmProvider.OpenAlgorithm(KeyDerivationAlgorithmNames.Pbkdf2Sha1);*/
			throw new NotImplementedException("WinRT...");
#else
			if (sha1 == null)
				sha1 = Encryptor.Current.CreateHash();
			var hash = sha1.Compute20(bytes);
			return BytesToString(hash);
#endif
		}

		public static string EncryptAsymmetric(byte[] exponent, byte[] modulus, string data)
		{
			var bytes = Encoding.UTF8.GetBytes(data);
			var results = new List<byte>();

#if NETFX_CORE
			throw new NotImplementedException("WinRT...");
#else
			using (var aesKeyGen = Encryptor.Current.CreateSymmetrical(keySize: 256))
			{
				aesKeyGen.GenerateKey();
				aesKeyGen.GenerateIV();

				results.AddRange(AddEncryptedKeyAndIv(exponent, modulus, aesKeyGen.Key, aesKeyGen.IV));

				using (var encryptor = aesKeyGen.CreateEncryptor())
				{
					var encryptedBytes = encryptor.TransformEntireBlock(bytes);
					results.AddRange(encryptedBytes);
				}
			}
			return BytesToString(results.ToArray());
#endif
		}

#if SILVERLIGHT
		private static byte[] AddEncryptedKeyAndIv(byte[] exponent, byte[] modulus, byte[] key, byte[] iv)
		{
			// http://msdn.microsoft.com/en-us/library/cc265159(v=vs.95).aspx
			// http://msdn.microsoft.com/en-us/library/system.security.cryptography.rsacryptoserviceprovider(v=vs.95).aspx
			return null;
		}
#elif NETFX_CORE
		
#else
		private static byte[] AddEncryptedKeyAndIv(byte[] exponent, byte[] modulus, byte[] key, byte[] iv)
		{
			using (var rsa = Encryptor.Current.CreateAsymmetrical(exponent, modulus))
			{
				return rsa.Encrypt(key.Concat(iv).ToArray(), true);
			}
		}
#endif

		/**** On the wire *****/

		public static Dictionary<string, string> ParseDictionary(string data)
		{
			return data.Split(',')
				.Select(item =>
				{
					var items = item.Split(new[] { '=' }, StringSplitOptions.None);
					if (items.Length > 2)
					{
						return new[] { items[0], string.Join("=", items.Skip(1)) };
					}
					return items;
				})
				.ToDictionary(
					item => (item.First()).Trim(),
					item => (item.Skip(1).FirstOrDefault() ?? "").Trim()
				);
		}

		public static string DictionaryToString(Dictionary<string, string> data)
		{
			return string.Join(",", data.Select(item => item.Key + "=" + item.Value));
		}

		public static byte[] ParseBytes(string data)
		{
			if (data == null)
				return null;
			return Convert.FromBase64String(data);
		}

		public static string BytesToString(byte[] data)
		{
			if (data == null)
				return null;
			return Convert.ToBase64String(data);
		}
	}
}
