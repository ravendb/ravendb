using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using Raven.Abstractions.Extensions;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Abstractions.Connection
{
	public static class OAuthHelper
	{
		public static class Keys
		{
			public const string RSAPublicKey = "rsa key";
			public const string EncryptedData = "data";
			public const string APIKeyName = "api key name";
			public const string Challenge = "challenge";
			public const string Response = "response";

			public const string ChallengeTimestamp = "timestamp";
			public const string ChallengeSalt = "salt";
			public const int ChallengeSaltLength = 64;

			public const string ResponseFormat = "{0};{1}";
			public const string WWWAuthenticateHeaderKey = "Raven ";
		}

		private static readonly RandomNumberGenerator rng = RandomNumberGenerator.Create();
		private static readonly SHA1 sha1 = SHA1.Create();
		private static RSA rsa = null;
		private static Aes aes = null;

		private static string rsaPublicKey = null;

		public static void InitializeServerKeys()
		{
			rsa = RSA.Create();
			aes = Aes.Create();

			rsaPublicKey = SerializeRsaParameters(rsa.ExportParameters(false));
		}

		public static string RSAPublicKey
		{
			get { return rsaPublicKey; }
		}

		/**** Cryptography *****/

		public static byte[] RandomBytes(int count)
		{
			var result = new byte[count];
			rng.GetBytes(result);
			return result;
		}

		public static string Hash(string data)
		{
			var bytes = Encoding.UTF8.GetBytes(data);
			var hash = sha1.ComputeHash(bytes);
			return BytesToString(hash);
		}

		public static string EncryptSymmetric(string data)
		{
			var bytes = Encoding.UTF8.GetBytes(data);
			using (var encryptor = aes.CreateEncryptor())
			{
				var result = encryptor.TransformEntireBlock(bytes);
				return BytesToString(result);
			}
		}

		public static string DecryptSymmetric(string data)
		{
			var bytes = ParseBytes(data);
			using (var decryptor = aes.CreateDecryptor())
			{
				var result = decryptor.TransformEntireBlock(bytes);
				return Encoding.UTF8.GetString(result);
			}
		}

		public static string EncryptAssymetric(RSAParameters parameters, string data)
		{
			var bytes = Encoding.UTF8.GetBytes(data);
			using (var rsa = new RSACryptoServiceProvider())
			{
				rsa.ImportParameters(parameters);
				var result = rsa.EncryptValue(bytes);
				return BytesToString(result);
			}
		}

		public static string DecryptAsymmetric(string data)
		{
			var bytes = ParseBytes(data);
			var result = rsa.DecryptValue(bytes);
			return Encoding.UTF8.GetString(result);
		}

		/**** On the wire *****/

		public static Dictionary<string, string> ParseDictionary(string data)
		{
			return data.Split(',')
				.Select(item => item.Split(new[] { '=' }, 2))
				.ToDictionary(
					item => (item.First() ?? "").Trim(),
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

			try
			{
				return Convert.FromBase64String(data);
			}
			catch (FormatException)
			{
				return null;
			}
		}

		public static string BytesToString(byte[] data)
		{
			if (data == null)
				return null;
			return Convert.ToBase64String(data);
		}

		public static DateTime? ParseDateTime(string data)
		{
			DateTime result;
			if (DateTime.TryParseExact(data, "O", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out result))
				return result;
			else
				return null;
		}

		public static string DateTimeToString(DateTime data)
		{
			return data.ToString("O", CultureInfo.InvariantCulture);
		}

		public static string SerializeRsaParameters(RSAParameters data)
		{
			using (var writer = new StringWriter())
			{
				new JsonSerializer().Serialize(writer, data);
				return writer.ToString();
			}
		}

		public static RSAParameters DeserializeRSAParameters(string data)
		{
			using (var reader = new StringReader(data))
			{
				return new JsonSerializer().Deserialize<RSAParameters>(reader);
			}
		}
	}
}
