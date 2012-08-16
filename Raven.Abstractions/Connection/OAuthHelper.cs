using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Extensions.Internal;

namespace Raven.Abstractions.Connection
{
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

		private static readonly ThreadLocal<SHA1> sha1 = new ThreadLocal<SHA1>(() => SHA1.Create());


		/**** Cryptography *****/

		public static string Hash(string data)
		{
			var bytes = Encoding.UTF8.GetBytes(data);
			var hash = sha1.Value.ComputeHash(bytes);
			return BytesToString(hash);
		}

		public static string EncryptAssymetric(RSAParameters parameters, string data)
		{
			var bytes = Encoding.UTF8.GetBytes(data);

			using (var rsa = new RSACryptoServiceProvider())
			{
				rsa.ImportParameters(parameters);
				var encrypted = rsa.Encrypt(bytes, true);
				return BytesToString(encrypted);
			}
		}

		/**** On the wire *****/

		public static Dictionary<string, string> ParseDictionary(string data)
		{
			return data.Split(',')
				.Select(item => item.Split(new[] { '=' }, 2))
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
	}
}
