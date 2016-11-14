using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.NewClient.Abstractions.Extensions;

namespace Raven.NewClient.Abstractions.Connection
{
    using Raven.NewClient.Abstractions.Util.Encryptors;

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

        [ThreadStatic]
        private static IHashEncryptor sha1;

        /**** Cryptography *****/

        public static string Hash(string data)
        {
            var bytes = Encoding.UTF8.GetBytes(data);

            if (sha1 == null)
                sha1 = Encryptor.Current.CreateHash();
            var hash = sha1.Compute20(bytes);
            return BytesToString(hash);
        }

        public static string EncryptAsymmetric(byte[] exponent, byte[] modulus, string data)
        {
            var bytes = Encoding.UTF8.GetBytes(data);
            var results = new List<byte>();

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
        }

        private static byte[] AddEncryptedKeyAndIv(byte[] exponent, byte[] modulus, byte[] key, byte[] iv)
        {
            using (var rsa = Encryptor.Current.CreateAsymmetrical(exponent, modulus))
            {
                return rsa.Encrypt(key.Concat(iv).ToArray());
            }
        }

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
