using System;
using System.Globalization;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using Lucene.Net.Search;
using Raven.Abstractions.Data;
using Raven.Imports.Newtonsoft.Json;
using Raven.Abstractions;
using Raven.Json.Linq;
using Raven.Abstractions.Util.Encryptors;

namespace Raven.Server.Authentication
{
    public class AccessToken
    {
        public string Body { get; set; }
        public string Signature { get; set; }

        public static AccessToken Create(RSAParameters parameters, AccessTokenBody tokenBody)
        {
            tokenBody.Issued = (SystemTime.UtcNow - DateTime.MinValue).TotalMilliseconds;

            var body = RavenJObject.FromObject(tokenBody)
                .ToString(Formatting.None);

            var signature = Sign(body, parameters);

            return new AccessToken {Body = body, Signature = signature};
        }

        public static string Sign(string body, RSAParameters parameters)
        {
            var data = Encoding.Unicode.GetBytes(body);
            using (var rsa = Encryptor.Current.CreateAsymmetrical())
            {
                var hash = Encryptor.Current.Hash.ComputeForOAuth(data);

                rsa.ImportParameters(parameters);

                var sig = rsa.SignHash(hash);

                return Convert.ToBase64String(sig);
            }
        }

        public string Serialize()
        {
            return RavenJObject.FromObject(this).ToString(Formatting.None);
        }

        internal static byte[] HexToByteArray(string hexString)
        {
            byte[] bytes = new byte[hexString.Length / 2];

            for (int i = 0; i < hexString.Length; i += 2)
            {
                string s = hexString.Substring(i, 2);
                bytes[i / 2] = byte.Parse(s, NumberStyles.HexNumber, null);
            }

            return bytes;
        }

        private bool MatchesSignature(RSAParameters parameters)
        {
            var signatureData = Convert.FromBase64String(Signature);

            using (var rsa = Encryptor.Current.CreateAsymmetrical())
            {
                rsa.ImportParameters(parameters);

                var bodyData = Encoding.Unicode.GetBytes(Body);

                var hash = Encryptor.Current.Hash.ComputeForOAuth(bodyData);

                return rsa.VerifyHash(hash, signatureData);
            }
        }

        private static bool TryParse(string token, out AccessToken accessToken)
        {
            try
            {
                accessToken = JsonConvert.DeserializeObject<AccessToken>(token);
                return true;
            }
            catch
            {
                accessToken = null;
                return false;
            }
        }

        public static bool TryParseBody(RSAParameters parameters, string token, out AccessTokenBody body)
        {
            // TODO (OAuth): implement and check if already known token in dictionary to avoid reparsing every time
            AccessToken accessToken;
            if (TryParse(token, out accessToken) == false)
            {
                body = null;
                return false;
            }

            if (accessToken.MatchesSignature(parameters) == false)
            {
                body = null;
                return false;
            }

            try
            {
                body = JsonConvert.DeserializeObject<AccessTokenBody>(accessToken.Body);
                return true;
            }
            catch
            {
                body = null;
                return false;
            }
        }
    }
}