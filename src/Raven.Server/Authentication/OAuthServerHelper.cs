using System;
using System.Globalization;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Util.Encryptors;
using Raven.Server.Config;
using Raven.Server.ServerWide;


namespace Raven.Server.Authentication
{
    internal static class OAuthServerHelper
    {
        private const int RsaKeySize = 2048;

        private static readonly RandomNumberGenerator rng = RandomNumberGenerator.Create();
        private static readonly IAsymmetricalEncryptor rsa;
        private static readonly ISymmetricalEncryptor aes;

        private static readonly string rsaExponent;
        private static readonly string rsaModulus;

        static OAuthServerHelper()
        {
            RSAParameters privateRsaParameters;
            RSAParameters publicRsaParameters;
            using (var rsaKeyGen = Encryptor.Current.CreateAsymmetrical(RsaKeySize))
            {
                privateRsaParameters = rsaKeyGen.ExportParameters(true);
                publicRsaParameters = rsaKeyGen.ExportParameters(false);
            }

            Tuple<byte[], byte[]> aesKeyAndIV;
            using (var aesKeyGen = Encryptor.Current.CreateSymmetrical())
            {
                aesKeyAndIV = Tuple.Create(aesKeyGen.Key, aesKeyGen.IV);
            }

            rsa = Encryptor.Current.CreateAsymmetrical();
            rsa.ImportParameters(privateRsaParameters);

            aes = Encryptor.Current.CreateSymmetrical();
            aes.Key = aesKeyAndIV.Item1;
            aes.IV = aesKeyAndIV.Item2;

            rsaExponent = OAuthHelper.BytesToString(publicRsaParameters.Exponent);
            rsaModulus = OAuthHelper.BytesToString(publicRsaParameters.Modulus);
        }

        public static string RSAExponent
        {
            get { return rsaExponent; }
        }

        public static string RSAModulus
        {
            get { return rsaModulus; }
        }

        public static byte[] RandomBytes(int count)
        {
            var result = new byte[count];
            rng.GetBytes(result);
            return result;
        }

        public static string EncryptSymmetric(string data)
        {
            var bytes = Encoding.UTF8.GetBytes(data);
            using (var encryptor = aes.CreateEncryptor())
            {
                var result = encryptor.TransformFinalBlock(bytes, 0, bytes.Length);
                return OAuthHelper.BytesToString(result);
            }
        }

        public static string DecryptSymmetric(string data)
        {
            var bytes = OAuthHelper.ParseBytes(data);
            using (var decryptor = aes.CreateDecryptor())
            {
                var result = decryptor.TransformFinalBlock(bytes, 0, bytes.Length);
                return Encoding.UTF8.GetString(result);
            }
        }

        public static string DecryptAsymmetric(string data)
        {
            var bytes = OAuthHelper.ParseBytes(data);

            var encryptedKeyAndIv = bytes.Take(256).ToArray();
            var decrypted = rsa.Decrypt(encryptedKeyAndIv);

            var key = decrypted.Take(32).ToArray();
            var iv = decrypted.Skip(32).ToArray();

            using (var decryptor = aes.CreateDecryptor(key, iv))
            {
                var arr = bytes.Skip(256).ToArray();
                var block = decryptor.TransformFinalBlock(arr, 0, arr.Length);
                return Encoding.UTF8.GetString(block);
            }

        }

        public static DateTime? ParseDateTime(string data)
        {
            DateTime result;
            if (DateTime.TryParseExact(data, "O", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out result))
                return result;
            else
                return null;
        }

        public static string DateTimeToString(DateTime data)
        {
            return data.ToString("O", CultureInfo.InvariantCulture);
        }

        public static RSAParameters GetOAuthParameters(RavenConfiguration configuration)
        {
            var modulus = configuration.Server.OAuthTokenCertificateModulus;
            var exponent = configuration.Server.OAuthTokenCertificateExponent;

            if (string.IsNullOrEmpty(modulus) || string.IsNullOrEmpty(exponent))
                return DefaultOauthKey.Value; // ensure we only create this once per process

            return new RSAParameters
            {
                Exponent = Convert.FromBase64String(exponent),
                Modulus = Convert.FromBase64String(modulus)
            };
        }

        private static readonly Lazy<RSAParameters> DefaultOauthKey = new Lazy<RSAParameters>(() =>
        {
            using (var rsa = Encryptor.Current.CreateAsymmetrical())
            {
                return rsa.ExportParameters(true);
            }
        });
    }
}
