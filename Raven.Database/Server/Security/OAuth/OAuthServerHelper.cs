using System;
using System.Globalization;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Extensions;

namespace Raven.Database.Server.Security.OAuth
{
    using Raven.Abstractions.Util.Encryptors;

    internal static class OAuthServerHelper
    {
        private const int RsaKeySize = 2048;

        private static readonly ThreadLocal<RNGCryptoServiceProvider> rng = new ThreadLocal<RNGCryptoServiceProvider>(() => new RNGCryptoServiceProvider());
        private static readonly ThreadLocal<IAsymmetricalEncryptor> rsa;
        private static readonly ThreadLocal<ISymmetricalEncryptor> aes;

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

            rsa = new ThreadLocal<IAsymmetricalEncryptor>(() =>
            {
                var result = Encryptor.Current.CreateAsymmetrical();
                result.ImportParameters(privateRsaParameters);
                return result;
            });

            aes = new ThreadLocal<ISymmetricalEncryptor>(() =>
            {
                var result = Encryptor.Current.CreateSymmetrical();
                result.Key = aesKeyAndIV.Item1;
                result.IV = aesKeyAndIV.Item2;
                return result;
            });

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
            rng.Value.GetBytes(result);
            return result;
        }

        public static string EncryptSymmetric(string data)
        {
            var bytes = Encoding.UTF8.GetBytes(data);
            using (var encryptor = aes.Value.CreateEncryptor())
            {
                var result = encryptor.TransformEntireBlock(bytes);
                return OAuthHelper.BytesToString(result);
            }
        }

        public static string DecryptSymmetric(string data)
        {
            var bytes = OAuthHelper.ParseBytes(data);
            using (var decryptor = aes.Value.CreateDecryptor())
            {
                var result = decryptor.TransformEntireBlock(bytes);
                return Encoding.UTF8.GetString(result);
            }
        }

        public static string DecryptAsymmetric(string data)
        {
            var bytes = OAuthHelper.ParseBytes(data);

            var encryptedKeyAndIv = bytes.Take(256).ToArray();
            var decrypted = rsa.Value.Decrypt(encryptedKeyAndIv, true);

            var key = decrypted.Take(32).ToArray();
            var iv = decrypted.Skip(32).ToArray();

            using (var decryptor = aes.Value.CreateDecryptor(key, iv))
            {
                var block = decryptor.TransformEntireBlock(bytes.Skip(256).ToArray());
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
    }
}
