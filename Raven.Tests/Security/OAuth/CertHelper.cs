using System;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;

namespace Raven.Tests.Security.OAuth
{
    public class CertHelper
    {
        public static string Sign(string text, string certPath)
        {
            var cert = new X509Certificate2(certPath, "Password123");
            var csp = (RSACryptoServiceProvider)cert.PrivateKey;
            
            var data = new UnicodeEncoding().GetBytes(text);
            var hash = new SHA1Managed().ComputeHash(data);

            return Convert.ToBase64String(csp.SignHash(hash, CryptoConfig.MapNameToOID("SHA1")));
        }

        public static bool Verify(string text, byte[] signature, string certPath)
        {
            var cert = new X509Certificate2(certPath);
            var csp = (RSACryptoServiceProvider)cert.PublicKey.Key;

            var data = new UnicodeEncoding().GetBytes(text);
            var hash = new SHA1Managed().ComputeHash(data);

            return csp.VerifyHash(hash, CryptoConfig.MapNameToOID("SHA1"), signature);
        }
    }
}