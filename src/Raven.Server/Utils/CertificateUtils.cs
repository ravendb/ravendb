using System;
using System.IO;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using JetBrains.Annotations;
using Org.BouncyCastle.Asn1.X509;
using Org.BouncyCastle.Crypto;
using Org.BouncyCastle.Crypto.Generators;
using Org.BouncyCastle.Crypto.Operators;
using Org.BouncyCastle.Crypto.Prng;
using Org.BouncyCastle.Pkcs;
using Org.BouncyCastle.Security;
using Org.BouncyCastle.Utilities;
using Org.BouncyCastle.X509;
using BigInteger = Org.BouncyCastle.Math.BigInteger;
using X509Certificate = Org.BouncyCastle.X509.X509Certificate;

namespace Raven.Server.Utils
{
    internal class CertificateUtils
    {
        public static X509Certificate2 CreateSelfSignedCertificate(string subjectName, string issuerName)
        {
            CreateCertificateAuthorityCertificate(subjectName, out AsymmetricKeyParameter caPrivateKey);
            return CreateSelfSignedCertificateBasedOnPrivateKey(subjectName, issuerName, caPrivateKey, false, 1);
        }

        public static X509Certificate2 CreateSelfSignedClientCertificate(string subjectName, RavenServer.CertificateHolder certificateHolder)
        {
            return CreateSelfSignedCertificateBasedOnPrivateKey(
                subjectName,
                certificateHolder.Certificate.Subject,
                certificateHolder.PrivateKey.Key,
                true,
                5);
        }

        public static X509Certificate2 CreateSelfSignedExpiredClientCertificate(string subjectName, RavenServer.CertificateHolder certificateHolder)
        {
            return CreateSelfSignedCertificateBasedOnPrivateKey(
                subjectName,
                certificateHolder.Certificate.Subject,
                certificateHolder.PrivateKey.Key,
                true,
                -1);
        }

        private static X509Certificate2 CreateSelfSignedCertificateBasedOnPrivateKey(string subjectName, string issuerName,
            AsymmetricKeyParameter issuerPrivKey, bool isClientCertificate, int yearsUntilExpiration)
        {
            const int keyStrength = 2048;

            // Generating Random Numbers
            var random = GetSeededSecureRandom();
            ISignatureFactory signatureFactory = new Asn1SignatureFactory("SHA512WITHRSA", issuerPrivKey, random);

            // The Certificate Generator
            X509V3CertificateGenerator certificateGenerator = new X509V3CertificateGenerator();

            var extendedKeyUsage = isClientCertificate
                ? new ExtendedKeyUsage(KeyPurposeID.IdKPClientAuth)
                : new ExtendedKeyUsage(KeyPurposeID.IdKPServerAuth, KeyPurposeID.IdKPClientAuth);

            certificateGenerator.AddExtension(X509Extensions.ExtendedKeyUsage.Id, true, extendedKeyUsage);

            // Serial Number
            BigInteger serialNumber = BigIntegers.CreateRandomInRange(BigInteger.One, BigInteger.ValueOf(Int64.MaxValue), random);
            certificateGenerator.SetSerialNumber(serialNumber);

            // Issuer and Subject Name
            X509Name subjectDN = new X509Name("CN=" + subjectName);
            X509Name issuerDN = new X509Name("CN=" + issuerName);
            certificateGenerator.SetIssuerDN(issuerDN);
            certificateGenerator.SetSubjectDN(subjectDN);

            // Valid For
            DateTime notBefore = DateTime.UtcNow.Date.AddDays(-7);
            DateTime notAfter = notBefore.AddYears(yearsUntilExpiration);
            certificateGenerator.SetNotBefore(notBefore);
            certificateGenerator.SetNotAfter(notAfter);

            // Subject Public Key
            var keyGenerationParameters = new KeyGenerationParameters(random, keyStrength);
            var keyPairGenerator = new RsaKeyPairGenerator();
            keyPairGenerator.Init(keyGenerationParameters);
            var subjectKeyPair = keyPairGenerator.GenerateKeyPair();

            certificateGenerator.SetPublicKey(subjectKeyPair.Public);

            X509Certificate certificate = certificateGenerator.Generate(signatureFactory);
            var store = new Pkcs12Store();
            string friendlyName = certificate.SubjectDN.ToString();
            var certificateEntry = new X509CertificateEntry(certificate);
            store.SetCertificateEntry(friendlyName, certificateEntry);
            store.SetKeyEntry(friendlyName, new AsymmetricKeyEntry(subjectKeyPair.Private), new[] { certificateEntry });
            var stream = new MemoryStream();
            store.Save(stream, new char[0], random);
            var convertedCertificate =
                new X509Certificate2(
                    stream.ToArray(), (string)null,
                    X509KeyStorageFlags.PersistKeySet | X509KeyStorageFlags.Exportable);
            stream.Position = 0;

            return convertedCertificate;
        }

        private static void CreateCertificateAuthorityCertificate(string subjectName, [CanBeNull] out AsymmetricKeyParameter caPrivateKey)
        {
            const int keyStrength = 2048;

            var random = GetSeededSecureRandom();

            // The Certificate Generator
            X509V3CertificateGenerator certificateGenerator = new X509V3CertificateGenerator();

            // Serial Number
            BigInteger serialNumber = BigIntegers.CreateRandomInRange(BigInteger.One, BigInteger.ValueOf(Int64.MaxValue), random);
            certificateGenerator.SetSerialNumber(serialNumber);

            // Issuer and Subject Name
            X509Name subjectDN = new X509Name("CN=" + subjectName);
            X509Name issuerDN = subjectDN;
            certificateGenerator.SetIssuerDN(issuerDN);
            certificateGenerator.SetSubjectDN(subjectDN);

            // Valid For
            DateTime notBefore = DateTime.UtcNow.Date.AddDays(-7);
            DateTime notAfter = notBefore.AddYears(2);

            certificateGenerator.SetNotBefore(notBefore);
            certificateGenerator.SetNotAfter(notAfter);

            // Subject Public Key
            var keyGenerationParameters = new KeyGenerationParameters(random, keyStrength);
            var keyPairGenerator = new RsaKeyPairGenerator();
            keyPairGenerator.Init(keyGenerationParameters);
            var subjectKeyPair = keyPairGenerator.GenerateKeyPair();

            certificateGenerator.SetPublicKey(subjectKeyPair.Public);

            // Generating the Certificate
            var issuerKeyPair = subjectKeyPair;
            ISignatureFactory signatureFactory = new Asn1SignatureFactory("SHA512WITHRSA", issuerKeyPair.Private, random);

            // selfsign certificate
            certificateGenerator.Generate(signatureFactory);

            caPrivateKey = issuerKeyPair.Private;
        }

        private static SecureRandom GetSeededSecureRandom()
        {
            var buffer = new byte[32];
            using (var cryptoRandom = RandomNumberGenerator.Create())
            {
                cryptoRandom.GetBytes(buffer);
            }
            var randomGenerator = new VmpcRandomGenerator();
            randomGenerator.AddSeedMaterial(buffer);
            SecureRandom random = new SecureRandom(randomGenerator);
            return random;
        }
    }
}
