using System;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Org.BouncyCastle.Asn1.X509;
using Org.BouncyCastle.Crypto;
using Org.BouncyCastle.Crypto.Generators;
using Org.BouncyCastle.Crypto.Operators;
using Org.BouncyCastle.Crypto.Prng;
using Org.BouncyCastle.Pkcs;
using Org.BouncyCastle.Security;
using Org.BouncyCastle.X509;
using Org.BouncyCastle.X509.Extension;
using Sparrow;
using Sparrow.Platform;
using BigInteger = Org.BouncyCastle.Math.BigInteger;
using X509Certificate = Org.BouncyCastle.X509.X509Certificate;

namespace Raven.Server.Utils
{
    internal class CertificateUtils
    {
        private const int BitsPerByte = 8;

        public static byte[] CreateSelfSignedTestCertificate(string commonNameValue, string issuerName, StringBuilder log = null)
        {
            // Note this is for tests only!
            CreateCertificateAuthorityCertificate(commonNameValue + " CA", out var ca, out var caSubjectName, log);
            CreateSelfSignedCertificateBasedOnPrivateKey(commonNameValue, caSubjectName, ca, false, false, DateTime.UtcNow.Date.AddMonths(3), out var certBytes, log: log);
            var selfSignedCertificateBasedOnPrivateKey = new X509Certificate2(certBytes, (string)null, X509KeyStorageFlags.MachineKeySet);
            selfSignedCertificateBasedOnPrivateKey.Verify();

            // We had a problem where we didn't cleanup the user store in Linux (~/.dotnet/corefx/cryptography/x509stores/ca)
            // and it exploded with thousands of certificates. This caused ssl handshakes to fail on that machine, because it would timeout when
            // trying to match one of these certs to validate the chain
            RemoveOldTestCertificatesFromOsStore(commonNameValue);
            return certBytes;
        }

        public static (byte[], byte[]) CreateTwoTestCertificatesWithSameKey(string commonNameValue, string issuerName, StringBuilder log = null)
        {
            CreateCertificateAuthorityCertificate(commonNameValue + " CA", out var ca, out var caSubjectName, log);

            var existingKeyPair = GetRsaKey();

            CreateSelfSignedCertificateBasedOnPrivateKey(commonNameValue, caSubjectName, ca, false, false, DateTime.UtcNow.Date.AddMonths(3), out var certBytes1, existingKeyPair, log);
            var selfSignedCertificateBasedOnPrivateKey1 = new X509Certificate2(certBytes1, (string)null, X509KeyStorageFlags.MachineKeySet);
            selfSignedCertificateBasedOnPrivateKey1.Verify();

            CreateSelfSignedCertificateBasedOnPrivateKey(commonNameValue, caSubjectName, ca, false, false, DateTime.UtcNow.Date.AddMonths(3), out var certBytes2, existingKeyPair, log);
            var selfSignedCertificateBasedOnPrivateKey2 = new X509Certificate2(certBytes2, (string)null, X509KeyStorageFlags.MachineKeySet);
            selfSignedCertificateBasedOnPrivateKey2.Verify();

            RemoveOldTestCertificatesFromOsStore(commonNameValue);
            return (certBytes1, certBytes2);
        }

        private static void RemoveOldTestCertificatesFromOsStore(string commonNameValue)
        {
            // We have the same logic in AddCertificateChainToTheUserCertificateAuthorityStoreAndCleanExpiredCerts when the server starts
            // and when we renew a certificate. There we delete certificates only if expired but here in the tests we delete them all and keep
            // just the ones from the last couple days
            var storeName = PlatformDetails.RunningOnMacOsx ? StoreName.My : StoreName.CertificateAuthority;
            using (var userIntermediateStore = new X509Store(storeName, StoreLocation.CurrentUser, OpenFlags.ReadWrite))
            {
                var twoDaysAgo = DateTime.Today.AddDays(-2);
                var existingCerts = userIntermediateStore.Certificates.Find(X509FindType.FindBySubjectName, commonNameValue, false);
                foreach (var c in existingCerts)
                {
                    if (c.NotBefore.ToUniversalTime() > twoDaysAgo)
                        continue;

                    var chain = new X509Chain();
                    chain.ChainPolicy.DisableCertificateDownloads = true;

                    chain.Build(c);

                    foreach (var element in chain.ChainElements)
                    {
                        if (element.Certificate.NotBefore.ToUniversalTime() > twoDaysAgo)
                            continue;
                        try
                        {
                            userIntermediateStore.Remove(element.Certificate);
                        }
                        catch (CryptographicException)
                        {
                            // Access denied?
                        }
                    }
                }
            }
        }

        public static X509Certificate2 CreateSelfSignedClientCertificate(string commonNameValue, RavenServer.CertificateHolder certificateHolder, out byte[] certBytes, DateTime notAfter)
        {
            var serverCertBytes = certificateHolder.Certificate.Export(X509ContentType.Cert);
            var readCertificate = new X509CertificateParser().ReadCertificate(serverCertBytes);
            CreateSelfSignedCertificateBasedOnPrivateKey(
                commonNameValue,
                readCertificate.SubjectDN,
                (certificateHolder.PrivateKey.Key, readCertificate.GetPublicKey()),
                true,
                false,
                notAfter,
                out certBytes);

            ValidateNoPrivateKeyInServerCert(serverCertBytes);

            Pkcs12Store store = new Pkcs12StoreBuilder().Build();
            var serverCert = DotNetUtilities.FromX509Certificate(certificateHolder.Certificate);

            store.Load(new MemoryStream(certBytes), Array.Empty<char>());
            store.SetCertificateEntry(serverCert.SubjectDN.ToString(), new X509CertificateEntry(serverCert));

            var memoryStream = new MemoryStream();
            store.Save(memoryStream, Array.Empty<char>(), GetSeededSecureRandom());
            certBytes = memoryStream.ToArray();

            var cert = new X509Certificate2(certBytes, (string)null, X509KeyStorageFlags.PersistKeySet | X509KeyStorageFlags.Exportable | X509KeyStorageFlags.MachineKeySet);
            return cert;
        }

        private static void ValidateNoPrivateKeyInServerCert(byte[] serverCertBytes)
        {
            var collection = new X509Certificate2Collection();
            // without the server private key here
            collection.Import(serverCertBytes, (string)null, X509KeyStorageFlags.MachineKeySet);

            if (new X509Certificate2Collection(collection).OfType<X509Certificate2>().FirstOrDefault(x => x.HasPrivateKey) != null)
                throw new InvalidOperationException("After export of CERT, still have private key from signer in certificate, should NEVER happen");
        }

        public static X509Certificate2 CreateSelfSignedExpiredClientCertificate(string commonNameValue, RavenServer.CertificateHolder certificateHolder)
        {
            var readCertificate = new X509CertificateParser().ReadCertificate(certificateHolder.Certificate.Export(X509ContentType.Cert));

            CreateSelfSignedCertificateBasedOnPrivateKey(
                commonNameValue,
                readCertificate.SubjectDN,
                (certificateHolder.PrivateKey.Key, readCertificate.GetPublicKey()),
                true,
                false,
                DateTime.UtcNow.Date.AddYears(-1),
                out var certBytes);

            return new X509Certificate2(certBytes, (string)null, X509KeyStorageFlags.MachineKeySet);
        }

        public static void CreateSelfSignedCertificateBasedOnPrivateKey(string commonNameValue,
            X509Name issuer,
            (AsymmetricKeyParameter PrivateKey, AsymmetricKeyParameter PublicKey) issuerKeyPair,
            bool isClientCertificate,
            bool isCaCertificate,
            DateTime notAfter,
            out byte[] certBytes,
            AsymmetricCipherKeyPair subjectKeyPair = null,
            StringBuilder log = null)
        {
            log?.AppendLine("CreateSelfSignedCertificateBasedOnPrivateKey:");

            // Generating Random Numbers
            var random = GetSeededSecureRandom();
            ISignatureFactory signatureFactory = new Asn1SignatureFactory("SHA512WITHRSA", issuerKeyPair.PrivateKey, random);

            // The Certificate Generator
            X509V3CertificateGenerator certificateGenerator = new X509V3CertificateGenerator();
            var authorityKeyIdentifier = new AuthorityKeyIdentifierStructure(issuerKeyPair.PublicKey);
            certificateGenerator.AddExtension(X509Extensions.AuthorityKeyIdentifier.Id, false, authorityKeyIdentifier);

            if (isClientCertificate)
            {
                certificateGenerator.AddExtension(X509Extensions.ExtendedKeyUsage.Id, true, new ExtendedKeyUsage(KeyPurposeID.IdKPClientAuth));
            }
            else
            {
                certificateGenerator.AddExtension(X509Extensions.ExtendedKeyUsage.Id, true,
                    new ExtendedKeyUsage(KeyPurposeID.IdKPServerAuth, KeyPurposeID.IdKPClientAuth));
            }

            if (isCaCertificate)
            {
                certificateGenerator.AddExtension(X509Extensions.BasicConstraints.Id, true, new BasicConstraints(0));
                certificateGenerator.AddExtension(X509Extensions.KeyUsage.Id, false,
                    new X509KeyUsage(X509KeyUsage.KeyCertSign | X509KeyUsage.CrlSign));
            }

            // Serial Number
            var serialNumberBytes = new byte[20];
            random.NextBytes(serialNumberBytes);
            var serialNumber = new BigInteger(serialNumberBytes).Abs();
            certificateGenerator.SetSerialNumber(serialNumber);
            log?.AppendLine($"serialNumber = {serialNumber}");

            // Issuer and Subject Name

            X509Name subjectDN = new X509Name("CN=" + commonNameValue);
            certificateGenerator.SetIssuerDN(issuer);
            certificateGenerator.SetSubjectDN(subjectDN);
            log?.AppendLine($"issuerDN = {issuer}");
            log?.AppendLine($"subjectDN = {subjectDN}");

            // Valid For
            DateTime notBefore = DateTime.UtcNow.Date.AddDays(-7);

            certificateGenerator.SetNotBefore(notBefore);
            certificateGenerator.SetNotAfter(notAfter);
            log?.AppendLine($"notBefore = {notBefore}");
            log?.AppendLine($"notAfter = {notAfter}");

            if (subjectKeyPair == null)
                subjectKeyPair = GetRsaKey();

            certificateGenerator.SetPublicKey(subjectKeyPair.Public);

            X509Certificate certificate = certificateGenerator.Generate(signatureFactory);
            var store = new Pkcs12Store();
            string friendlyName = certificate.SubjectDN.ToString();
            var certificateEntry = new X509CertificateEntry(certificate);
            var keyEntry = new AsymmetricKeyEntry(subjectKeyPair.Private);

            log?.AppendLine($"certificateEntry.Certificate = {certificateEntry.Certificate}");

            store.SetCertificateEntry(friendlyName, certificateEntry);
            store.SetKeyEntry(friendlyName, keyEntry, new[] { certificateEntry });
            var stream = new MemoryStream();
            store.Save(stream, new char[0], random);

            certBytes = stream.ToArray();

            log?.AppendLine($"certBytes.Length = {certBytes.Length}");
            log?.AppendLine($"cert in base64 = {Convert.ToBase64String(certBytes)}");
        }

        public static void CreateCertificateAuthorityCertificate(string commonNameValue,
            out (AsymmetricKeyParameter PrivateKey, AsymmetricKeyParameter PublicKey) ca,
            out X509Name name, StringBuilder log = null)
        {
            log?.AppendLine("CreateCertificateAuthorityCertificate:");
            var random = GetSeededSecureRandom();

            // The Certificate Generator
            X509V3CertificateGenerator certificateGenerator = new X509V3CertificateGenerator();

            // Serial Number
            BigInteger serialNumber = new BigInteger(20 * BitsPerByte, random);
            log?.AppendLine($"serialNumber = {serialNumber}");
            certificateGenerator.SetSerialNumber(serialNumber);

            // Issuer and Subject Name
            X509Name subjectDN = new X509Name("CN=" + commonNameValue);
            X509Name issuerDN = subjectDN;
            certificateGenerator.SetIssuerDN(issuerDN);
            certificateGenerator.SetSubjectDN(subjectDN);
            log?.AppendLine($"issuerDN = {issuerDN}");
            log?.AppendLine($"subjectDN = {subjectDN}");

            // Valid For
            DateTime notBefore = DateTime.UtcNow.Date.AddDays(-7);
            DateTime notAfter = notBefore.AddYears(2);
            certificateGenerator.SetNotBefore(notBefore);
            certificateGenerator.SetNotAfter(notAfter);
            log?.AppendLine($"notBefore = {notBefore}");
            log?.AppendLine($"notAfter = {notAfter}");

            var subjectKeyPair = new AsymmetricCipherKeyPair(
                PublicKeyFactory.CreateKey(caKeyPair.Value.Public),
                PrivateKeyFactory.CreateKey(caKeyPair.Value.Private)
                );

            certificateGenerator.SetPublicKey(subjectKeyPair.Public);

            // Generating the Certificate
            var issuerKeyPair = subjectKeyPair;
            ISignatureFactory signatureFactory = new Asn1SignatureFactory("SHA512WITHRSA", issuerKeyPair.Private, random);

            // selfsign certificate
            var certificate = certificateGenerator.Generate(signatureFactory);

            ca = (issuerKeyPair.Private, issuerKeyPair.Public);
            name = certificate.SubjectDN;
        }

        // generating this can take a while, so we cache that at the process level, to significantly speed up the tests
        private static Lazy<(byte[] Private, byte[] Public)>
            caKeyPair = new Lazy<(byte[] Private, byte[] Public)>(GenerateKey);

        private static (byte[] Private, byte[] Public) GenerateKey()
        {
            AsymmetricCipherKeyPair kp = GetRsaKey();

            var privateKeyInfo = PrivateKeyInfoFactory.CreatePrivateKeyInfo(kp.Private);
            var publicKeyInfo = SubjectPublicKeyInfoFactory.CreateSubjectPublicKeyInfo(kp.Public);

            return (privateKeyInfo.ToAsn1Object().GetDerEncoded(), publicKeyInfo.ToAsn1Object().GetDerEncoded());
        }

        private static AsymmetricCipherKeyPair GetRsaKey()
        {
            var keyGenerationParameters = new KeyGenerationParameters(GetSeededSecureRandom(), 4096);
            var keyPairGenerator = new RsaKeyPairGenerator();
            keyPairGenerator.Init(keyGenerationParameters);
            var kp = keyPairGenerator.GenerateKeyPair();
            return kp;
        }

        public static SecureRandom GetSeededSecureRandom()
        {
            return new SecureRandom(new CryptoApiRandomGenerator());
        }
    }

    public static class PublicKeyPinningHashHelpers
    {
        public static string GetPublicKeyPinningHash(this X509Certificate2 cert)
        {
            //Get the SubjectPublicKeyInfo member of the certificate
            var subjectPublicKeyInfo = GetSubjectPublicKeyInfoRaw(cert);

            //Take the SHA2-256 hash of the DER ASN.1 encoded value
            byte[] digest;
            using (var sha2 = new SHA256Managed())
            {
                digest = sha2.ComputeHash(subjectPublicKeyInfo);
            }

            //Convert hash to base64
            var hash = Convert.ToBase64String(digest);

            return hash;
        }

        public static unsafe byte[] GetSubjectPublicKeyInfoRaw(X509Certificate2 cert)
        {
            /*
             Certificate is, by definition:

                Certificate  ::=  SEQUENCE  {
                    tbsCertificate       TBSCertificate,
                    signatureAlgorithm   AlgorithmIdentifier,
                    signatureValue       BIT STRING
                }

               TBSCertificate  ::=  SEQUENCE  {
                    version         [0]  EXPLICIT Version DEFAULT v1,
                    serialNumber         CertificateSerialNumber,
                    signature            AlgorithmIdentifier,
                    issuer               Name,
                    validity             Validity,
                    subject              Name,
                    subjectPublicKeyInfo SubjectPublicKeyInfo,
                    issuerUniqueID  [1]  IMPLICIT UniqueIdentifier OPTIONAL, -- If present, version MUST be v2 or v3
                    subjectUniqueID [2]  IMPLICIT UniqueIdentifier OPTIONAL, -- If present, version MUST be v2 or v3
                    extensions      [3]  EXPLICIT Extensions       OPTIONAL  -- If present, version MUST be v3
                }

            So we walk the ASN.1 DER tree in order to drill down to the SubjectPublicKeyInfo item
            */

            var rawCert = cert.GetRawCertData();
            var bufferLength = rawCert.Length;

            fixed (byte* certPtr = rawCert)
            {
                var ptr = AsnNext(certPtr, ref bufferLength, true, false);  // unwrap certificate sequence
                ptr = AsnNext(ptr, ref bufferLength, false, false); // get tbsCertificate
                ptr = AsnNext(ptr, ref bufferLength, true, false);  // unwrap tbsCertificate sequence
                ptr = AsnNext(ptr, ref bufferLength, false, true);  // skip tbsCertificate.Version
                ptr = AsnNext(ptr, ref bufferLength, false, true);  // skip tbsCertificate.SerialNumber
                ptr = AsnNext(ptr, ref bufferLength, false, true);  // skip tbsCertificate.Signature
                ptr = AsnNext(ptr, ref bufferLength, false, true);  // skip tbsCertificate.Issuer
                ptr = AsnNext(ptr, ref bufferLength, false, true);  // skip tbsCertificate.Validity
                ptr = AsnNext(ptr, ref bufferLength, false, true);  // skip tbsCertificate.Subject
                ptr = AsnNext(ptr, ref bufferLength, false, false); // get tbsCertificate.SubjectPublicKeyInfo

                var subjectPublicKeyInfo = new byte[bufferLength];
                fixed (byte* newPtr = subjectPublicKeyInfo)
                {
                    Memory.Copy(newPtr, ptr, bufferLength);
                }
                return subjectPublicKeyInfo;
            }
        }

        private static unsafe byte* AsnNext(byte* buffer, ref int bufferLength, bool unwrap, bool getRemaining)
        {
            if (bufferLength < 2)
            {
                return buffer;
            }

            var index = 0;
            //var entityType = buffer[index];
            index++;

            int length = buffer[index];
            index++;

            var lengthBytes = 1;
            if (length >= 0x80)
            {
                lengthBytes = length & 0x0F; //low nibble is number of length bytes to follow
                length = 0;

                for (var i = 0; i < lengthBytes; i++)
                {
                    length = (length << 8) + (int)buffer[index + i];
                }
                lengthBytes++;
            }

            int skip;
            int take;
            if (unwrap)
            {
                skip = 1 + lengthBytes;
                take = length;
            }
            else
            {
                skip = 0;
                take = 1 + lengthBytes + length;
            }

            if (getRemaining == false)
            {
                buffer += skip;
                bufferLength = take;
            }
            else
            {
                buffer += skip + take;
                bufferLength -= (skip + take);
            }

            return buffer;
        }
    }
}
