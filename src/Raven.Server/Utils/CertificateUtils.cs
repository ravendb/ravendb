using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Org.BouncyCastle.Asn1;
using Org.BouncyCastle.Asn1.X509;
using Org.BouncyCastle.Crypto;
using Org.BouncyCastle.Crypto.Generators;
using Org.BouncyCastle.Crypto.Operators;
using Org.BouncyCastle.Crypto.Prng;
using Org.BouncyCastle.Pkcs;
using Org.BouncyCastle.Security;
using Org.BouncyCastle.X509;
using Org.BouncyCastle.X509.Extension;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Util;
using Raven.Server.Commercial;
using Raven.Server.Commercial.SetupWizard;
using Raven.Server.Config.Categories;
using Sparrow;
using Sparrow.Logging;
using Sparrow.Platform;
using BigInteger = Org.BouncyCastle.Math.BigInteger;
using X509Certificate = Org.BouncyCastle.X509.X509Certificate;

namespace Raven.Server.Utils
{
    public static class CertificateUtils
    {
        private const int BitsPerByte = 8;

        private static readonly Logger Logger = LoggingSource.Instance.GetLogger("Server", typeof(CertificateUtils).FullName);

        private static string GetCertificateName(X509Certificate2 certificate)
        {
            if (certificate == null)
                return string.Empty;

            return string.IsNullOrEmpty(certificate.FriendlyName) == false ? certificate.FriendlyName : certificate.Subject;
        }

        private static string GenerateCertificateChainDebugLog(X509Chain chain)
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("Certificate Chain (from leaf to CA) (name - pinning hash):");
            foreach (var element in chain.ChainElements)
            {
                var certificate = element.Certificate;
                stringBuilder.AppendLine($"{GetCertificateName(certificate)} - {certificate.GetPublicKeyPinningHash()}");
            }

            return stringBuilder.ToString();
        }

        internal static bool CertHasKnownIssuer(X509Certificate2 userCertificate, X509Certificate2 knownCertificate, SecurityConfiguration securityConfiguration, List<string> explanations = null)
        {
            X509Certificate2 issuerCertificate = null;

            var userChain = new X509Chain();
            // we are not disabling certificate downloads because this method is checking public key pinning hashes
            // in order to do that properly it needs to be able to verify the chain by download the certificates
            // userChain.ChainPolicy.DisableCertificateDownloads = true;

            var knownCertChain = new X509Chain();
            // we are not disabling certificate downloads because this method is checking public key pinning hashes
            // in order to do that properly it needs to be able to verify the chain by download the certificates
            //knownCertChain.ChainPolicy.DisableCertificateDownloads = true;

            explanations?.Add($"Try building client certificate chain - {GetCertificateName(userCertificate)}.");
            try
            {
                userChain.Build(userCertificate);
            }
            catch (Exception e)
            {
                var message = $"Cannot validate new client certificate '{GetCertificateName(userCertificate)} - ({userCertificate.Thumbprint})'," +
                              $" failed to build the chain.";
                explanations?.Add(message);
                if (Logger.IsInfoEnabled) 
                    Logger.Info(message, e);

                return false;
            }

            try
            {
                issuerCertificate = userChain.ChainElements.Count > 1
                    ? userChain.ChainElements[1].Certificate
                    : userChain.ChainElements[0].Certificate;
            }
            catch (Exception e)
            {
                var message = $"Cannot extract pinning hash from the client certificate's issuer '{issuerCertificate?.FriendlyName} {issuerCertificate?.Thumbprint}'.";
                explanations?.Add(message);
                if (Logger.IsInfoEnabled) 
                    Logger.Info(message, e);

                return false;
            }

            explanations?.Add($"Try building know certificate chain - {GetCertificateName(knownCertificate)}.");
            try
            {
                knownCertChain.Build(knownCertificate);
            }
            catch (Exception e)
            {
                var message = $"Cannot validate new client certificate '{GetCertificateName(userCertificate)} {userCertificate.Thumbprint}'." +
                              $" Found a known certificate '{knownCertificate.Thumbprint}' with the same hash but failed to build its chain.";
                explanations?.Add(message);
                if (Logger.IsInfoEnabled) 
                    Logger.Info(message, e);

                return false;
            }

            explanations?.Add("Comparing certificates (leafs):\n" +
                              $"Client certificate - {GetCertificateName(userCertificate)} - {userCertificate.GetPublicKeyPinningHash()}\n" +
                              $"Known certificate - {GetCertificateName(knownCertificate)} - {knownCertificate.GetPublicKeyPinningHash()}");
            // client certificates (leafs) Public Key pinning hashes must match
            if (userCertificate.GetPublicKeyPinningHash() != knownCertificate.GetPublicKeyPinningHash())
            {
                explanations?.Add("Client Certificate Public Key pinning hashes does not match");
                return false;
            }

            // support self-signed certs
            if (knownCertChain.ChainElements.Count == 1 && userChain.ChainElements.Count == 1)
            {
                explanations?.Add("Client certificate and known certificate are self-signed and have matching public key pinning hashes.");
                return true;
            }

            if (explanations != null)
            {
                explanations.Add("Comparing issuers pinning hashes of client certificate and known certificate.");
                explanations.Add($"Client certificate chain info:\n{GenerateCertificateChainDebugLog(userChain)}");
                explanations.Add($"Known certificate chain info:\n{GenerateCertificateChainDebugLog(knownCertChain)}");
            }
            
            // compare issuers pinning hashes starting from top of the chain (CA) since it's least likely to change
            // chain may have additional elements due to cross-signing, that's why we compare every issuer with each other
            for (var i = knownCertChain.ChainElements.Count - 1; i > 0; i--)
            {
                var knownPinningHash = knownCertChain.ChainElements[i].Certificate.GetPublicKeyPinningHash();
                for (int j = userChain.ChainElements.Count - 1; j > 0; j--)
                {
                    if (knownPinningHash == userChain.ChainElements[j].Certificate.GetPublicKeyPinningHash())
                    {
                        explanations?.Add($"Client certificate has issuer with matching public key pinning hash - {userChain.ChainElements[j].Certificate.FriendlyName}");
                        return true;
                    }
                }
            }

            explanations?.Add("None of the issuers Public Key pinning hashes match.");
            return false;
        }

        public sealed class CertificateHolder : IDisposable
        {
            public readonly string CertificateForClients;
            public readonly X509Certificate2 Certificate;
            public readonly SslStreamCertificateContext CertificateContext;
            public readonly AsymmetricKeyEntry PrivateKey;

            private CertificateHolder()
            {
            }

            public CertificateHolder(X509Certificate2 certificate, AsymmetricKeyEntry privateKey)
                : this(certificate, privateKey, certificateForClients: null)
            {
            }

            public CertificateHolder(X509Certificate2 certificate, AsymmetricKeyEntry privateKey, string certificateForClients)
            {
                Certificate = certificate ?? throw new ArgumentNullException(nameof(certificate));
                CertificateContext = SslStreamCertificateContext.Create(Certificate, additionalCertificates: null);
                PrivateKey = privateKey ?? throw new ArgumentNullException(nameof(privateKey));
                CertificateForClients = certificateForClients;
            }

            public void Dispose()
            {
                Certificate?.Dispose();
            }

            public static CertificateHolder CreateEmpty() => new();
        }
        public static byte[] CreateSelfSignedTestCertificate(string commonNameValue, string issuerName, StringBuilder log = null)
        {
            // Note this is for tests only!
            CreateCertificateAuthorityCertificate(commonNameValue + " CA", out var ca, out var caSubjectName, log);
            CreateSelfSignedCertificateBasedOnPrivateKey(commonNameValue, caSubjectName, ca, false, false, DateTime.UtcNow.Date.AddMonths(3), out var certBytes, log: log);
            var selfSignedCertificateBasedOnPrivateKey = CertificateLoaderUtil.CreateCertificate(certBytes);
            selfSignedCertificateBasedOnPrivateKey.Verify();

            // We had a problem where we didn't cleanup the user store in Linux (~/.dotnet/corefx/cryptography/x509stores/ca)
            // and it exploded with thousands of certificates. This caused ssl handshakes to fail on that machine, because it would timeout when
            // trying to match one of these certs to validate the chain
            RemoveOldTestCertificatesFromOsStore(commonNameValue);
            return certBytes;
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

        public static X509Certificate2 CreateSelfSignedClientCertificate(string commonNameValue, CertificateHolder certificateHolder, out byte[] certBytes, DateTime notAfter)
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

            var cert = CertificateLoaderUtil.CreateCertificate(certBytes, flags: CertificateLoaderUtil.FlagsForPersist);
            return cert;
        }

        private static void ValidateNoPrivateKeyInServerCert(byte[] serverCertBytes)
        {
            var collection = new X509Certificate2Collection();
            // without the server private key here
            CertificateLoaderUtil.Import(collection, serverCertBytes);

            if (new X509Certificate2Collection(collection).OfType<X509Certificate2>().FirstOrDefault(x => x.HasPrivateKey) != null)
                throw new InvalidOperationException("After export of CERT, still have private key from signer in certificate, should NEVER happen");
        }

        public static X509Certificate2 CreateSelfSignedExpiredClientCertificate(string commonNameValue, CertificateHolder certificateHolder)
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

            return CertificateLoaderUtil.CreateCertificate(certBytes);
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
            certificateGenerator.AddExtension(X509Extensions.KeyUsage.Id, true, new KeyUsage(KeyUsage.DigitalSignature | KeyUsage.KeyEncipherment));
            if (isClientCertificate)
            {
                certificateGenerator.AddExtension(X509Extensions.ExtendedKeyUsage.Id, true, new ExtendedKeyUsage(KeyPurposeID.id_kp_clientAuth));
            }
            else
            {
                certificateGenerator.AddExtension(X509Extensions.ExtendedKeyUsage.Id, true,
                    new ExtendedKeyUsage(KeyPurposeID.id_kp_serverAuth, KeyPurposeID.id_kp_clientAuth));
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
            var store = new Pkcs12StoreBuilder().Build();
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

        public static X509Certificate2 CreateCertificateAuthorityCertificate(string commonNameValue,
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

            certificateGenerator.AddExtension(
                X509Extensions.BasicConstraints.Id, true, new BasicConstraints(true));
            certificateGenerator.AddExtension(X509Extensions.KeyUsage.Id, true,
                new KeyUsage(KeyUsage.DigitalSignature | KeyUsage.CrlSign | KeyUsage.KeyCertSign));
            certificateGenerator.AddExtension(X509Extensions.ExtendedKeyUsage.Id, true,
                new ExtendedKeyUsage(KeyPurposeID.id_kp_serverAuth, KeyPurposeID.id_kp_clientAuth));

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

            var authorityKeyIdentifier =
                new AuthorityKeyIdentifier(
                    SubjectPublicKeyInfoFactory.CreateSubjectPublicKeyInfo(issuerKeyPair.Public),
                    new GeneralNames(new GeneralName(issuerDN)),
                    serialNumber);
            certificateGenerator.AddExtension(
                X509Extensions.AuthorityKeyIdentifier.Id, false, authorityKeyIdentifier);

            var subjectKeyIdentifier =
                new SubjectKeyIdentifier(
                    SubjectPublicKeyInfoFactory.CreateSubjectPublicKeyInfo(subjectKeyPair.Public));
            certificateGenerator.AddExtension(
                X509Extensions.SubjectKeyIdentifier.Id, false, subjectKeyIdentifier);

            // selfsign certificate
            var certificate = certificateGenerator.Generate(signatureFactory);

            ca = (issuerKeyPair.Private, issuerKeyPair.Public);
            name = certificate.SubjectDN;

            var store = new Pkcs12StoreBuilder().Build();
            string friendlyName = certificate.SubjectDN.ToString();
            var certificateEntry = new X509CertificateEntry(certificate);
            var keyEntry = new AsymmetricKeyEntry(subjectKeyPair.Private);

            log?.AppendLine($"certificateEntry.Certificate = {certificateEntry.Certificate}");

            store.SetCertificateEntry(friendlyName, certificateEntry);
            store.SetKeyEntry(friendlyName, keyEntry, new[] { certificateEntry });
            var stream = new MemoryStream();
            store.Save(stream, Array.Empty<char>(), random);

            return new X509Certificate2(stream.ToArray());
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

        public static string GetServerUrlFromCertificate(X509Certificate2 cert, SetupInfo setupInfo, string nodeTag, int port, int tcpPort, out string publicTcpUrl, out string domain)
        {
            publicTcpUrl = null;
            var node = setupInfo.NodeSetupInfos[nodeTag];

            var subjectAlternativeNames = GetCertificateAlternativeNames(cert).ToList();
            var subject = subjectAlternativeNames.FirstOrDefault();
            
            // fallback to common name
            if (string.IsNullOrEmpty(subject))
                subject = cert.GetNameInfo(X509NameType.SimpleName, false);
            Debug.Assert(string.IsNullOrEmpty(subject) == false, nameof(subject) + " is null or empty");
            if (subject[0] == '*')
            {
                var parts = subject.Split("*.");
                if (parts.Length != 2)
                    throw new FormatException($"{subject} is not a valid wildcard name for a certificate.");

                domain = parts[1];

                publicTcpUrl = node.ExternalTcpPort != Constants.Network.ZeroValue
                    ? $"tcp://{nodeTag.ToLower()}.{domain}:{node.ExternalTcpPort}"
                    : $"tcp://{nodeTag.ToLower()}.{domain}:{tcpPort}";

                if (setupInfo.NodeSetupInfos[nodeTag].ExternalPort != Constants.Network.ZeroValue)
                    return $"https://{nodeTag.ToLower()}.{domain}:{node.ExternalPort}";

                return port == Constants.Network.DefaultSecuredRavenDbHttpPort
                    ? $"https://{nodeTag.ToLower()}.{domain}"
                    : $"https://{nodeTag.ToLower()}.{domain}:{port}";
            }

            domain = subject; //default for one node case

            foreach (var value in subjectAlternativeNames)
            {
                if (value.StartsWith(nodeTag + ".", StringComparison.OrdinalIgnoreCase) == false)
                    continue;

                domain = value;
                break;
            }

            var url = $"https://{domain}";

            if (node.ExternalPort != Constants.Network.ZeroValue)
                url += ":" + node.ExternalPort;
            else if (port != Constants.Network.DefaultSecuredRavenDbHttpPort)
                url += ":" + port;

            publicTcpUrl = node.ExternalTcpPort != Constants.Network.ZeroValue
                ? $"tcp://{domain}:{node.ExternalTcpPort}"
                : $"tcp://{domain}:{tcpPort}";

            node.PublicServerUrl = url;
            node.PublicTcpServerUrl = publicTcpUrl;

            return url;
        }

        public static IEnumerable<string> GetCertificateAlternativeNames(X509Certificate2 cert)
        {
            // If we have alternative names, find the appropriate url using the node tag
            var sanNames = cert.Extensions["2.5.29.17"];

            if (sanNames == null)
                yield break;

            var generalNames = GeneralNames.GetInstance(Asn1Object.FromByteArray(sanNames.RawData));

            foreach (var certHost in generalNames.GetNames())
            {
                yield return certHost.Name.ToString();
            }
        }

        public static void RegisterClientCertInOs(Action<IOperationProgress> onProgress, SetupProgressAndResult progress, X509Certificate2 clientCert)
        {
            using (var userPersonalStore = new X509Store(StoreName.My, StoreLocation.CurrentUser, OpenFlags.ReadWrite))
            {
                try
                {
                    userPersonalStore.Add(clientCert);
                    progress.AddInfo($"Successfully registered the admin client certificate in the OS Personal CurrentUser Store '{userPersonalStore.Name}'.");
                    onProgress(progress);
                }
                catch (Exception e)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Failed to register client certificate in the current user personal store '{userPersonalStore.Name}'.", e);
                }
            }
        }

        public static async Task<X509Certificate2> CompleteAuthorizationAndGetCertificate(CompleteAuthorizationAndGetCertificateParameters parameters)
        {
            if (parameters.ChallengeResult.Challange == null && parameters.ChallengeResult.Cache != null)
            {
                return BuildNewPfx(parameters.SetupInfo, parameters.ChallengeResult.Cache.Certificate, parameters.ChallengeResult.Cache.PrivateKey);
            }

            try
            {
                await parameters.Client.CompleteChallenges(parameters.Token);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to Complete Let's Encrypt challenge(s).", e);
            }

            parameters.OnValidationSuccessful();

            (X509Certificate2 Cert, RSA PrivateKey) result;
            try
            {
                result = await parameters.Client.GetCertificate(parameters.ExistingPrivateKey, parameters.Token);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to acquire certificate from Let's Encrypt.", e);
            }

            try
            {
                return BuildNewPfx(parameters.SetupInfo, result.Cert, result.PrivateKey);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to build certificate from Let's Encrypt.", e);
            }
        }

        private static X509Certificate2 BuildNewPfx(SetupInfo setupInfo, X509Certificate2 certificate, RSA privateKey)
        {
            var certWithKey = certificate.CopyWithPrivateKey(privateKey);

            Pkcs12Store store = new Pkcs12StoreBuilder().Build();

            var chain = new X509Chain();
            chain.ChainPolicy.DisableCertificateDownloads = true;

            chain.Build(certificate);

            foreach (var item in chain.ChainElements)
            {
                var x509Certificate = DotNetUtilities.FromX509Certificate(item.Certificate);

                if (item.Certificate.Thumbprint == certificate.Thumbprint)
                {
                    var key = new AsymmetricKeyEntry(DotNetUtilities.GetKeyPair(certWithKey.GetRSAPrivateKey()).Private);
                    store.SetKeyEntry(x509Certificate.SubjectDN.ToString(), key, new[] { new X509CertificateEntry(x509Certificate) });
                    continue;
                }

                store.SetCertificateEntry(item.Certificate.Subject, new X509CertificateEntry(x509Certificate));
            }

            var memoryStream = new MemoryStream();
            store.Save(memoryStream, Array.Empty<char>(), new SecureRandom(new CryptoApiRandomGenerator()));
            var certBytes = memoryStream.ToArray();

            Debug.Assert(certBytes != null);
            setupInfo.Certificate = Convert.ToBase64String(certBytes);

            return CertificateLoaderUtil.CreateCertificate(certBytes, flags: CertificateLoaderUtil.FlagsForExport);
        }

        public static string GetBasicCertificateInfo(this X509Certificate2 certificate)
        {
            return $"Thumbprint: {certificate.Thumbprint}, Subject: {certificate.Subject}";
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
            using (var sha2 = SHA256.Create())
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
