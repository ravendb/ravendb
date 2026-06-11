using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Formats.Asn1;
using System.Linq;
using System.Net.Security;
using System.Numerics;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.Util;
using Raven.Server.Commercial;
using Raven.Server.Commercial.SetupWizard;
using Raven.Server.Config.Categories;
using Raven.Server.Logging;
using Raven.Server.ServerWide;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Server.Logging;

namespace Raven.Server.Utils
{
    public static class CertificateUtils
    {
        private const int BitsPerByte = 8;

        private static readonly RavenLogger Logger = RavenLogManager.Instance.GetLoggerForServer(typeof(CertificateUtils));

        private static string GenerateCertificateChainDebugLog(X509Chain chain)
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("Certificate Chain (from leaf to CA) (name - pinning hash):");
            foreach (var element in chain.ChainElements)
            {
                var certificate = element.Certificate;
                stringBuilder.AppendLine($"{certificate.GetDisplayName()} - {certificate.GetPublicKeyPinningHash()}");
            }

            return stringBuilder.ToString();
        }

        internal static bool CertHasKnownIssuer(X509Certificate2 userCertificate, X509Certificate2 knownCertificate, SecurityConfiguration securityConfiguration, List<string> explanations = null)
        {
            X509Certificate2 issuerCertificate = null;

            var userChain = new X509Chain();
            var knownCertChain = new X509Chain();

            if (PlatformDetails.RunningOnMacOsx)
            {
                using (var store = new X509Store(StoreName.CertificateAuthority, StoreLocation.CurrentUser))
                {
                    store.Open(OpenFlags.ReadOnly);
                    userChain.ChainPolicy.ExtraStore.AddRange(store.Certificates);
                    knownCertChain.ChainPolicy.ExtraStore.AddRange(store.Certificates);
                }
            }

            // we are not disabling certificate downloads because this method is checking public key pinning hashes
            // in order to do that properly it needs to be able to verify the chain by download the certificates
            // userChain.ChainPolicy.DisableCertificateDownloads = true;
            // knownCertChain.ChainPolicy.DisableCertificateDownloads = true;

            explanations?.Add($"Try building client certificate chain - {userCertificate.GetDisplayName()}.");
            try
            {
                userChain.Build(userCertificate);
            }
            catch (Exception e)
            {
                var message = $"Cannot validate new client certificate '{userCertificate.GetDisplayName()} - ({userCertificate.Thumbprint})'," +
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

            explanations?.Add($"Try building know certificate chain - {knownCertificate.GetDisplayName()}.");
            try
            {
                knownCertChain.Build(knownCertificate);
            }
            catch (Exception e)
            {
                var message = $"Cannot validate new client certificate '{userCertificate.GetDisplayName()} {userCertificate.Thumbprint}'." +
                              $" Found a known certificate '{knownCertificate.Thumbprint}' with the same hash but failed to build its chain.";
                explanations?.Add(message);
                if (Logger.IsInfoEnabled)
                    Logger.Info(message, e);

                return false;
            }

            explanations?.Add("Comparing certificates (leafs):\n" +
                              $"Client certificate - {userCertificate.GetDisplayName()} - {userCertificate.GetPublicKeyPinningHash()}\n" +
                              $"Known certificate - {knownCertificate.GetDisplayName()} - {knownCertificate.GetPublicKeyPinningHash()}");
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
            public string ServerCertificateForClients { get; private set; }
            public X509Certificate2 ServerCertificate { get; }
            public X509Certificate2 ClientCertificate { get; } // this is cert with client EKU and server cert key pair
            public SslStreamCertificateContext ServerCertificateContext { get; private set; }
            public readonly AsymmetricAlgorithm PrivateKey;

            private CertificateHolder()
            {
            }

            public CertificateHolder(X509Certificate2 serverCertificate, AsymmetricAlgorithm privateKey)
            {
                ServerCertificate = serverCertificate ?? throw new ArgumentNullException(nameof(serverCertificate));
                PrivateKey = privateKey ?? throw new ArgumentNullException(nameof(privateKey));
                ServerCertificateContext = SslStreamCertificateContext.Create(ServerCertificate, additionalCertificates: null);
                ServerCertificateForClients = Convert.ToBase64String(ServerCertificate.Export(X509ContentType.Cert));

                if (SecretProtection.HasCertificateClientAuthEnhancedKeyUsage(ServerCertificate))
                {
                    ClientCertificate = serverCertificate;
                }
                else
                {
                    var clientCertificate = CreateClientCertificateFromServerCertificate(serverCertificate, out _);
                    ClientCertificate = clientCertificate;
                }
            }

            public void Dispose()
            {
                ServerCertificate?.Dispose();
                ClientCertificate?.Dispose();
            }

            public static CertificateHolder CreateEmpty() => new();
        }

        public static byte[] CreateSelfSignedTestCertificate(string commonNameValue, string issuerName, StringBuilder log = null, bool with2Eku = true)
        {
            // Note this is for tests only!
            var caCert = CreateCertificateAuthorityCertificate(
                $"{commonNameValue} CA",
                out var caSubjectName,
                log);

            CreateSelfSignedCertificateBasedOnPrivateKey(
                commonNameValue: commonNameValue,
                issuerCN: caSubjectName,
                issuerKeyPair: (caCert.GetExportableRsaPrivateKey(), caCert.GetRSAPublicKey()),
                isClientCertificate: false,
                isCaCertificate: false,
                notAfter: DateTime.UtcNow.Date.AddMonths(3),
                certBytes: out var certBytes,
                log: log,
                sans: [commonNameValue, "localhost", $"*.{commonNameValue}"],
                with2Eku: with2Eku);

            var selfSignedCertificateBasedOnPrivateKey = CertificateLoaderUtil.CreateCertificate(certBytes);
            selfSignedCertificateBasedOnPrivateKey.Verify();
            GC.KeepAlive(selfSignedCertificateBasedOnPrivateKey); // https://github.com/dotnet/runtime/issues/122642#issuecomment-3720461147

            // We had a problem where we didn't cleanup the user store in Linux (~/.dotnet/corefx/cryptography/x509stores/ca)
            // and it exploded with thousands of certificates. This caused ssl handshakes to fail on that machine, because it would timeout when
            // trying to match one of these certs to validate the chain
            RemoveOldTestCertificatesFromOsStore(commonNameValue);

            return certBytes;
        }

        internal static void RemoveOldTestCertificatesFromOsStore(string commonNameValue)
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

        public static X509Certificate2 CreateSelfSignedClientCertificate(string commonNameValue, X509Certificate2 issuerCertificate, AsymmetricAlgorithm issuerPrivateKey, out byte[] certBytes, DateTime notAfter)
        {
            var serverCertBytes = issuerCertificate.Export(X509ContentType.Cert);
            CreateSelfSignedCertificateBasedOnPrivateKey(
                commonNameValue,
                issuerCertificate.SubjectName,
                (issuerPrivateKey, issuerCertificate.GetRSAPublicKey()),
                true,
                false,
                notAfter,
                out certBytes);

            ValidateNoPrivateKeyInServerCert(serverCertBytes);

            // Create a collection to hold all the certificates.
            var pfxCollection = new X509Certificate2Collection();

            // Import the existing PFX file (client certificate) into the collection
#pragma warning disable SYSLIB0057
            pfxCollection.Import(certBytes, null, CertificateLoaderUtil.FlagsForExport);
#pragma warning restore SYSLIB0057

            // Add the server certificate to the collection
            pfxCollection.Add(CertificateLoaderUtil.CreateCertificate(serverCertBytes, flags: CertificateLoaderUtil.FlagsForExport));

            // Export the entire collection as a new PFX file.
            // The native .NET method handles the complex encoding and
            // combines all certificates into a single PFX byte array.
            certBytes = pfxCollection.Export(X509ContentType.Pfx, string.Empty);

            var cert = CertificateLoaderUtil.CreateCertificate(certBytes, flags: CertificateLoaderUtil.FlagsForPersist);
            return cert;
        }

        private static void ValidateNoPrivateKeyInServerCert(byte[] serverCertBytes)
        {
            var collection = new X509Certificate2Collection();

            try
            {
                // without the server private key here
                CertificateLoaderUtil.Import(collection, serverCertBytes);
            }
            catch (Exception e)
            {
                throw new CryptographicException("Failed to import server certificate", e);
            }

            if (new X509Certificate2Collection(collection).OfType<X509Certificate2>().FirstOrDefault(x => x.HasPrivateKey) != null)
                throw new InvalidOperationException("After export of CERT, still have private key from signer in certificate, should NEVER happen");
        }

        private static void ValidateNoPrivateKeyInServerCert(X509Certificate2 certificate)
        {
            if (certificate.HasPrivateKey)
                throw new InvalidOperationException("After export of CERT, still have private key from signer in certificate, should NEVER happen");
        }

        public static X509Certificate2 CreateSelfSignedExpiredClientCertificate(string commonNameValue, X509Certificate2 serverCertificate, AsymmetricAlgorithm privateKey)
        {
            CreateSelfSignedCertificateBasedOnPrivateKey(
                commonNameValue,
                serverCertificate.SubjectName,
                (privateKey, serverCertificate.GetRSAPublicKey()),
                true,
                false,
                DateTime.UtcNow.Date.AddYears(-1),
                out var certBytes,
                notBefore: DateTime.UtcNow.Date.AddYears(-2));

            return CertificateLoaderUtil.CreateCertificate(certBytes);
        }

        public static void CreateSelfSignedCertificateBasedOnPrivateKey(
            string commonNameValue,
            X500DistinguishedName issuerCN,
            (AsymmetricAlgorithm PrivateKey, AsymmetricAlgorithm PublicKey) issuerKeyPair,
            bool isClientCertificate,
            bool isCaCertificate,
            DateTime notAfter,
            out byte[] certBytes,
            AsymmetricAlgorithm subjectPrivateKey = null,
            StringBuilder log = null,
            IEnumerable<string> sans = null,
            bool with2Eku = false,
            byte[] issuerCertBytes = null,
            DateTime? notBefore = null)
        {
            log?.AppendLine("CreateSelfSignedCertificateBasedOnPrivateKey:");

            // Prepare Subject Key Pair
            // currently we support only RSA keys
            RSA privateKey = subjectPrivateKey as RSA ?? GetRsaKey();
            log?.AppendLine("Subject key pair prepared.");

            // Prepare Distinguished Names
            var subjectName = new X500DistinguishedName(string.Empty);
            if (string.IsNullOrEmpty(commonNameValue) == false)
            {
                var commonNameBuilder = new X500DistinguishedNameBuilder();
                commonNameBuilder.AddCommonName(commonNameValue);
                subjectName = commonNameBuilder.Build();
            }

            log?.AppendLine($"subjectDN = {subjectName}");
            log?.AppendLine($"issuerDN = {issuerCN}");

            // Create the Certificate Request
            var request = new CertificateRequest(subjectName, privateKey, HashAlgorithmName.SHA512, RSASignaturePadding.Pkcs1);
            log?.AppendLine("CertificateRequest object created.");

            // Add Extensions
            X509KeyUsageFlags keyUsageFlags = X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment;
            if (isCaCertificate)
            {
                keyUsageFlags |= X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign;
            }

            request.CertificateExtensions.Add(new X509KeyUsageExtension(keyUsageFlags, isCaCertificate));

            if (with2Eku)
            {
                request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
                    new OidCollection { new Oid(Constants.Certificates.ClientAuthenticationOid), new Oid(Constants.Certificates.ServerAuthenticationOid) }, false));
            }
            else
            {
                var purposeOid = isClientCertificate ? new Oid(Constants.Certificates.ClientAuthenticationOid) : new Oid(Constants.Certificates.ServerAuthenticationOid);
                request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(new OidCollection { purposeOid }, false));
            }

            if (sans != null && sans.Any())
            {
                var builder = new SubjectAlternativeNameBuilder();
                foreach (var san in sans)
                {
                    builder.AddDnsName(san);
                }

                request.CertificateExtensions.Add(builder.Build());
            }

            if (isCaCertificate)
            {
                request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, true, 0, true));
            }

            // Create the serial number.
            byte[] serialNumberBytes = new byte[20];
            using (var rng = RandomNumberGenerator.Create())
            {
                rng.GetBytes(serialNumberBytes);
            }

            serialNumberBytes[0] &= 0x7F; // Force positive number, preventing the '00' padding

            log?.AppendLine($"serialNumber bytes generated.");

            if (issuerCertBytes is { Length: > 0 })
            {
                request.CertificateExtensions.Add(new X509Extension(new Oid(Constants.Certificates.ServerCertExtensionOid), issuerCertBytes, false));
            }

            // Create the signature generator.
            // This is the correct way to pass the private key for signing in older .NET versions.
            var signatureGenerator = X509SignatureGenerator.CreateForRSA((RSA)issuerKeyPair.PrivateKey, RSASignaturePadding.Pkcs1);

            // Create and sign the certificate using the signature generator.
            notBefore ??= DateTime.UtcNow.Date.AddDays(-7);
            X509Certificate2 certificate = request.Create(
                issuerCN,
                signatureGenerator,
                notBefore.Value,
                notAfter.ToUniversalTime(),
                serialNumberBytes);

            certificate = certificate.CopyWithPrivateKey(privateKey);

            log?.AppendLine($"Certificate created.");
            log?.AppendLine($"serialNumber = {new BigInteger(serialNumberBytes)}");
            log?.AppendLine($"notBefore = {certificate.NotBefore}");
            log?.AppendLine($"notAfter = {certificate.NotAfter}");

            // Export the certificate to a PFX byte array.
            certBytes = certificate.Export(X509ContentType.Pfx, string.Empty);
            log?.AppendLine($"certBytes.Length = {certBytes.Length}");
        }

        public static X509Certificate2 CreateCertificateAuthorityCertificate(
            string commonNameValue,
            out X500DistinguishedName name,
            StringBuilder log = null,
            bool generateNewKeyPair = false)
        {
            log?.AppendLine("CreateCertificateAuthorityCertificate:");

            // Generate the RSA key pair for the CA.
            (AsymmetricAlgorithm PrivateKey, AsymmetricAlgorithm PublicKey) keyPair = (RSA.Create(),
                RSA.Create());
            if (generateNewKeyPair)
            {
                var newRsaKeyPair = GenerateRsaKey();
                ((RSA)keyPair.PrivateKey).ImportRSAPrivateKey(newRsaKeyPair.Private, out _);
                ((RSA)keyPair.PublicKey).ImportRSAPublicKey(newRsaKeyPair.Public, out _);
                log?.AppendLine("CA key pair generated.");
            }
            else
            {
                ((RSA)keyPair.PrivateKey).ImportRSAPrivateKey(caKeyPair.Value.Private, out _);
                ((RSA)keyPair.PublicKey).ImportRSAPublicKey(caKeyPair.Value.Public, out _);
                log?.AppendLine("Reusing cached CA key pair.");
            }

            log?.AppendLine("PrivateKey = " + ((RSA)keyPair.PrivateKey).ExportRSAPrivateKeyPem());
            log?.AppendLine("PublicKey = " + ((RSA)keyPair.PrivateKey).ExportRSAPublicKeyPem());

            // Define the subject name.
            name = new X500DistinguishedName($"CN={commonNameValue}");
            log?.AppendLine($"SubjectDN = {name}");

            // Create a CertificateRequest object.
            var request = new CertificateRequest(
                name,
                (RSA)keyPair.PrivateKey,
                HashAlgorithmName.SHA512,
                RSASignaturePadding.Pkcs1);

            // Add the required extensions for a CA certificate.
            // BasicConstraintsExtension is crucial for marking it as a CA.
            request.CertificateExtensions.Add(
                new X509BasicConstraintsExtension(true, true, 0, true));
            log?.AppendLine("BasicConstraints extension added.");

            // KeyUsageExtension specifies the purpose of the key.
            request.CertificateExtensions.Add(
                new X509KeyUsageExtension(
                    X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign | X509KeyUsageFlags.DigitalSignature,
                    true));
            log?.AppendLine("KeyUsage extension added.");

            // Create the self-signed certificate.
            // The CreateSelfSigned method automatically adds AuthorityKeyIdentifier and SubjectKeyIdentifier.
            var notBefore = DateTimeOffset.UtcNow.Date.AddDays(-7);
            var notAfter = notBefore.AddYears(2);
            var cert = request.CreateSelfSigned(notBefore, notAfter);
            log?.AppendLine($"Certificate created. NotBefore: {cert.NotBefore}, NotAfter: {cert.NotAfter}");

            return CertificateLoaderUtil.CreateCertificate(
                cert.Export(X509ContentType.Pfx),
                flags: CertificateLoaderUtil.FlagsForExport);
        }

        public static X509Certificate2 CreateClientCertificateFromServerCertificate(X509Certificate2 serverCertificate, out byte[] clientCertBytes)
        {
            // Get the private and public keys from the server certificate.
            var issuerPrivateKey = serverCertificate.GetRSAPrivateKey();
            var issuerPublicKey = serverCertificate.GetRSAPublicKey();

            // Call the native .NET method to create and sign the client certificate.
            CreateSelfSignedCertificateBasedOnPrivateKey(
                commonNameValue: "client-cert-for-cluster-communication",
                issuerCN: serverCertificate.SubjectName,
                issuerKeyPair: (issuerPrivateKey, issuerPublicKey),
                isClientCertificate: true,
                isCaCertificate: false,
                notAfter: serverCertificate.NotAfter,
                certBytes: out clientCertBytes,
                subjectPrivateKey: issuerPrivateKey,
                issuerCertBytes: serverCertificate.Export(X509ContentType.Cert));

            // Return a new X509Certificate2 object from the generated PFX byte array.
            var flags = X509KeyStorageFlags.PersistKeySet;
#pragma warning disable SYSLIB0057
            return new X509Certificate2(clientCertBytes, (string)null, flags);
#pragma warning restore SYSLIB0057
        }

        public static X509Certificate2 ExtractServerCertificateFromExtension(X509Certificate2 clientCert)
        {
            X509Certificate2 serverCertificateFromExtension = null;
            try
            {
                // Find the custom extension by its OID.
                var extension = clientCert.Extensions
                    .FirstOrDefault(ext => ext.Oid?.Value == Constants.Certificates.ServerCertExtensionOid);

                if (extension == null)
                    return null; // No server certificate found

                // Try standard .NET method first
                try
                {
                    // The RawData property of the extension contains the DER-encoded certificate bytes.
                    // The native .NET X509Certificate2 constructor can directly create a certificate from these bytes.
#pragma warning disable SYSLIB0057
                    serverCertificateFromExtension = new X509Certificate2(extension.RawData);
#pragma warning restore SYSLIB0057
                }
                catch (CryptographicException)
                {
                    // If that fails, try alternative approach for BouncyCastle certs

                    // Get the raw data and try to find a certificate structure
                    var rawData = extension.RawData;

                    // Try to extract certificate from ASN.1 structure

                    // Skip any header/metadata that might be present
                    // Look for the start of a possible X.509 certificate (tag 0x30 for SEQUENCE)
                    const int asn1SequenceTag = 0x30;
                    for (int i = 0; i < rawData.Length - 4; i++)
                    {
                        if (rawData[i] != asn1SequenceTag)
                            continue;

                        try
                        {
                            // Try to create a certificate from this position
                            var certBytes = new byte[rawData.Length - i];
                            Array.Copy(rawData, i, certBytes, 0, certBytes.Length);
#pragma warning disable SYSLIB0057
                            serverCertificateFromExtension = new X509Certificate2(certBytes);
#pragma warning restore SYSLIB0057
                            break;
                        }
                        catch
                        {
                            // Keep searching
                        }
                    }

                    if (serverCertificateFromExtension == null)
                        throw;
                }
            }
            catch (Exception ex)
            {
                if (Logger.IsErrorEnabled)
                    Logger.Error("Failed to extract server certificate from client certificate extension" +
                                      " using standard .NET method and BouncyCastle method.", ex);

                return null;
            }

            ValidateNoPrivateKeyInServerCert(serverCertificateFromExtension);

            return serverCertificateFromExtension;
        }


        // generating this can take a while, so we cache that at the process level, to significantly speed up the tests

        private static Lazy<(byte[] Private, byte[] Public)>
            caKeyPair = new Lazy<(byte[] Private, byte[] Public)>(GenerateRsaKey, isThreadSafe: true);

        private static (byte[] Private, byte[] Public) GenerateRsaKey()
        {
            var kp = GetRsaKey();

            // Export the private RSA key
            byte[] privateKey = kp.ExportRSAPrivateKey();

            // Export the public RSA key
            byte[] publicKey = kp.ExportRSAPublicKey();

            return (privateKey, publicKey);
        }

        private static RSA GetRsaKey()
        {
            return RSA.Create(4096);
        }

        public static RandomNumberGenerator GetSeededSecureRandom()
        {
            // In .NET, RandomNumberGenerator.Create() returns a cryptographically strong
            // random number generator that is already seeded by the OS.
            return RandomNumberGenerator.Create();
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
            var sanExtension = (X509SubjectAlternativeNameExtension)cert.Extensions
                .FirstOrDefault(ext => ext.Oid?.Value == "2.5.29.17");

            if (sanExtension == null)
            {
                yield break;
            }

            // Enumerate through the DNS names within the extension.
            foreach (var dnsName in sanExtension.EnumerateDnsNames())
            {
                yield return dnsName;
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

        public static async Task<X509Certificate2> CompleteAuthorizationAndGetCertificate(CompleteAuthorizationAndGetCertificateParameters parameters, string acmeProfile)
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

            (X509Certificate2 Cert, AsymmetricAlgorithm PrivateKey) result;
            try
            {
                result = await parameters.Client.GetCertificate(parameters.ExistingPrivateKey, acmeProfile, parameters.Token);
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

        public static X509Certificate2 BuildNewPfx(SetupInfo setupInfo, X509Certificate2 certificate, AsymmetricAlgorithm privateKey)
        {
            X509Certificate2 safeCertificate = certificate;

            if (PlatformDetails.RunningOnMacOsx)
            {
                // Stripping the keychain context by exporting to raw public bytes (CER) and re-importing
                // prevents the AppleCrypto crash during CopyWithPrivateKey.
                byte[] rawPublicBytes = certificate.Export(X509ContentType.Cert);
#pragma warning disable SYSLIB0057
                safeCertificate = new X509Certificate2(rawPublicBytes);
#pragma warning restore SYSLIB0057
            }

            // Combine the main certificate and the private key safely for both RSA and ECDSA.
            X509Certificate2 certificateWithPrivateKey;
            if (privateKey is RSA rsa)
                certificateWithPrivateKey = safeCertificate.CopyWithPrivateKey(rsa);
            else if (privateKey is ECDsa ecdsa)
                certificateWithPrivateKey = safeCertificate.CopyWithPrivateKey(ecdsa);
            else
                throw new NotSupportedException($"Unsupported key type: {privateKey.GetType().Name}");
            
            if (PlatformDetails.RunningOnMacOsx)
            {
                safeCertificate.Dispose();
            }
            // Build the complete certificate chain.
            using var chain = new X509Chain();
            chain.ChainPolicy.DisableCertificateDownloads = true;
            chain.Build(certificate);

            // Create a collection to hold all certificates for the PFX.
            var pfxCollection = new X509Certificate2Collection();

            // Add the main certificate with its private key.
            pfxCollection.Add(certificateWithPrivateKey);

            // Add the rest of the chain.
            for (int i = 1; i < chain.ChainElements.Count; i++)
            {
                var issuerCert = chain.ChainElements[i].Certificate;
                pfxCollection.Add(issuerCert);
            }

            // Export the entire collection to a single PKCS#12 (PFX) byte array.
            // This Export overload exists in older .NET versions.
            byte[] pfxBytes = pfxCollection.Export(
                X509ContentType.Pfx,
                string.Empty);

            // Store the Base64 representation.
            setupInfo.Certificate = Convert.ToBase64String(pfxBytes);

            // Return a new X509Certificate2 object from the exported PFX data.
            var flags = X509KeyStorageFlags.Exportable | X509KeyStorageFlags.PersistKeySet;
#pragma warning disable SYSLIB0057
            return new X509Certificate2(pfxBytes, string.Empty, flags);
#pragma warning restore SYSLIB0057
        }

        public static string GetBasicCertificateInfo(this X509Certificate2 certificate)
        {
            return $"Thumbprint: {certificate.Thumbprint}, Subject: {certificate.Subject}, Raven Display Name: {certificate.GetDisplayName()}";
        }

        public static string GetDisplayName(this X509Certificate2 certificate)
        {
            if (certificate == null)
                return "(null)";

            if (string.IsNullOrEmpty(certificate.Subject) == false)
                return certificate.Subject;

            if (string.IsNullOrEmpty(certificate.FriendlyName) == false)
                return certificate.FriendlyName;

            var dnsNames = GetCertificateAlternativeNames(certificate).ToList();
            if (dnsNames.Any())
                return string.Join(',', dnsNames);

            return string.Empty;
        }

        public static RSA GetExportableRsaPrivateKey(this X509Certificate2 cert)
        {
            var rsa = cert.GetRSAPrivateKey();
            return rsa?.GetExportableRsaPrivateKey();
        }

        public static RSA GetExportableRsaPrivateKey(this RSA privateKey)
        {
            if (privateKey == null)
                return null;

            const CngExportPolicies exportability = CngExportPolicies.AllowExport | CngExportPolicies.AllowPlaintextExport;

            // Thankfully we don't have to deal with all this on Linux
            if (!PlatformDetails.RunningOnWindows)
                return privateKey;

            // We always expect an RSACng on Windows these days, but that could change
            if ((privateKey is RSACng rsaCng) == false)
                return privateKey;

            // Is the AllowPlaintextExport policy flag already set?
            if ((rsaCng.Key.ExportPolicy & exportability) != CngExportPolicies.AllowExport)
                return privateKey;

            // Export the original RSA private key to an encrypted blob - note you will get "The requested operation
            // is not supported" if trying to export without encryption, so we export with encryption!
            var exported = privateKey.ExportEncryptedPkcs8PrivateKey(nameof(GetExportableRsaPrivateKey),
                new PbeParameters(PbeEncryptionAlgorithm.Aes256Cbc, HashAlgorithmName.SHA256, 2048));

            // Load the exported blob into a fresh RSA object, which will have the AllowPlaintextExport policy without
            // having to do anything else
            RSA copy = RSA.Create();
            copy.ImportEncryptedPkcs8PrivateKey(nameof(GetExportableRsaPrivateKey), exported, out _);

            return copy;
        }

        /// <summary>
        /// Compares two public-key pinning hashes. These are base64-encoded public-key hashes, not secrets, so a
        /// plain ordinal comparison is used (no constant-time requirement) - this also avoids allocating byte
        /// arrays on the SSO auth path, which runs this once per registered server hash.
        /// Both inputs must be non-null and non-empty - otherwise returns false.
        /// </summary>
        internal static bool PinningHashEquals(string a, string b)
        {
            if (string.IsNullOrEmpty(a) || string.IsNullOrEmpty(b))
                return false;

            return string.Equals(a, b, StringComparison.Ordinal);
        }

        // Upper bound on the length of identity fields (username/domain) parsed from a client-presented SSO
        // certificate extension. Any real provider identity is far shorter; this guards against a crafted cert.
        private const int MaxSsoIdentityFieldLength = 1024;

        /// <summary>
        /// Decodes the SSO user payload from a custom X.509 extension's RawData.
        /// The value must be a DER-encoded ASN.1 UTF8String containing JSON:
        ///   {"username":"...", "provider":"Github|Google|Microsoft|Windows", "domain":"..."}
        /// Returns an empty payload if the input is null, empty, or malformed.
        /// </summary>
        public static SsoExtensionPayload DecodeSsoUserIdExtension(byte[] rawData)
        {
            if (rawData == null || rawData.Length == 0)
                return default;

            try
            {
                var reader = new AsnReader(rawData, AsnEncodingRules.DER);
                var json = reader.ReadCharacterString(UniversalTagNumber.UTF8String);
                reader.ThrowIfNotEmpty();

                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;

                if (root.TryGetProperty("username", out var usernameEl) == false ||
                    root.TryGetProperty("provider", out var providerEl) == false)
                    return default;

                var username = usernameEl.GetString();
                if (Enum.TryParse<SsoProvider>(providerEl.GetString(), ignoreCase: true, out var provider) == false)
                    return default;

                string domain = null;
                if (root.TryGetProperty("domain", out var domainEl))
                    domain = domainEl.GetString();

                // The values come straight out of a client-presented certificate extension whose RawData can be tens
                // of KB. Cap them: a crafted cert could otherwise carry a multi-MB identity that lives on the
                // connection for its lifetime, is written into every audit line, and is string-compared against every
                // SsoClient entry on each connection. No real provider identity needs anywhere near this.
                if (username != null && username.Length > MaxSsoIdentityFieldLength)
                    return default;
                if (domain != null && domain.Length > MaxSsoIdentityFieldLength)
                    return default;

                return new SsoExtensionPayload(username, provider, domain);
            }
            catch (Exception e) when (e is AsnContentException or JsonException or DecoderFallbackException or ArgumentException)
            {
                return default;
            }
        }
    }
}
