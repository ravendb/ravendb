using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Formats.Asn1;
using System.IO;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Properties;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Platform;

namespace FastTests;

public partial class RavenTestBase
{
    public readonly CertificatesTestBase Certificates;

    public class CertificatesTestBase
    {
        private static TestCertificatesHolder SelfSignedCertificates1Eku;
        private static TestCertificatesHolder SelfSignedCertificates2Eku;
        private static SsoTestCertificates CachedSsoTestCertificates;

        private static int Counter;

        private readonly RavenTestBase _parent;

        public CertificatesTestBase(RavenTestBase parent)
        {
            _parent = parent ?? throw new ArgumentNullException(nameof(parent));
        }

        public X509Certificate2 RegisterClientCertificate(TestCertificatesHolder certificates, Dictionary<string, DatabaseAccess> permissions, SecurityClearance clearance = SecurityClearance.ValidUser, RavenServer server = null)
        {
            return RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate1.Value, permissions, clearance, server);
        }

        public X509Certificate2 RegisterClientCertificate(
            X509Certificate2 serverCertificateForCommunication,
            X509Certificate2 clientCertificate,
            Dictionary<string, DatabaseAccess> permissions,
            SecurityClearance clearance = SecurityClearance.ValidUser,
            RavenServer server = null,
            string certificateName = "client certificate")
        {
            using var store = _parent.GetDocumentStore(new Options
            {
                CreateDatabase = false,
                Server = server,
                ClientCertificate = serverCertificateForCommunication,
                AdminCertificate = serverCertificateForCommunication,
                ModifyDocumentStore = s => s.Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true,
                    DisposeCertificate = false
                }
            });
            store.Maintenance.Server.Send(new PutClientCertificateOperation(certificateName, clientCertificate, permissions, clearance));
            return clientCertificate;
        }

        public TestCertificatesHolder SetupServerAuthentication(IDictionary<string, string> customSettings = null,
            string serverUrl = null,
            TestCertificatesHolder certificates = null,
            [CallerMemberName] string caller = null,
            bool with2Eku = true)
        {
            if (customSettings == null)
                customSettings = new ConcurrentDictionary<string, string>();

            if (certificates == null)
                certificates = GenerateAndSaveSelfSignedCertificate(caller: caller, with2Eku: with2Eku);

            if (customSettings.TryGetValue(RavenConfiguration.GetKey(x => x.Security.CertificateLoadExec), out var _) == false)
                customSettings[RavenConfiguration.GetKey(x => x.Security.CertificatePath)] = certificates.ServerCertificatePath;

            customSettings[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = serverUrl ?? (
                PlatformDetails.RunningOnMacOsx 
                ? "https://localhost:0" 
                : $"https://{Environment.MachineName}:0");

            _parent.DoNotReuseServer(customSettings);

            return certificates;
        }

        public TestCertificatesHolder GenerateAndSaveSelfSignedCertificate(bool createNew = false, [CallerMemberName] string caller = null, bool with2Eku = true)
        {
            if (createNew)
                return ReturnCertificatesHolder(Generate(caller, gen: Interlocked.Increment(ref Counter)));

            var selfSignedCertificates = with2Eku ? SelfSignedCertificates2Eku : SelfSignedCertificates1Eku;
            if (selfSignedCertificates != null)
                return ReturnCertificatesHolder(selfSignedCertificates);

            lock (typeof(TestBase))
            {
                selfSignedCertificates = with2Eku ? SelfSignedCertificates2Eku : SelfSignedCertificates1Eku;
                if (selfSignedCertificates == null)
                {
                    if (with2Eku)
                        SelfSignedCertificates2Eku = selfSignedCertificates = Generate(caller);
                    else
                        SelfSignedCertificates1Eku = selfSignedCertificates = Generate(caller);
                }

                return ReturnCertificatesHolder(selfSignedCertificates);
            }

            TestCertificatesHolder ReturnCertificatesHolder(TestCertificatesHolder certificates)
            {
                return new TestCertificatesHolder(certificates, _parent.GetTempFileName);
            }

            TestCertificatesHolder Generate(string caller, int gen = 0)
            {
                var log = new StringBuilder();
                byte[] certBytes;
                string ekuSuffix = with2Eku ? "2eku" : "1eku";

                var name = $"{Environment.MachineName}_{gen}_{RavenVersionAttribute.Instance.Build}_{DateTime.Today:yyyy-MM-dd}_{ekuSuffix}";
                var serverCertificatePath = Path.Combine(Path.GetTempPath(), $"{name}.pfx");

                if (File.Exists(serverCertificatePath) == false)
                {
                    try
                    {
                        var commonNameValue = name[..Math.Min(name.Length, 64)];
                        certBytes = CertificateUtils.CreateSelfSignedTestCertificate(commonNameValue, "RavenTestsServer", log, with2Eku);
                    }
                    catch (Exception e)
                    {
                        throw new CryptographicException($"Unable to generate the test certificate for the machine '{Environment.MachineName}'. Log: {log}", e);
                    }

                    if (certBytes.Length == 0)
                        throw new CryptographicException($"Test certificate length is 0 bytes. Machine: '{Environment.MachineName}', Log: {log}");

                    try
                    {
                        File.WriteAllBytes(serverCertificatePath, certBytes);
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException("Failed to write the test certificate to a temp file." +
                                                            $"tempFileName = {serverCertificatePath}" +
                                                            $"certBytes.Length = {certBytes.Length}" +
                                                            $"MachineName = {Environment.MachineName}.", e);
                    }
                }
                else
                {
                    certBytes = File.ReadAllBytes(serverCertificatePath);
                }

                X509Certificate2 serverCertificate;
                AsymmetricAlgorithm pk = null;
                try
                {
#pragma warning disable SYSLIB0057
                    serverCertificate = new X509Certificate2(certBytes, (string)null, X509KeyStorageFlags.MachineKeySet | CertificateLoaderUtil.FlagsForExport);
#pragma warning restore SYSLIB0057
                }
                catch (Exception e)
                {
                    throw new CryptographicException($"Unable to load the test certificate for the machine '{Environment.MachineName}'. Log: {log}", e);
                }
                if (PlatformDetails.RunningOnMacOsx)
                {
                    SecretProtection.ValidatePrivateKeyOnMacOs(serverCertificatePath,serverCertificate, out pk);
                }
                else
                {
                    SecretProtection.ValidatePrivateKey(serverCertificatePath, null, certBytes, out pk);
                }
                SecretProtection.ValidateServerKeyUsages(serverCertificatePath, serverCertificate, validateKeyUsages: true);

                string serverCertificateForCommunicationPath;
                if (SecretProtection.HasCertificateClientAuthEnhancedKeyUsage(serverCertificate) == false)
                {
                    serverCertificateForCommunicationPath = Path.Combine(Path.GetTempPath(), $"{Environment.MachineName}_SCC_{gen}_{RavenVersionAttribute.Instance.Build}_{DateTime.Today:yyyy-MM-dd}_{ekuSuffix}.pfx");
                    if (File.Exists(serverCertificateForCommunicationPath) == false)
                    {
                        byte[] serverClientCertBytes;
                        try
                        {
                            CertificateUtils.CreateClientCertificateFromServerCertificate(serverCertificate, out serverClientCertBytes);
                        }
                        catch (Exception e)
                        {
                            throw new CryptographicException($"Unable to generate the server certificate for communication for the machine '{Environment.MachineName}'. Log: {log}", e);
                        }

                        if (serverClientCertBytes.Length == 0)
                            throw new CryptographicException($"Test server certificate for communication length is 0 bytes. Machine: '{Environment.MachineName}', Log: {log}");

                        try
                        {
                            File.WriteAllBytes(serverCertificateForCommunicationPath, serverClientCertBytes);
                        }
                        catch (Exception e)
                        {
                            throw new InvalidOperationException("Failed to write the test certificate to a temp file." +
                                                                $"tempFileName = {serverCertificateForCommunicationPath}" +
                                                                $"certBytes.Length = {serverClientCertBytes.Length}" +
                                                                $"MachineName = {Environment.MachineName}.", e);
                        }
                    }
                }
                else
                {
                    serverCertificateForCommunicationPath = serverCertificatePath;
                }

                var clientCertificate1Path = GenerateClientCertificate(1, serverCertificate, pk, ekuSuffix, gen);
                var clientCertificate2Path = GenerateClientCertificate(2, serverCertificate, pk, ekuSuffix, gen);
                var clientCertificate3Path = GenerateClientCertificate(3, serverCertificate, pk, ekuSuffix, gen);
                
                pk?.Dispose();
                return new TestCertificatesHolder(serverCertificatePath, serverCertificateForCommunicationPath, clientCertificate1Path, clientCertificate2Path, clientCertificate3Path);
            }

            string GenerateClientCertificate(int index, X509Certificate2 serverCertificate, AsymmetricAlgorithm pk, string ekuSuffix, int gen)
            {
                string name = $"{Environment.MachineName}_CC_{gen}_{RavenVersionAttribute.Instance.Build}_{index}_{DateTime.Today:yyyy-MM-dd}_{ekuSuffix}";
                string clientCertificatePath = Path.Combine(Path.GetTempPath(), name + ".pfx");

                if (File.Exists(clientCertificatePath) == false)
                {
                    var commonNameValue = name[..Math.Min(name.Length, 64)];
                    CertificateUtils.CreateSelfSignedClientCertificate(
                        commonNameValue,
                        serverCertificate,
                        pk,
                        out var certBytes,
                        DateTime.UtcNow.Date.AddYears(5));

                    try
                    {
                        File.WriteAllBytes(clientCertificatePath, certBytes);
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException("Failed to write the test certificate to a temp file." +
                                                            $"tempFileName = {clientCertificatePath}" +
                                                            $"certBytes.Length = {certBytes.Length}" +
                                                            $"MachineName = {Environment.MachineName}.", e);
                    }
                }

                return clientCertificatePath;
            }
        }

        public SsoTestCertificates GenerateAndSaveSsoTestCertificates()
        {
            if (CachedSsoTestCertificates != null)
                return CachedSsoTestCertificates;

            lock (typeof(TestBase))
            {
                if (CachedSsoTestCertificates != null)
                    return CachedSsoTestCertificates;

                var name = $"{Environment.MachineName}_SSO_{RavenVersionAttribute.Instance.Build}_{DateTime.Today:yyyy-MM-dd}";
                var ssoServerCertPath = Path.Combine(Path.GetTempPath(), $"{name}.pfx");

                RSA ssoServerKey;
                X509Certificate2 ssoServerCert;

                if (File.Exists(ssoServerCertPath))
                {
                    var existingBytes = File.ReadAllBytes(ssoServerCertPath);
#pragma warning disable SYSLIB0057
                    ssoServerCert = new X509Certificate2(existingBytes, (string)null, X509KeyStorageFlags.MachineKeySet | CertificateLoaderUtil.FlagsForExport);
#pragma warning restore SYSLIB0057
                    ssoServerKey = ssoServerCert.GetRSAPrivateKey();
                }
                else
                {
                    ssoServerKey = RSA.Create(4096);

                    var commonNameValue = name[..Math.Min(name.Length, 64)];
                    var subjectName = new X500DistinguishedName($"CN={commonNameValue}");

                    CertificateUtils.CreateSelfSignedCertificateBasedOnPrivateKey(
                        commonNameValue: commonNameValue,
                        issuerCN: subjectName,
                        issuerKeyPair: (ssoServerKey, ssoServerKey),
                        isClientCertificate: false,
                        isCaCertificate: true,
                        notAfter: DateTime.UtcNow.AddMonths(3),
                        certBytes: out var certBytes,
                        subjectPrivateKey: ssoServerKey,
                        with2Eku: false);

#pragma warning disable SYSLIB0057
                    ssoServerCert = new X509Certificate2(certBytes, (string)null, X509KeyStorageFlags.MachineKeySet | CertificateLoaderUtil.FlagsForExport);
#pragma warning restore SYSLIB0057
                    ssoServerKey = ssoServerCert.GetRSAPrivateKey();

                    File.WriteAllBytes(ssoServerCertPath, certBytes);
                }

                var publicCertBytes = ssoServerCert.Export(X509ContentType.Cert);
                var pinningHash = ssoServerCert.GetPublicKeyPinningHash();

                CachedSsoTestCertificates = new SsoTestCertificates(
                    ssoServerCert,
                    ssoServerKey,
                    pinningHash,
                    Convert.ToBase64String(publicCertBytes));

                return CachedSsoTestCertificates;
            }
        }

        public X509Certificate2 CreateSsoUserCertificate(SsoTestCertificates ssoCerts, string ssoUserId, SsoProvider provider = SsoProvider.Github, string domain = null)
        {
            const string ssoUserIdExtensionOid = "1.3.6.1.4.1.45751.2.2";

            using var userKey = RSA.Create(2048);
            var subjectName = new X500DistinguishedName($"CN=SSO User {ssoUserId}");

            var request = new CertificateRequest(subjectName, userKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);

            request.CertificateExtensions.Add(new X509KeyUsageExtension(
                X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, false));

            request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
                new OidCollection { new Oid("1.3.6.1.5.5.7.3.2") }, false));

            // Add the SSO user ID custom OID extension, DER-encoded as a UTF8String containing JSON —
            // matches the format parsed by CertificateUtils.DecodeSsoUserIdExtension.
            var jsonPayload = BuildSsoUserIdJsonPayload(ssoUserId, provider, domain);
            var asnWriter = new AsnWriter(AsnEncodingRules.DER);
            asnWriter.WriteCharacterString(UniversalTagNumber.UTF8String, jsonPayload);
            request.CertificateExtensions.Add(new X509Extension(new Oid(ssoUserIdExtensionOid), asnWriter.Encode(), false));

            byte[] serialNumber = new byte[20];
            using (var rng = RandomNumberGenerator.Create())
                rng.GetBytes(serialNumber);
            serialNumber[0] &= 0x7F;

            var signatureGenerator = X509SignatureGenerator.CreateForRSA(
                (RSA)ssoCerts.SsoServerPrivateKey, RSASignaturePadding.Pkcs1);

            var cert = request.Create(
                ssoCerts.SsoServerCert.SubjectName,
                signatureGenerator,
                DateTimeOffset.UtcNow.AddDays(-1),
                DateTimeOffset.UtcNow.AddYears(1),
                serialNumber);

            // Combine with private key and return
            var certWithKey = cert.CopyWithPrivateKey(userKey);
            var pfxBytes = certWithKey.Export(X509ContentType.Pfx, string.Empty);
            return CertificateLoaderUtil.CreateCertificate(pfxBytes, flags: CertificateLoaderUtil.FlagsForPersist);
        }

        private static string BuildSsoUserIdJsonPayload(string username, SsoProvider provider, string domain)
        {
            // Use a JSON writer so values are properly escaped (mirrors the production decoder which uses System.Text.Json).
            using var stream = new MemoryStream();
            using (var writer = new System.Text.Json.Utf8JsonWriter(stream))
            {
                writer.WriteStartObject();
                writer.WriteString("username", username);
                writer.WriteString("provider", provider.ToString());
                if (provider == SsoProvider.Windows && string.IsNullOrEmpty(domain) == false)
                    writer.WriteString("domain", domain);
                writer.WriteEndObject();
            }
            return Encoding.UTF8.GetString(stream.ToArray());
        }

        public void RegisterSsoServerCert(TestCertificatesHolder certificates, SsoTestCertificates ssoCerts, string name = "SSO Server Certificate", RavenServer server = null)
        {
            RegisterSsoServerCertCore(certificates, ssoCerts.SsoServerCertBase64, name, server);
        }

        public void RegisterSsoServerCert(TestCertificatesHolder certificates, X509Certificate2 cert, string name = "SSO Server Certificate", RavenServer server = null)
        {
            RegisterSsoServerCertCore(certificates, Convert.ToBase64String(cert.Export(X509ContentType.Cert)), name, server);
        }

        private void RegisterSsoServerCertCore(TestCertificatesHolder certificates, string certBase64, string name, RavenServer server)
        {
            using var store = _parent.GetDocumentStore(new Options
            {
                CreateDatabase = false,
                Server = server,
                ClientCertificate = certificates.ServerCertificateForCommunication.Value,
                AdminCertificate = certificates.ServerCertificateForCommunication.Value,
                ModifyDocumentStore = s => s.Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true,
                    DisposeCertificate = false
                }
            });
            var requestExecutor = store.GetRequestExecutor();
            using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var command = new PutSsoServerCertCommand(store.Conventions, name, certBase64);
                requestExecutor.Execute(command, context);
            }
        }

        public string RegisterSsoUserEntry(TestCertificatesHolder certificates, string ssoUserId, string ssoServerPublicKeyPinningHash,
            Dictionary<string, DatabaseAccess> permissions, SecurityClearance clearance = SecurityClearance.ValidUser, RavenServer server = null,
            SsoProvider provider = SsoProvider.Github, string domain = null)
        {
            return RegisterSsoUserEntry(certificates, ssoUserId, ssoServerPublicKeyPinningHash, allowAnySsoServer: false, permissions, clearance, server, provider, domain);
        }

        public string RegisterSsoUserEntry(TestCertificatesHolder certificates, string ssoUserId, string ssoServerPublicKeyPinningHash,
            bool allowAnySsoServer, Dictionary<string, DatabaseAccess> permissions, SecurityClearance clearance = SecurityClearance.ValidUser, RavenServer server = null,
            SsoProvider provider = SsoProvider.Github, string domain = null)
        {
            using var store = _parent.GetDocumentStore(new Options
            {
                CreateDatabase = false,
                Server = server,
                ClientCertificate = certificates.ServerCertificateForCommunication.Value,
                AdminCertificate = certificates.ServerCertificateForCommunication.Value,
                ModifyDocumentStore = s => s.Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true,
                    DisposeCertificate = false
                }
            });
            var requestExecutor = store.GetRequestExecutor();
            using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var command = new PutSsoUserEntryCommand(store.Conventions, ssoUserId, ssoServerPublicKeyPinningHash, allowAnySsoServer, permissions, clearance, provider, domain);
                requestExecutor.Execute(command, context);
                return command.GeneratedThumbprint;
            }
        }

        private sealed class PutSsoServerCertCommand : RavenCommand, IRaftCommand
        {
            private readonly DocumentConventions _conventions;
            private readonly string _name;
            private readonly string _certBase64;

            public PutSsoServerCertCommand(DocumentConventions conventions, string name, string certBase64)
            {
                _conventions = conventions;
                _name = name;
                _certBase64 = certBase64;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/certificates?raftRequestId={RaftUniqueRequestId}";
                return new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(async stream =>
                    {
                        await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                        {
                            writer.WriteStartObject();
                            writer.WritePropertyName(nameof(CertificateDefinition.Name));
                            writer.WriteString(_name);
                            writer.WriteComma();
                            writer.WritePropertyName(nameof(CertificateDefinition.Certificate));
                            writer.WriteString(_certBase64);
                            writer.WriteComma();
                            writer.WritePropertyName(nameof(CertificateDefinition.SecurityClearance));
                            writer.WriteString(SecurityClearance.ValidUser.ToString());
                            writer.WriteComma();
                            writer.WritePropertyName(nameof(CertificateDefinition.Usage));
                            writer.WriteString(CertificateUsage.SsoServer.ToString());
                            writer.WriteComma();
                            writer.WritePropertyName(nameof(CertificateDefinition.Permissions));
                            writer.WriteStartObject();
                            writer.WriteEndObject();
                            writer.WriteEndObject();
                        }
                    }, _conventions)
                };
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }

        private sealed class PutSsoUserEntryCommand : RavenCommand, IRaftCommand
        {
            private readonly DocumentConventions _conventions;
            private readonly string _ssoUserId;
            private readonly List<string> _ssoServerPublicKeyPinningHashes;
            private readonly bool _allowAnySsoServer;
            private readonly Dictionary<string, DatabaseAccess> _permissions;
            private readonly SecurityClearance _clearance;
            private readonly SsoProvider _provider;
            private readonly string _domain;

            public string GeneratedThumbprint { get; private set; }

            public PutSsoUserEntryCommand(DocumentConventions conventions, string ssoUserId, string ssoServerPublicKeyPinningHash,
                bool allowAnySsoServer, Dictionary<string, DatabaseAccess> permissions, SecurityClearance clearance,
                SsoProvider provider, string domain)
            {
                _conventions = conventions;
                _ssoUserId = ssoUserId;
                _ssoServerPublicKeyPinningHashes = ssoServerPublicKeyPinningHash != null ? [ssoServerPublicKeyPinningHash] : [];
                _allowAnySsoServer = allowAnySsoServer;
                _permissions = permissions;
                _clearance = clearance;
                _provider = provider;
                _domain = domain;
                ResponseType = RavenCommandResponseType.Object;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/certificates/sso/user?raftRequestId={RaftUniqueRequestId}";
                return new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(async stream =>
                    {
                        await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                        {
                            writer.WriteStartObject();
                            writer.WritePropertyName(nameof(CertificateDefinition.Name));
                            writer.WriteString(_ssoUserId);
                            writer.WriteComma();
                            writer.WritePropertyName(nameof(CertificateDefinition.Usage));
                            writer.WriteString(CertificateUsage.SsoClient.ToString());
                            writer.WriteComma();
                            writer.WritePropertyName(nameof(CertificateDefinition.SsoIdentifiers));
                            writer.WriteStartArray();
                            writer.WriteStartObject();
                            writer.WritePropertyName(nameof(SsoIdentifier.Provider));
                            writer.WriteString(_provider.ToString());
                            writer.WriteComma();
                            writer.WritePropertyName(nameof(SsoIdentifier.Identifier));
                            writer.WriteString(_ssoUserId);
                            if (string.IsNullOrEmpty(_domain) == false)
                            {
                                writer.WriteComma();
                                writer.WritePropertyName(nameof(SsoIdentifier.Domain));
                                writer.WriteString(_domain);
                            }
                            writer.WriteEndObject();
                            writer.WriteEndArray();
                            writer.WriteComma();
                            writer.WriteArray(nameof(CertificateDefinition.SsoServerPublicKeyPinningHashes), _ssoServerPublicKeyPinningHashes);
                            writer.WriteComma();
                            writer.WritePropertyName(nameof(CertificateDefinition.AllowAnySsoServer));
                            writer.WriteBool(_allowAnySsoServer);
                            writer.WriteComma();
                            writer.WritePropertyName(nameof(CertificateDefinition.SecurityClearance));
                            writer.WriteString(_clearance.ToString());
                            writer.WriteComma();
                            writer.WritePropertyName(nameof(CertificateDefinition.Permissions));
                            writer.WriteStartObject();
                            var first = true;
                            foreach (var kvp in _permissions)
                            {
                                if (first == false)
                                    writer.WriteComma();
                                first = false;
                                writer.WriteString(kvp.Key);
                                writer.WriteComma();
                                writer.WriteString(kvp.Value.ToString());
                            }
                            writer.WriteEndObject();
                            writer.WriteEndObject();
                        }
                    }, _conventions)
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    return;
                if (response.TryGet(nameof(CertificateDefinition.Thumbprint), out string thumbprint))
                    GeneratedThumbprint = thumbprint;
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }

    public sealed class SsoTestCertificates
    {
        public X509Certificate2 SsoServerCert { get; }
        public AsymmetricAlgorithm SsoServerPrivateKey { get; }
        public string SsoServerPublicKeyPinningHash { get; }
        public string SsoServerCertBase64 { get; }

        public SsoTestCertificates(X509Certificate2 ssoServerCert, AsymmetricAlgorithm ssoServerPrivateKey, string ssoServerPublicKeyPinningHash, string ssoServerCertBase64)
        {
            SsoServerCert = ssoServerCert;
            SsoServerPrivateKey = ssoServerPrivateKey;
            SsoServerPublicKeyPinningHash = ssoServerPublicKeyPinningHash;
            SsoServerCertBase64 = ssoServerCertBase64;
        }
    }
}
