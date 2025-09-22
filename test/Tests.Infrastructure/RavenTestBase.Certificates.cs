using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using Raven.Client.Documents.Conventions;
using Raven.Client.Properties;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.ServerWide;
using Raven.Server.Utils;

namespace FastTests;

public partial class RavenTestBase
{
    public readonly CertificatesTestBase Certificates;

    public class CertificatesTestBase
    {
        private static TestCertificatesHolder SelfSignedCertificates;

        private static int Counter;

        private readonly RavenTestBase _parent;

        public CertificatesTestBase(RavenTestBase parent)
        {
            _parent = parent ?? throw new ArgumentNullException(nameof(parent));
        }

        public X509Certificate2 RegisterClientCertificate(TestCertificatesHolder certificates, Dictionary<string, DatabaseAccess> permissions, SecurityClearance clearance = SecurityClearance.ValidUser, RavenServer server = null)
        {
            return RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, permissions, clearance, server);
        }

        public X509Certificate2 RegisterClientCertificate(
            X509Certificate2 serverCertificate,
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
                ClientCertificate = serverCertificate,
                AdminCertificate = serverCertificate,
                ModifyDocumentStore = s => s.Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true,
                    DisposeCertificate = false
                }
            });
            store.Maintenance.Server.Send(new PutClientCertificateOperation(certificateName, clientCertificate, permissions, clearance));
            return clientCertificate;
        }

        public TestCertificatesHolder SetupServerAuthentication(IDictionary<string, string> customSettings = null, string serverUrl = null, TestCertificatesHolder certificates = null, [CallerMemberName] string caller = null)
        {
            if (customSettings == null)
                customSettings = new ConcurrentDictionary<string, string>();

            if (certificates == null)
                certificates = GenerateAndSaveSelfSignedCertificate(caller: caller);

            if (customSettings.TryGetValue(RavenConfiguration.GetKey(x => x.Security.CertificateLoadExec), out var _) == false)
                customSettings[RavenConfiguration.GetKey(x => x.Security.CertificatePath)] = certificates.ServerCertificatePath;

            customSettings[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = serverUrl ?? "https://" + Environment.MachineName + ":0";

            _parent.DoNotReuseServer(customSettings);

            return certificates;
        }

        public TestCertificatesHolder GenerateAndSaveSelfSignedCertificate(bool createNew = false, [CallerMemberName] string caller = null)
        {
            if (createNew)
                return ReturnCertificatesHolder(Generate(caller, Interlocked.Increment(ref Counter)));

            var selfSignedCertificates = SelfSignedCertificates;
            if (selfSignedCertificates != null)
                return ReturnCertificatesHolder(selfSignedCertificates);

            lock (typeof(TestBase))
            {
                selfSignedCertificates = SelfSignedCertificates;
                if (selfSignedCertificates == null)
                    SelfSignedCertificates = selfSignedCertificates = Generate(caller);

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
                string serverCertificatePath = null;

                serverCertificatePath = Path.Combine(Path.GetTempPath(), $"Server-{gen}-{RavenVersionAttribute.Instance.Build}-{DateTime.Today:yyyy-MM-dd}.pfx");

                if (File.Exists(serverCertificatePath) == false)
                {
                    try
                    {
                        certBytes = CertificateUtils.CreateSelfSignedTestCertificate(Environment.MachineName, "RavenTestsServer", log);
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
                try
                {
                    serverCertificate = CertificateHelper.CreateCertificateFromPfx(certBytes, (string)null, X509KeyStorageFlags.UserKeySet);
                }
                catch (Exception e)
                {
                    throw new CryptographicException($"Unable to load the test certificate for the machine '{Environment.MachineName}'. Log: {log}", e);
                }

                SecretProtection.ValidatePrivateKey(serverCertificatePath, null, certBytes, out var pk);
                SecretProtection.ValidateKeyUsages(serverCertificatePath, serverCertificate, validateKeyUsages: true);

                var clientCertificate1Path = GenerateClientCertificate(1, serverCertificate, pk);
                var clientCertificate2Path = GenerateClientCertificate(2, serverCertificate, pk);
                var clientCertificate3Path = GenerateClientCertificate(3, serverCertificate, pk);

                return new TestCertificatesHolder(serverCertificatePath, clientCertificate1Path, clientCertificate2Path, clientCertificate3Path);
            }

            string GenerateClientCertificate(int index, X509Certificate2 serverCertificate, Org.BouncyCastle.Pkcs.AsymmetricKeyEntry pk)
            {
                string name = $"{Environment.MachineName}_CC_{RavenVersionAttribute.Instance.Build}_{index}_{DateTime.Today:yyyy-MM-dd}";
                string clientCertificatePath = Path.Combine(Path.GetTempPath(), name + ".pfx");

                if (File.Exists(clientCertificatePath) == false)
                {
                    CertificateUtils.CreateSelfSignedClientCertificate(
                        name,
                        new CertificateUtils.CertificateHolder(serverCertificate, pk),
                        out var certBytes, DateTime.UtcNow.Date.AddYears(5));

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
    }
}
