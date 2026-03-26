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
using Sparrow.Platform;

namespace FastTests;

public partial class RavenTestBase
{
    public readonly CertificatesTestBase Certificates;

    public class CertificatesTestBase
    {
        private static TestCertificatesHolder SelfSignedCertificates1Eku;
        private static TestCertificatesHolder SelfSignedCertificates2Eku;

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
                    serverCertificate = new X509Certificate2(certBytes, (string)null, X509KeyStorageFlags.MachineKeySet | CertificateLoaderUtil.FlagsForExport);
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
    }
}
