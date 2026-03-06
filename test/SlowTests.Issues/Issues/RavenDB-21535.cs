using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Raven.Client;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_21535 : DisableParallelTestBase
    {
        public RavenDB_21535(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Certificates)]
        public void KnownIssuerCert_CanNotAccess_WithoutSAN()
        {
            var log = new StringBuilder();
            string caBase64 = GenerateCaAndServerCert(out byte[] clientCertBytes, out var caCertBytes, log: log);

            var client = CertificateLoaderUtil.CreateCertificate(clientCertBytes);

            using var server = GetNewServer(new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Security.WellKnownIssuers)] = caBase64,
                    [RavenConfiguration.GetKey(x => x.Security.ValidateSanForCertificateWithWellKnownIssuer)] = true.ToString(),
                    [RavenConfiguration.GetKey(x => x.Core.PublicServerUrl)] = $"http://{LocalDomainName}",
                }
            });

            var result = server.AuthenticateConnectionCertificate(client, null, log);
            if (result.Status != RavenServer.AuthenticationStatus.UnfamiliarCertificate)
            {
                SaveCertificatesToTempPath(caCertBytes, clientCertBytes, log);
            }

            Assert.True(RavenServer.AuthenticationStatus.UnfamiliarCertificate == result.Status, $"Expected: {RavenServer.AuthenticationStatus.UnfamiliarCertificate} but got: {result.Status}. Log: {log}");
        }

        [RavenTheory(RavenTestCategory.Certificates)]
        [InlineData("a.localhost", "*.localhost")]
        [InlineData("c.localhost", "c.localhost")]
        [InlineData("test.localhost", "*.localhost")]
        [InlineData("longdomainname.localhost", "*.localhost")]
        [InlineData("localhost", "localhost")]
        public void KnownIssuerCert_CanAccess_WithValidSAN(string publicDomain, string san)
        {
            var log = new StringBuilder();
            string caBase64 = GenerateCaAndServerCert(out byte[] clientCertBytes, out var caCertBytes, [san], log);

            var client = CertificateLoaderUtil.CreateCertificate(clientCertBytes);

            using var server = GetNewServer(new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Security.WellKnownIssuers)] = caBase64,
                    [RavenConfiguration.GetKey(x => x.Security.ValidateSanForCertificateWithWellKnownIssuer)] = true.ToString(),
                    [RavenConfiguration.GetKey(x => x.Core.PublicServerUrl)] = $"http://{publicDomain}",
                }
            });

            var result = server.AuthenticateConnectionCertificate(client, null, log);
            if (result.Status != RavenServer.AuthenticationStatus.ClusterAdmin)
            {
                SaveCertificatesToTempPath(caCertBytes, clientCertBytes, log);
            }

            Assert.True(RavenServer.AuthenticationStatus.ClusterAdmin == result.Status, $"Expected: {RavenServer.AuthenticationStatus.ClusterAdmin} but got: {result.Status}. Log: {log}");
        }

        [RavenTheory(RavenTestCategory.Certificates)]
        [InlineData("a.b.localhost", "*.localhost")]
        [InlineData("a.localhost", "*.a.localhost")]
        [InlineData("aaa.localhost", "bbb.localhost")]
        [InlineData("aaa.localhost.bbb", "aaa.localhost")]
        [InlineData("aaa.localhost", "aaa.localhost.bbb")]
        public void KnownIssuerCert_CanNotAccess_WithInvalidSAN(string publicDomain, string san)
        {
            var log = new StringBuilder();
            string caBase64 = GenerateCaAndServerCert(out byte[] clientCertBytes, out var caCertBytes, [san], log);

            var client = CertificateLoaderUtil.CreateCertificate(clientCertBytes);

            using var server = GetNewServer(new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Security.WellKnownIssuers)] = caBase64,
                    [RavenConfiguration.GetKey(x => x.Security.ValidateSanForCertificateWithWellKnownIssuer)] = true.ToString(),
                    [RavenConfiguration.GetKey(x => x.Core.PublicServerUrl)] = $"http://{publicDomain}",
                }
            });

            var result = server.AuthenticateConnectionCertificate(client, null, log);
            if (result.Status != RavenServer.AuthenticationStatus.UnfamiliarCertificate)
            {
                SaveCertificatesToTempPath(caCertBytes, clientCertBytes, log);
            }

            Assert.True(RavenServer.AuthenticationStatus.UnfamiliarCertificate == result.Status, $"Expected: {RavenServer.AuthenticationStatus.UnfamiliarCertificate} but got: {result.Status}. Log: {log}");
        }

        [RavenFact(RavenTestCategory.Certificates)]
        public void KnownIssuerCert_CanAccess_WhenSANValidation_IsDisabled_AndNotMatchingServerDomainName()
        {
            var log = new StringBuilder();
            string caBase64 = GenerateCaAndServerCert(out byte[] clientCertBytes, out var caCertBytes, [LocalDomainName], log);

            var client = CertificateLoaderUtil.CreateCertificate(clientCertBytes);

            using var server = GetNewServer(new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Security.WellKnownIssuers)] = caBase64,
                    [RavenConfiguration.GetKey(x => x.Core.PublicServerUrl)] = $"http://a.{LocalDomainName}",
                }
            });

            var result = server.AuthenticateConnectionCertificate(client, null, log);
            if (result.Status != RavenServer.AuthenticationStatus.ClusterAdmin)
            {
                SaveCertificatesToTempPath(caCertBytes, clientCertBytes, log);
            }

            Assert.True(RavenServer.AuthenticationStatus.ClusterAdmin == result.Status, $"Expected: {RavenServer.AuthenticationStatus.ClusterAdmin} but got: {result.Status}. Log: {log}");
        }

        private static string GenerateCaAndServerCert(out byte[] clientCertBytes, out byte[] caCertBytes, string[] sans = null, StringBuilder log = null)
        {
            var suffix = Guid.NewGuid().ToString().Split('-')[0];

            var caCommonNameValue = $"ca-{suffix}";
            var ca = CertificateUtils.CreateCertificateAuthorityCertificate(caCommonNameValue, out _, generateNewKeyPair: true);
            var caBase64 = Convert.ToBase64String(ca.Export(X509ContentType.Cert));
            log?.AppendLine($"Created CA: {ca.GetDisplayName()} ({ca.Thumbprint})");

            CertificateUtils.CreateSelfSignedCertificateBasedOnPrivateKey(
                commonNameValue: $"admin-{suffix}",
                issuerCN: ca.SubjectName,
                issuerKeyPair: (ca.GetExportableRsaPrivateKey(), ca.GetRSAPublicKey()),
                isClientCertificate: true,
                isCaCertificate: false,
                notAfter: DateTime.UtcNow.Date.AddMonths(1),
                out clientCertBytes,
                sans: sans);
            var adminCert = CertificateLoaderUtil.CreateCertificate(clientCertBytes);
            log?.AppendLine($"Created admin cert: {adminCert.GetDisplayName()} ({adminCert.Thumbprint})");

            CertificateUtils.RemoveOldTestCertificatesFromOsStore(caCommonNameValue);
            caCertBytes = ca.Export(X509ContentType.Pfx);
            return caBase64;
        }

        private static void SaveCertificatesToTempPath(byte[] caCertBytes, byte[] clientCertBytes, StringBuilder log)
        {
            var tempPath = Path.GetTempPath();
            var path = Path.Combine(tempPath, Path.GetTempFileName());
            log.AppendLine($"Saving CA pfx to {path}");
            File.WriteAllBytes(path, caCertBytes);

            path = Path.Combine(tempPath, Path.GetTempFileName());
            log.AppendLine($"Saving client pfx to {path}");
            File.WriteAllBytes(path, clientCertBytes);
        }

        private const string LocalDomainName = "localhost";
    }
}
