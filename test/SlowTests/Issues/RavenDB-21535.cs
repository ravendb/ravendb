using System;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_21535 : ClusterTestBase
    {
        public RavenDB_21535(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Certificates)]
        public void KnownIssuerCert_CanNotAccess_WithoutSAN()
        {
            string caBase64 = GenerateCaAndServerCert(out byte[] clientCertBytes);

            var client = new X509Certificate2(clientCertBytes);

            var server = GetNewServer(new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Security.WellKnownIssuers)] = caBase64,
                    [RavenConfiguration.GetKey(x => x.Security.ValidateSanForCertificateWithWellKnownIssuer)] = true.ToString(),
                    [RavenConfiguration.GetKey(x => x.Core.PublicServerUrl)] = $"http://{LocalDomainName}",
                }
            });

            var result = server.AuthenticateConnectionCertificate(client, null);
            Assert.Equal(RavenServer.AuthenticationStatus.UnfamiliarCertificate, result.Status);
        }

        [RavenTheory(RavenTestCategory.Certificates)]
        [InlineData("a.localhost", "*.localhost")]
        [InlineData("c.localhost", "c.localhost")]
        [InlineData("test.localhost", "*.localhost")]
        [InlineData("longdomainname.localhost", "*.localhost")]
        [InlineData("localhost", "localhost")]
        public void KnownIssuerCert_CanAccess_WithValidSAN(string publicDomain, string san)
        {
            string caBase64 = GenerateCaAndServerCert(out byte[] clientCertBytes, san);

            var client = new X509Certificate2(clientCertBytes);

            var server = GetNewServer(new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Security.WellKnownIssuers)] = caBase64,
                    [RavenConfiguration.GetKey(x => x.Security.ValidateSanForCertificateWithWellKnownIssuer)] = true.ToString(),
                    [RavenConfiguration.GetKey(x => x.Core.PublicServerUrl)] = $"http://{publicDomain}",
                }
            });

            var result = server.AuthenticateConnectionCertificate(client, null);
            Assert.Equal(RavenServer.AuthenticationStatus.ClusterAdmin, result.Status);
        }

        [RavenTheory(RavenTestCategory.Certificates)]
        [InlineData("a.b.localhost", "*.localhost")]
        [InlineData("a.localhost", "*.a.localhost")]
        [InlineData("aaa.localhost", "bbb.localhost")]
        [InlineData("aaa.localhost.bbb", "aaa.localhost")]
        [InlineData("aaa.localhost", "aaa.localhost.bbb")]
        public void KnownIssuerCert_CanNotAccess_WithInvalidSAN(string publicDomain, string san)
        {
            string caBase64 = GenerateCaAndServerCert(out byte[] clientCertBytes, san);

            var client = new X509Certificate2(clientCertBytes);

            var server = GetNewServer(new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Security.WellKnownIssuers)] = caBase64,
                    [RavenConfiguration.GetKey(x => x.Security.ValidateSanForCertificateWithWellKnownIssuer)] = true.ToString(),
                    [RavenConfiguration.GetKey(x => x.Core.PublicServerUrl)] = $"http://{publicDomain}",
                }
            });

            var result = server.AuthenticateConnectionCertificate(client, null);
            Assert.Equal(RavenServer.AuthenticationStatus.UnfamiliarCertificate, result.Status);
        }

        [RavenFact(RavenTestCategory.Certificates)]
        public void KnownIssuerCert_CanAccess_WhenSANValidation_IsDisabled_AndNotMatchingServerDomainName()
        {
            string caBase64 = GenerateCaAndServerCert(out byte[] clientCertBytes, LocalDomainName);

            var client = new X509Certificate2(clientCertBytes);

            var server = GetNewServer(new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Security.WellKnownIssuers)] = caBase64,
                    [RavenConfiguration.GetKey(x => x.Core.PublicServerUrl)] = $"http://a.{LocalDomainName}",
                }
            });

            var result = server.AuthenticateConnectionCertificate(client, null);
            Assert.Equal(RavenServer.AuthenticationStatus.ClusterAdmin, result.Status);
        }

        private static string GenerateCaAndServerCert(out byte[] clientCertBytes, params string[] sans)
        {
            var suffix = Guid.NewGuid().ToString().Split('-')[0];
            
            var ca = CertificateUtils.CreateCertificateAuthorityCertificate($"ca-{suffix}", out var caKeyPair, out _);
            var caBase64 = Convert.ToBase64String(ca.Export(X509ContentType.Cert));

            CertificateUtils.CreateSelfSignedCertificateBasedOnPrivateKey(
                commonNameValue: $"admin-{suffix}",
                issuerCN: ca.SubjectName,
                issuerKeyPair: caKeyPair,
                isClientCertificate: true,
                isCaCertificate: false,
                notAfter: DateTime.UtcNow.Date.AddMonths(1),
                out clientCertBytes,
                sans: sans);
            return caBase64;
        }

        private const string LocalDomainName = "localhost";
    }
}
