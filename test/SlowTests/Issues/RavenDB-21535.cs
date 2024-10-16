using System;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using Raven.Server;
using Raven.Server.Config;
using Tests.Infrastructure;
using Tests.Infrastructure.Utils;
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
            var caKeyPair = CertificateGenerator.GenerateRSAKeyPair();
            var ca = CertificateGenerator.GenerateRootCACertificate("ca", 2, caKeyPair);
            var caBase64 = Convert.ToBase64String(ca.Export(X509ContentType.Cert));

            var clientKeyPair = CertificateGenerator.GenerateRSAKeyPair();
            var client = CertificateGenerator.GenerateSignedClientCertificate(ca, caKeyPair, LocalDomainName, 1, clientKeyPair, []);

            var server = GetNewServer(new ServerCreationOptions
                {
                    CustomSettings = new Dictionary<string, string>
                    {
                        [RavenConfiguration.GetKey(x => x.Security.WellKnownIssuers)] = caBase64,
                        [RavenConfiguration.GetKey(x => x.Security.ValidateSanForCertificateWithWellKnownIssuer)] = true.ToString(),
                        [RavenConfiguration.GetKey(x => x.Core.PublicServerUrl)] = $"http://{LocalDomainName}",
                    }
                }
            );

            var result = server.AuthenticateConnectionCertificate(client, null);
            Assert.Equal(RavenServer.AuthenticationStatus.UnfamiliarCertificate, result.Status);
        }

        [RavenTheory(RavenTestCategory.Certificates)]
        [InlineData($"a.{LocalDomainName}", $"*.{LocalDomainName}")]
        [InlineData($"c.{LocalDomainName}", $"c.{LocalDomainName}")]
        [InlineData($"test.{LocalDomainName}", $"*.{LocalDomainName}")]
        [InlineData($"longdomainname.{LocalDomainName}", $"*.{LocalDomainName}")]
        [InlineData($"{LocalDomainName}", $"{LocalDomainName}")]
        public void KnownIssuerCert_CanAccess_WithValidSAN(string publicDomain, string san)
        {
            var caKeyPair = CertificateGenerator.GenerateRSAKeyPair();
            var ca = CertificateGenerator.GenerateRootCACertificate("ca", 2, caKeyPair);
            var caBase64 = Convert.ToBase64String(ca.Export(X509ContentType.Cert));

            var clientKeyPair = CertificateGenerator.GenerateRSAKeyPair();
            var client = CertificateGenerator.GenerateSignedClientCertificate(ca, caKeyPair, "admin", 1, clientKeyPair, [san]);

            var server = GetNewServer(new ServerCreationOptions
                {
                    CustomSettings = new Dictionary<string, string>
                    {
                        [RavenConfiguration.GetKey(x => x.Security.WellKnownIssuers)] = caBase64,
                        [RavenConfiguration.GetKey(x => x.Security.ValidateSanForCertificateWithWellKnownIssuer)] = true.ToString(),
                        [RavenConfiguration.GetKey(x => x.Core.PublicServerUrl)] = $"http://{publicDomain}",
                    }
                }
            );

            var result = server.AuthenticateConnectionCertificate(client, null);
            Assert.Equal(RavenServer.AuthenticationStatus.ClusterAdmin, result.Status);
        }

        [RavenTheory(RavenTestCategory.Certificates)]
        [InlineData($"a.b.{LocalDomainName}", $"*.{LocalDomainName}")]
        [InlineData($"a.{LocalDomainName}", $"*.a.{LocalDomainName}")]
        [InlineData($"aaa.{LocalDomainName}", $"bbb.{LocalDomainName}")]
        [InlineData($"aaa.{LocalDomainName}.bbb", $"aaa.{LocalDomainName}")]
        [InlineData($"aaa.{LocalDomainName}", $"aaa.{LocalDomainName}.bbb")]
        public void KnownIssuerCert_CanNotAccess_WithInvalidSAN(string publicDomain, string san)
        {
            var caKeyPair = CertificateGenerator.GenerateRSAKeyPair();
            var ca = CertificateGenerator.GenerateRootCACertificate("ca", 2, caKeyPair);
            var caBase64 = Convert.ToBase64String(ca.Export(X509ContentType.Cert));

            var clientKeyPair = CertificateGenerator.GenerateRSAKeyPair();
            var client = CertificateGenerator.GenerateSignedClientCertificate(ca, caKeyPair, "admin", 1, clientKeyPair, [san]);

            var server = GetNewServer(new ServerCreationOptions
                {
                    CustomSettings = new Dictionary<string, string>
                    {
                        [RavenConfiguration.GetKey(x => x.Security.WellKnownIssuers)] = caBase64,
                        [RavenConfiguration.GetKey(x => x.Security.ValidateSanForCertificateWithWellKnownIssuer)] = true.ToString(),
                        [RavenConfiguration.GetKey(x => x.Core.PublicServerUrl)] = $"http://{publicDomain}",
                    }
                }
            );

            var result = server.AuthenticateConnectionCertificate(client, null);
            Assert.Equal(RavenServer.AuthenticationStatus.UnfamiliarCertificate, result.Status);
        }

        [RavenFact(RavenTestCategory.Certificates)]
        public void KnownIssuerCert_CanAccess_WhenSANValidation_IsDisabled_AndNotMatchingServerDomainName()
        {
            var caKeyPair = CertificateGenerator.GenerateRSAKeyPair();
            var ca = CertificateGenerator.GenerateRootCACertificate("ca", 2, caKeyPair);
            var caBase64 = Convert.ToBase64String(ca.Export(X509ContentType.Cert));

            var clientKeyPair = CertificateGenerator.GenerateRSAKeyPair();
            var client = CertificateGenerator.GenerateSignedClientCertificate(ca, caKeyPair, "admin", 1, clientKeyPair, [LocalDomainName]);

            var server = GetNewServer(new ServerCreationOptions
                {
                    CustomSettings = new Dictionary<string, string>
                    {
                        [RavenConfiguration.GetKey(x => x.Security.WellKnownIssuers)] = caBase64,
                        [RavenConfiguration.GetKey(x => x.Core.PublicServerUrl)] = $"http://a.{LocalDomainName}",
                    }
                }
            );

            var result = server.AuthenticateConnectionCertificate(client, null);
            Assert.Equal(RavenServer.AuthenticationStatus.ClusterAdmin, result.Status);
        }

        private const string LocalDomainName = "localhost";
    }
}
