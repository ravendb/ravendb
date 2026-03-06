using System;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Server.ServerWide;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_25049 : RavenTestBase
    {
        public RavenDB_25049(ITestOutputHelper output) : base(output)
        {
        }

        private record CertResponse(string PublicKey, string Certificate, string Thumbprint);

        [RavenFact(RavenTestCategory.Certificates)]
        public async Task GeneratePullReplicationCertificate_ShouldFail_WhenBothMonthsAndYearsProvided()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var serverCertificateForCommunication = certificates.ServerCertificateForCommunication.Value;

            using var store = GetDocumentStore(new Options
            {
                AdminCertificate = serverCertificateForCommunication,
                ClientCertificate = serverCertificateForCommunication
            });

            var http = store.GetRequestExecutor().HttpClient;
            var url = new Uri($"{Server.WebUrl}/databases/{Uri.EscapeDataString(store.Database)}/admin/pull-replication/generate-certificate?validMonths=1&validYears=1");
            using var request = new HttpRequestMessage(HttpMethod.Post, url);
            using var response = await http.SendAsync(request);

            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        }

        [RavenFact(RavenTestCategory.Certificates)]
        public async Task GeneratePullReplicationCertificate_ShouldReturnValidCertificate_WhenMonthsProvided()
        {
            var months = 2;
            var expectedNotAfterMin = DateTime.UtcNow.AddMonths(months).AddMinutes(-5); // allow small time differences
            var expectedNotAfterMax = DateTime.UtcNow.AddMonths(months).AddMinutes(5);

            var certificates = Certificates.SetupServerAuthentication();
            var serverCertificateForCommunication = certificates.ServerCertificateForCommunication.Value;

            using var store = GetDocumentStore(new Options
            {
                AdminCertificate = serverCertificateForCommunication,
                ClientCertificate = serverCertificateForCommunication
            });

            var http = store.GetRequestExecutor().HttpClient;
            var url = new Uri($"{Server.WebUrl}/databases/{Uri.EscapeDataString(store.Database)}/admin/pull-replication/generate-certificate?validMonths={months}");
            using var request = new HttpRequestMessage(HttpMethod.Post, url);
            using var response = await http.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            var parsed = JsonSerializer.Deserialize<CertResponse>(json, new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            });

            Assert.NotNull(parsed);
            Assert.False(string.IsNullOrEmpty(parsed!.PublicKey));
            Assert.False(string.IsNullOrEmpty(parsed.Certificate));
            Assert.False(string.IsNullOrEmpty(parsed.Thumbprint));

            // Load the returned PFX and CER
            var pfxBytes = Convert.FromBase64String(parsed.Certificate);
#pragma warning disable SYSLIB0057
            var cert = new X509Certificate2(pfxBytes, (string)null, X509KeyStorageFlags.Exportable | X509KeyStorageFlags.MachineKeySet);
#pragma warning restore SYSLIB0057
            Assert.Equal(parsed.Thumbprint, cert.Thumbprint);

            // verify NotAfter roughly matches requested validity
            Assert.InRange(cert.NotAfter.ToUniversalTime(), expectedNotAfterMin, expectedNotAfterMax);

            // PublicKey should match same cert when loaded as CER (no private key)
            var cerBytes = Convert.FromBase64String(parsed.PublicKey);
#pragma warning disable SYSLIB0057
            var publicOnly = new X509Certificate2(cerBytes);
#pragma warning restore SYSLIB0057
            Assert.Equal(cert.Thumbprint, publicOnly.Thumbprint);
            
            var hasClientAuthEku = SecretProtection.HasCertificateClientAuthEnhancedKeyUsage(cert);
            Assert.True(hasClientAuthEku, "Certificate should have Client Auth EKU");
        }
    }
}
