using System;
using System.IO;
using System.Net.Http;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_17305 : RavenTestBase
    {
        public RavenDB_17305(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Security)]
        public void WillExplicitlyTrustOurOwnCertificate_SelfSigned()
        {
            var now = DateTime.UtcNow;
            var certificate = CreateCertificateRequest(Environment.MachineName)
                .CreateSelfSigned(now.AddDays(-1), now.AddDays(1));
            ValidateServerCanTalkToItself(certificate);
        }

        [RavenFact(RavenTestCategory.Security)]
        public void WillExplicitlyTrustOurOwnCertificate_SelfSigned_Expired()
        {
            var now = DateTime.UtcNow;
            var certificate = CreateCertificateRequest(Environment.MachineName)
                .CreateSelfSigned(now.AddDays(-30), now.AddDays(-1));
            ValidateServerCanTalkToItself(certificate);
        }

        [RavenFact(RavenTestCategory.Security)]
        public void WillExplicitlyTrustOurOwnCertificate_SelfSigned_NotYetValid()
        {
            var now = DateTime.UtcNow;
            var certificate = CreateCertificateRequest(Environment.MachineName)
                .CreateSelfSigned(now.AddDays(1), now.AddDays(30));
            ValidateServerCanTalkToItself(certificate);
        }

        private void ValidateServerCanTalkToItself(X509Certificate2 certificate)
        {
            var certBytes = certificate.Export(X509ContentType.Pkcs12);
            string certFileName = GetTempFileName();
            File.WriteAllBytes(certFileName, certBytes);
            // Reload with persistent key storage — ephemeral keys from
            // CreateSelfSigned are rejected by Windows SChannel for expired certs.
            var clientCert = CertificateLoaderUtil.CreateCertificate(certBytes);
            var certificates = new TestCertificatesHolder(certFileName, certFileName, certFileName, certFileName, certFileName);
            Certificates.SetupServerAuthentication(certificates: certificates);
            var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = documentStore => documentStore.Certificate = clientCert
            });

            Assert.StartsWith("https", store.Urls[0]);

            var results = store.Maintenance.Server.Send(new PingOperation());
            Assert.Equal(1, results.Length);
            Assert.Null(results[0].Error);
        }

        private static CertificateRequest CreateCertificateRequest(string host)
        {
            var key = RSA.Create();
            var certificateRequest =
                new CertificateRequest(new X500DistinguishedName("CN=" + host), key, HashAlgorithmName.SHA256,
                    RSASignaturePadding.Pkcs1)
                {
                    CertificateExtensions =
                    {
                        new X509BasicConstraintsExtension(true, false, 0, false),
                        new X509KeyUsageExtension(
                            X509KeyUsageFlags.KeyEncipherment | X509KeyUsageFlags.KeyCertSign |
                            X509KeyUsageFlags.CrlSign | X509KeyUsageFlags.DigitalSignature, false),
                        new X509EnhancedKeyUsageExtension(new OidCollection
                        {
                            new("1.3.6.1.5.5.7.3.2"), // client authentication
                            new("1.3.6.1.5.5.7.3.1"), // server authentication
                        }, true)
                    }
                };
            return certificateRequest;
        }

        private class PingOperation : IServerOperation<PingOperation.PingResult[]>
        {
            public RavenCommand<PingResult[]> GetCommand(DocumentConventions conventions, JsonOperationContext context)
            {
                return new PingCommand();
            }

            public class PingResult
            {
#pragma warning disable CS0649 // deserialization targets
                public string Url;
                public long TcpInfoTime;
                public long SendTime;
                public long ReceiveTime;
                public string Error;
                public string[] Log;
#pragma warning restore CS0649
            }

            public class PingCommand : RavenCommand<PingResult[]>
            {
                public override bool IsReadRequest => true;

                public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
                {
                    url = $"{node.Url}/admin/debug/node/ping";
                    return new HttpRequestMessage { Method = HttpMethod.Get };
                }

                public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response,
                    bool fromCache)
                {
                    if (response.TryGetMember("Result", out var results) && results is BlittableJsonReaderArray array)
                    {
                        Result = JsonConvert.DeserializeObject<PingResult[]>(array.ToString());
                    }
                }
            }
        }
    }
}
