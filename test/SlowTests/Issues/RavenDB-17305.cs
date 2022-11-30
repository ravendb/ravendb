using System;
using System.IO;
using System.Net.Http;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Utils;
using Sparrow.Json;
using Voron.Util;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17305 : RavenTestBase
    {
        [Fact]
        public void WillExplicitlyTrustOurOwnCertificate_SelfSigned()
        {
            var certificate = CertificateRequest(Environment.MachineName).CreateSelfSigned(DateTime.Today.AddDays(-1), DateTime.Today.AddDays(1));
            ValidateServerCanTalkToItself(certificate);
        }
        
        private void ValidateServerCanTalkToItself(X509Certificate2 certificate)
        {
            string certFileName = Path.GetTempFileName();
            using var _ = new DisposableAction(() => IOExtensions.DeleteFile(certFileName));
            File.WriteAllBytes(certFileName, certificate.Export(X509ContentType.Pkcs12));
            SetupServerAuthentication(certificates: new TestCertificatesHolder(certFileName, null, null, null));
            var store = GetDocumentStore(new Options { ModifyDocumentStore = documentStore => documentStore.Certificate = new X509Certificate2(certFileName) });

            Assert.StartsWith("https", store.Urls[0]);
            
            WaitForUserToContinueTheTest(store);

            var results = store.Maintenance.Server.Send(new PingOperation());
            Assert.Equal(1, results.Length);
            Assert.Null(results[0].Error);
        }

        private static CertificateRequest CertificateRequest(string host)
        {
            RSA key = RSA.Create();
            var certificateRequest =
                new CertificateRequest(new X500DistinguishedName("CN=" + host), key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1)
                {
                    CertificateExtensions =
                    {
                        new X509BasicConstraintsExtension(true, false, 0, false),
                        new X509KeyUsageExtension(
                            X509KeyUsageFlags.KeyEncipherment | X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign | X509KeyUsageFlags.DigitalSignature, false),
                        new X509EnhancedKeyUsageExtension(new OidCollection
                        {
                            new Oid("1.3.6.1.5.5.7.3.2"), // client authentication
                            new Oid("1.3.6.1.5.5.7.3.1"), // server authentication
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
                public string Url;
                public long TcpInfoTime;
                public long SendTime;
                public long ReceiveTime;
                public string Error;
                public string[] Log;
            }

            public class PingCommand : RavenCommand<PingResult[]>
            {
                public override bool IsReadRequest => true;

                public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
                {
                    url = $"{node.Url}/admin/debug/node/ping";
                    return new HttpRequestMessage { Method = HttpMethod.Get };
                }

                public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
                {
                    // quick & dirty impl
                    if (response.TryGetMember("Result", out var results) && results is BlittableJsonReaderArray array)
                    {
                        Result = JsonConvert.DeserializeObject<PingResult[]>(array.ToString());
                    }
                }
            }
        }

        public RavenDB_17305(ITestOutputHelper output) : base(output)
        {
        }
    }
}
