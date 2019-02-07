using System;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using FastTests;
using Org.BouncyCastle.Crypto.Tls;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;
using Xunit;

namespace SlowTests.Server
{
    public class ServerStore : RavenTestBase
    {
        [Fact]
        public async Task Server_store_basic_read_write_should_work()
        {
            using (GetDocumentStore())
            {
                var certificate = new X509Certificate2(GenerateAndSaveSelfSignedCertificate(), (string)null, X509KeyStorageFlags.MachineKeySet);

                TransactionOperationContext context;
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out context))
                {
                    await Server.ServerStore.PutValueInClusterAsync(new PutCertificateCommand("foo/bar", new CertificateDefinition
                    {
                        Certificate = Convert.ToBase64String(certificate.Export(X509ContentType.Cert)),
                        Permissions = new Dictionary<string, DatabaseAccess>(),
                        SecurityClearance = SecurityClearance.ClusterAdmin,
                        Thumbprint = certificate.Thumbprint,
                        PublicKeyPinningHash = CertificateUtils.GetPublicKeyPinningHash(certificate),
                        NotAfter = certificate.NotAfter
                    }));
                }

                using (Server.ServerStore.ContextPool.AllocateOperationContext(out context))
                using (context.OpenReadTransaction())
                {
                    var fetched = Server.ServerStore.Cluster.Read(context, "foo/bar");
                    string val;
                    Assert.True(fetched.TryGet(nameof(CertificateDefinition.Thumbprint), out val));
                    Assert.Equal(certificate.Thumbprint, val);
                }

            }
        }

        [Fact(Skip = "RavenDB-8758")]
        public async Task Server_store_write_should_throw_concurrency_exception_if_relevant()
        {
            using (GetDocumentStore())
            {
                var certificate = new X509Certificate2(GenerateAndSaveSelfSignedCertificate(), (string)null, X509KeyStorageFlags.MachineKeySet);

                TransactionOperationContext context;
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out context))
                using (context.OpenWriteTransaction())
                {
                    using (Server.ServerStore.ContextPool.AllocateOperationContext(out context))
                    {
                        await Server.ServerStore.PutValueInClusterAsync(new PutCertificateCommand("foo/bar", new CertificateDefinition
                        {
                            Certificate = Convert.ToBase64String(certificate.Export(X509ContentType.Cert)),
                            Permissions = null,
                            SecurityClearance = SecurityClearance.ClusterAdmin,
                            Thumbprint = certificate.Thumbprint,
                            PublicKeyPinningHash = CertificateUtils.GetPublicKeyPinningHash(certificate),
                            NotAfter = certificate.NotAfter
                        }));
                    }
                }

                using (Server.ServerStore.ContextPool.AllocateOperationContext(out context))
                {
                    using (Server.ServerStore.ContextPool.AllocateOperationContext(out context))
                    {
                        await Server.ServerStore.PutValueInClusterAsync(new PutCertificateCommand("foo/bar", new CertificateDefinition
                        {
                            Certificate = Convert.ToBase64String(certificate.Export(X509ContentType.Cert)),
                            Permissions = null,
                            SecurityClearance = SecurityClearance.ClusterAdmin,
                            Thumbprint = certificate.Thumbprint,
                            PublicKeyPinningHash = CertificateUtils.GetPublicKeyPinningHash(certificate),
                            NotAfter = certificate.NotAfter
                        }));
                    }
                }

                using (Server.ServerStore.ContextPool.AllocateOperationContext(out context))
                {

                    var foo = new DynamicJsonValue
                    {
                        ["Foo"] = "Bar3"
                    };

                    using (var blittableObj = context.ReadObject(foo, "read test stuff"))
                    {
                        //TODO: Restore this.

                        ////this shouldn't throw, since expected etag == null
                        //Server.ServerStore.PutValueInClusterAsync(context, "foo/bar", blittableObj).Wait();

                        //var lastEtag = Server.ServerStore.ReadLastEtag(context);
                        ////this shouldn't throw, since expected etag == existing etag
                        //Server.ServerStore.Write(context, "foo/bar", blittableObj, lastEtag);

                        ////this should throw because existing etag doesn't match with existing etag
                        //Assert.Throws<global::Voron.Exceptions.ConcurrencyException>(
                        //    () => Server.ServerStore.Write(context, "foo/bar", blittableObj, 1));

                        ////this should throw because it has expected etag, but there is no existing value
                        //Assert.Throws<global::Voron.Exceptions.ConcurrencyException>(
                        //    () => Server.ServerStore.Write(context, "foo/bar2", blittableObj, 1));
                    }
                }

            }
        }
    }
}
