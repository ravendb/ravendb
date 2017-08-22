using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Conventions;
using Raven.Client.Exceptions;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Server
{
    public class ServerStore : RavenTestBase
    {
        [Fact]
        public void Admin_databases_endpoint_should_refuse_document_with_lower_etag_with_concurrency_Exception()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Load<object>("users/1");// just waiting for the db to load
                }

                TransactionOperationContext context;
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out context))
                {
                    var getCommand = new GetDatabaseDocumentTestCommand();
                    using (var requestExecutor = RequestExecutor.Create(store.Urls, store.Database, null, DocumentConventions.Default))
                    {
                        requestExecutor.Execute(getCommand, context);
                        using (var putCommand = new PutDatabaseDocumentTestCommand(getCommand.Result))
                        {
                            Assert.Throws<ConcurrencyException>(() => requestExecutor.Execute(putCommand, context));
                        }
                    }
                }
            }
        }

        [Fact]
        public void Admin_databases_endpoint_should_fetch_document_with_etag_in_metadata_property()
        {
            using (var store = GetDocumentStore())
            {
                TransactionOperationContext context;
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out context))
                {
                    var command = new GetDatabaseDocumentTestCommand();
                    using (var requestExecutor = RequestExecutor.Create(store.Urls, store.Database, null, DocumentConventions.Default))
                    {
                        requestExecutor.Execute(command, context);
                    }

                    var hasEtag = command.Result.TryGet("Etag", out long etag);
                    Assert.True(hasEtag && etag != 0, $"{hasEtag} - {etag}");
                }
            }
        }

        public class PutDatabaseDocumentTestCommand : RavenCommand<BlittableJsonReaderObject>, IDisposable
        {
            private readonly BlittableJsonReaderObject databaseDocument;

            public PutDatabaseDocumentTestCommand(BlittableJsonReaderObject databaseDocument)
            {
                this.databaseDocument = databaseDocument;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/databases?name={node.Database}";

                var message = new HttpRequestMessage
                {
                    Method = HttpMethod.Put
                };

                message.Headers.Add("ETag", "0");
                message.Content = new BlittableJsonContent(stream => databaseDocument.WriteJsonTo(stream));

                return message;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                Result = response;
            }

            public void Dispose()
            {
                databaseDocument.Dispose();
            }

            public override bool IsReadRequest => false;
        }

        public class GetDatabaseDocumentTestCommand : RavenCommand<BlittableJsonReaderObject>
        {
            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/databases?name={node.Database}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get,
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                Result = response;
            }

            public override bool IsReadRequest => true;
        }

        [Fact]
        public async Task Server_store_basic_read_write_should_work()
        {
            using (GetDocumentStore())
            {
                var certificate = new X509Certificate2(GenerateAndSaveSelfSignedCertificate());

                TransactionOperationContext context;
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out context))
                {
                    await Server.ServerStore.PutValueInClusterAsync(new PutCertificateCommand("foo/bar", new CertificateDefinition
                    {
                        Certificate = Convert.ToBase64String(certificate.Export(X509ContentType.Cert)),
                        Permissions = new Dictionary<string, DatabaseAccess>(),
                        SecurityClearance = SecurityClearance.ClusterAdmin,
                        Thumbprint = certificate.Thumbprint
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

        [Fact(Skip = "Should be restored")]
        public async Task Server_store_write_should_throw_concurrency_exception_if_relevant()
        {
            using (GetDocumentStore())
            {
                var certificate = new X509Certificate2(GenerateAndSaveSelfSignedCertificate());

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
                            Thumbprint = certificate.Thumbprint
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
                            Thumbprint = certificate.Thumbprint
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

