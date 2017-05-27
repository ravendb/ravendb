using System;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util.Helpers;
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
                    using (var requestExecuter = RequestExecutor.Create(store.Urls, store.Database, null))
                    {
                        requestExecuter.Execute(getCommand, context);
                        using (var putCommand = new PutDatabaseDocumentTestCommand(getCommand.Result))
                        {
                            Assert.Throws<ConcurrencyException>(() => requestExecuter.Execute(putCommand, context));
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
                    using (var requestExecuter = RequestExecutor.Create(store.Urls, store.Database, null))
                    {
                        requestExecuter.Execute(command, context);
                    }

                    var result = command.Result;
                    BlittableJsonReaderObject metadata;
                    var hasMetadataProperty = result.TryGet("@metadata", out metadata);
                    long etag;
                    var hasEtagProperty = metadata.TryGet("@etag", out etag);
                    Assert.True(hasMetadataProperty && hasEtagProperty && etag > 0, $"{hasMetadataProperty} - {hasEtagProperty} - {etag}");
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

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
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
            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
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
                TransactionOperationContext context;
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out context))
                {
                    var foo = new DynamicJsonValue
                    {
                        ["Foo"] = "Bar"
                    };

                    await Server.ServerStore.PutValueInClusterAsync("foo/bar", context.ReadObject(foo, "read test stuff"));
                }

                using (Server.ServerStore.ContextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenReadTransaction())
                {
                    var fetched = Server.ServerStore.Cluster.Read(context, "foo/bar");
                    string val;
                    Assert.True(fetched.TryGet("Foo", out val));
                    Assert.Equal("Bar", val);
                }

            }
        }

        [Fact(Skip = "Should be restored")]
        public void Server_store_write_should_throw_concurrency_exception_if_relevant()
        {
            using (GetDocumentStore())
            {
                TransactionOperationContext context;
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenWriteTransaction())
                {
                    var foo = new DynamicJsonValue
                    {
                        ["Foo"] = "Bar"
                    };

                    using (var obj = context.ReadObject(foo, "read test stuff"))
                    {
                        Server.ServerStore.PutValueInClusterAsync("foo/bar", obj).Wait();
                    }
                }

                using (Server.ServerStore.ContextPool.AllocateOperationContext(out context))
                {
                    var foo = new DynamicJsonValue
                    {
                        ["Foo"] = "Bar2"
                    };

                    using (var obj = context.ReadObject(foo, "read test stuff"))
                    {
                        Server.ServerStore.PutValueInClusterAsync("foo/bar", obj).Wait();
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
                        DevelopmentHelper.TimeBomb();

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

