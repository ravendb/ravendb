using System;
using System.IO;
using System.Net.Http;
using Raven.Client.Documents.Commands;
using Raven.Client.Http;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron.Exceptions;
using Xunit;

namespace FastTests.Server.Basic
{
    public class ServerStore : RavenTestBase
    {
        [Fact]
        public void Admin_databases_endpoint_should_refuse_document_with_lower_etag_with_concurrency_Exception()
        {
            using (var store = GetDocumentStore())
            {
                TransactionOperationContext context;
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out context))
                {
                    var getCommand = new GetDatabaseDocumentTestCommand();
                    using (var requestExecuter = new RequestExecuter(store.Url, store.DefaultDatabase, null))
                    {
                        requestExecuter.Execute(getCommand, context);
                        var putCommand = new PutDatabaseDocumentTestCommand(getCommand.Result);

                        var exception = Assert.Throws<InternalServerErrorException>(() => requestExecuter.Execute(putCommand, context));
                        Assert.Contains("ConcurrencyException", exception.Message);
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
                    Assert.True(HasEtagInDatabaseDocumentResponse(store.Url, store.DefaultDatabase, context));
                }
            }
        }

        public class PutDatabaseDocumentTestCommand : RavenCommand<BlittableJsonReaderObject>
        {
            private readonly BlittableJsonReaderObject databaseDocument;

            public PutDatabaseDocumentTestCommand(BlittableJsonReaderObject databaseDocument)
            {
                this.databaseDocument = databaseDocument;
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = String.Empty;
                IsAdminCommand = true;
                var message = new HttpRequestMessage
                {
                    Method = HttpMethod.Put
                };

                message.Headers.Add("ETag", "0");
                message.Content = new BlittableJsonContent(stream =>  databaseDocument.WriteJsonTo(stream));

                return message;
            }

            public override void SetResponse(BlittableJsonReaderObject response)
            {
                Result = response;
            }

        }

        public class GetDatabaseDocumentTestCommand : RavenCommand<BlittableJsonReaderObject>
        {        
            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = String.Empty;
                IsAdminCommand = true;
                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get,
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response)
            {
                Result = response;
            }

        }

        private bool HasEtagInDatabaseDocumentResponse(string url,string databaseName, JsonOperationContext context)
        {
            var command = new GetDatabaseDocumentTestCommand();
            using (var requestExecuter = new RequestExecuter(url,databaseName,null))
            {
                requestExecuter.Execute(command, context);
            }

            var result = command.Result;
            BlittableJsonReaderObject metadata;
            var hasMetadataProperty = result.TryGet("@metadata", out metadata);
            long etag;
            var hasEtagProperty = metadata.TryGet("@etag", out etag);
            return hasMetadataProperty && hasEtagProperty && etag > 0; 
        }

        [Fact]
        public void Server_store_basic_read_write_should_work()
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

                    Server.ServerStore.Write(context, "foo/bar", context.ReadObject(foo, "read test stuff"));
                    tx.Commit();
                }

                using (Server.ServerStore.ContextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenReadTransaction())
                {

                    var fetched = Server.ServerStore.Read(context, "foo/bar");
                    string val;
                    Assert.True(fetched.TryGet("Foo",out val));
                    Assert.Equal("Bar", val);
                }

            }
        }

        [Fact]
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
                        Server.ServerStore.Write(context, "foo/bar", obj);
                        tx.Commit();
                    }
                }

                using (Server.ServerStore.ContextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenWriteTransaction())
                {
                    var foo = new DynamicJsonValue
                    {
                        ["Foo"] = "Bar2"
                    };

                    using (var obj = context.ReadObject(foo, "read test stuff"))
                    {
                        Server.ServerStore.Write(context, "foo/bar", obj);
                        tx.Commit();
                    }
                }

                using (Server.ServerStore.ContextPool.AllocateOperationContext(out context))
                using (context.OpenWriteTransaction())
                {

                    var foo = new DynamicJsonValue
                    {
                        ["Foo"] = "Bar3"
                    };

                    using (var blittableObj = context.ReadObject(foo, "read test stuff"))
                    {
                        //this shouldn't throw, since expected etag == null
                        Server.ServerStore.Write(context, "foo/bar", blittableObj);

                        var lastEtag = Server.ServerStore.ReadLastEtag(context);
                        //this shouldn't throw, since expected etag == existing etag
                        Server.ServerStore.Write(context, "foo/bar", blittableObj, lastEtag);

                        //this should throw because existing etag doesn't match with existing etag
                        Assert.Throws<ConcurrencyException>(
                            () => Server.ServerStore.Write(context, "foo/bar", blittableObj, 1));

                        //this should throw because it has expected etag, but there is no existing value
                        Assert.Throws<ConcurrencyException>(
                            () => Server.ServerStore.Write(context, "foo/bar2", blittableObj, 1));
                    }
                }

            }
        }
    }
}

