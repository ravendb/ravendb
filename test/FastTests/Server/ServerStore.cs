using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server
{
    public class ServerStore : RavenTestBase
    {
        public ServerStore(ITestOutputHelper output) : base(output)
        {
        }

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

        public class PutDatabaseDocumentTestCommand : RavenCommand<BlittableJsonReaderObject>, IRaftCommand, IDisposable
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
                message.Content = new BlittableJsonContent(async stream => await databaseDocument.WriteJsonToAsync(stream));

                return message;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                Result = response;
            }

            public void Dispose()
            {
                databaseDocument.Dispose();
            }

            public override bool IsReadRequest => false;
            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
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

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                Result = response;
            }

            public override bool IsReadRequest => true;
            
        }
    }
}

