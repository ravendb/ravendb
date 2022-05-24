using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Documents.ETL.Providers.Raven.Test;
using Raven.Server.Documents.Handlers.Processors.ETL;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.ETL
{
    internal class ShardedEtlHandlerProcessorForTestEtl : AbstractEtlHandlerProcessorForTestEtl<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedEtlHandlerProcessorForTestEtl([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask GetAndWriteEtlTestScriptResultAsync(TransactionOperationContext context, BlittableJsonReaderObject testConfig)
        {
            using (var token = RequestHandler.CreateOperationToken())
            {
                if (testConfig.TryGet(nameof(TestRavenEtlScript.DocumentId), out string docId) == false)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                    return;
                }

                var shardNumber = RequestHandler.DatabaseContext.GetShardNumber(context, docId);
                var cmd = new ProxyCommand(new GetRavenEtlTestScriptResultCommand(testConfig), RequestHandler.HttpContext.Response);
                await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(cmd, shardNumber, token.Token);
            }
        }

        internal class GetRavenEtlTestScriptResultCommand : RavenCommand
        {
            private readonly BlittableJsonReaderObject _testConfig;
            public override bool IsReadRequest => true;

            public GetRavenEtlTestScriptResultCommand(BlittableJsonReaderObject testConfig)
            {
                _testConfig = testConfig;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/etl/raven/test";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream =>
                    {
                        await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                        {
                            writer.WriteObject(_testConfig);
                        }
                    })
                };

                return request;
            }
        }
    }
}
