using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Documents.ETL.Providers.Raven.Test;
using Raven.Server.Documents.ETL.Providers.SQL.Handlers.Processors;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.ETL
{
    internal class ShardedSqlEtlHandlerProcessorForTestSqlEtl : AbstractSqlEtlHandlerProcessorForTestSqlEtl<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedSqlEtlHandlerProcessorForTestSqlEtl([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask GetAndWriteSqlEtlScriptTestResultAsync(TransactionOperationContext context, BlittableJsonReaderObject sqlScript)
        {
            using (var token = RequestHandler.CreateOperationToken())
            {
                if (sqlScript.TryGet(nameof(TestRavenEtlScript.DocumentId), out string docId) == false)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                    return;
                }

                var shardNumber = RequestHandler.DatabaseContext.GetShardNumber(context, docId);
                var cmd = new ProxyCommand(new GetSqlEtlTestScriptResultCommand(sqlScript), RequestHandler.HttpContext.Response);
                await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(cmd, shardNumber, token.Token);
            }
        }

        internal class GetSqlEtlTestScriptResultCommand : RavenCommand
        {
            private readonly BlittableJsonReaderObject _testScript;
            public override bool IsReadRequest => true;

            public GetSqlEtlTestScriptResultCommand(BlittableJsonReaderObject testScript)
            {
                _testScript = testScript;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/etl/sql/test";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream =>
                    {
                        await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                        {
                            writer.WriteObject(_testScript);
                        }
                    })
                };

                return request;
            }
        }
    }
}
