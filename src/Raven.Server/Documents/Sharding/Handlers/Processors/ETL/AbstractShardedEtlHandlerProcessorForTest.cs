using System;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Http;
using Raven.Server.Documents.ETL.Providers.Raven.Handlers.Processors;
using Raven.Server.Documents.ETL.Test;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.ETL;

internal abstract class AbstractShardedEtlHandlerProcessorForTest<TTestEtlScript, TConfiguration, TConnectionString> : AbstractEtlHandlerProcessorForTest<ShardedDatabaseRequestHandler, TransactionOperationContext, TTestEtlScript, TConfiguration, TConnectionString>
    where TTestEtlScript : TestEtlScript<TConfiguration, TConnectionString>
    where TConfiguration : EtlConfiguration<TConnectionString>
    where TConnectionString : ConnectionString
{
    protected AbstractShardedEtlHandlerProcessorForTest([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => false;

    protected override IDisposable TestScript(TransactionOperationContext context, TTestEtlScript testScript, out TestEtlScriptResult testResult)
    {
        throw new NotSupportedException();
    }

    protected abstract RavenCommand CreateCommand(BlittableJsonReaderObject json);

    protected override async ValueTask HandleRemoteNodeAsync(TransactionOperationContext context, TTestEtlScript testScript, BlittableJsonReaderObject testScriptJson)
    {
        using (var token = RequestHandler.CreateOperationToken())
        {
            if (string.IsNullOrEmpty(testScript.DocumentId))
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                return;
            }

            var shardNumber = RequestHandler.DatabaseContext.GetShardNumber(context, testScript.DocumentId);

            var command = CreateCommand(testScriptJson);

            await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(new ProxyCommand(command, RequestHandler.HttpContext.Response), shardNumber, token.Token);
        }
    }
}
