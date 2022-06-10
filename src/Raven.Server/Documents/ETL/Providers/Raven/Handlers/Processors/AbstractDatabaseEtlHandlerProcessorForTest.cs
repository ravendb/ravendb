using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.ETL.Test;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.Raven.Handlers.Processors;

internal abstract class AbstractDatabaseEtlHandlerProcessorForTest<TTestEtlScript, TConfiguration, TConnectionString> : AbstractEtlHandlerProcessorForTest<DatabaseRequestHandler, DocumentsOperationContext, TTestEtlScript, TConfiguration, TConnectionString>
    where TTestEtlScript : TestEtlScript<TConfiguration, TConnectionString>
    where TConfiguration : EtlConfiguration<TConnectionString>
    where TConnectionString : ConnectionString
{
    protected AbstractDatabaseEtlHandlerProcessorForTest([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override IDisposable TestScript(DocumentsOperationContext context, TTestEtlScript testScript, out TestEtlScriptResult testResult)
    {
        return RavenEtl.TestScript(testScript, RequestHandler.Database, RequestHandler.ServerStore, context, out testResult);
    }

    protected override ValueTask HandleRemoteNodeAsync(DocumentsOperationContext context, TTestEtlScript testScript, BlittableJsonReaderObject testScriptJson)
    {
        throw new NotSupportedException();
    }
}
