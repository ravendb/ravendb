using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.ETL.Providers.Raven.Test;
using Raven.Server.Documents.ETL.Test;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.Raven.Handlers.Processors
{
    internal class EtlHandlerProcessorForTestEtl : AbstractEtlHandlerProcessorForTestEtl<DatabaseRequestHandler, DocumentsOperationContext, TestRavenEtlScript, RavenEtlConfiguration, RavenConnectionString>
    {
        public EtlHandlerProcessorForTestEtl([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override bool SupportsCurrentNode => true;

        protected override TestRavenEtlScript GetTestEtlScript(BlittableJsonReaderObject json) => JsonDeserializationServer.TestRavenEtlScript(json);

        protected override IDisposable TestScript(DocumentsOperationContext context, TestRavenEtlScript testScript, out TestEtlScriptResult testResult)
        {
            return RavenEtl.TestScript(testScript, RequestHandler.Database, RequestHandler.ServerStore, context, out testResult);
        }

        protected override ValueTask HandleRemoteNodeAsync(DocumentsOperationContext context, TestRavenEtlScript testScript, BlittableJsonReaderObject testScriptJson)
        {
            throw new NotSupportedException();
        }
    }
}
