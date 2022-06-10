using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.ETL.Test;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.Raven.Handlers.Processors
{
    internal abstract class AbstractEtlHandlerProcessorForTestEtl<TRequestHandler, TOperationContext, TTestEtlScript, TConfiguration, TConnectionString> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
        where TOperationContext : JsonOperationContext
        where TTestEtlScript : TestEtlScript<TConfiguration, TConnectionString>
        where TConfiguration : EtlConfiguration<TConnectionString>
        where TConnectionString : ConnectionString
    {
        protected AbstractEtlHandlerProcessorForTestEtl([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract bool SupportsCurrentNode { get; }

        protected abstract TTestEtlScript GetTestEtlScript(BlittableJsonReaderObject json);

        protected abstract IDisposable TestScript(TOperationContext context, TTestEtlScript testScript, out TestEtlScriptResult testResult);

        private async ValueTask HandleCurrentNodeAsync(TOperationContext context, TTestEtlScript testScript)
        {
            using (TestScript(context, testScript, out var testResult))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(testResult);
                    writer.WriteObject(context.ReadObject(djv, "et/sql/test"));
                }
            }
        }

        protected abstract ValueTask HandleRemoteNodeAsync(TOperationContext context, TTestEtlScript testScript, BlittableJsonReaderObject testScriptJson);

        public override async ValueTask ExecuteAsync()
        {
            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "etl/test/script");

                var testScript = GetTestEtlScript(json);

                if (SupportsCurrentNode)
                    await HandleCurrentNodeAsync(context, testScript);
                else
                    await HandleRemoteNodeAsync(context, testScript, json);
            }
        }
    }
}
