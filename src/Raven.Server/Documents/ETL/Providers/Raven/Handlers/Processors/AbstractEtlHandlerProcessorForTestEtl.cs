using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.Raven.Handlers.Processors
{
    internal abstract class AbstractEtlHandlerProcessorForTestEtl<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        public AbstractEtlHandlerProcessorForTestEtl([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract ValueTask GetAndWriteEtlTestScriptResultAsync(TOperationContext context, BlittableJsonReaderObject testConfig);

        public override async ValueTask ExecuteAsync()
        {
            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            {
                var testConfig = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "TestRavenEtlScript");

                await GetAndWriteEtlTestScriptResultAsync(context, testConfig);
            }
        }
    }
}
