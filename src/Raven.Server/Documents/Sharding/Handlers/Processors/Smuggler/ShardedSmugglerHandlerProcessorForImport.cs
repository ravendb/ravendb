using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Smuggler;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Smuggler
{
    internal class ShardedSmugglerHandlerProcessorForImport : AbstractSmugglerHandlerProcessorForImport<ShardedSmugglerHandler, TransactionOperationContext>
    {
        public ShardedSmugglerHandlerProcessorForImport([NotNull] ShardedSmugglerHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask ImportAsync(JsonOperationContext context, long? operationId)
        {
            operationId ??= RequestHandler.DatabaseContext.Operations.GetNextOperationId();
            await Import(context, RequestHandler.DatabaseContext.DatabaseName, RequestHandler.DoImportInternalAsync, RequestHandler.DatabaseContext.Operations, operationId.Value);
        }
    }
}
