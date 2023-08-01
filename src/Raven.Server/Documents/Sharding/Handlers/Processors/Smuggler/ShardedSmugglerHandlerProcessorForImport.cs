using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Smuggler;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Smuggler
{
    internal sealed class ShardedSmugglerHandlerProcessorForImport : AbstractSmugglerHandlerProcessorForImport<ShardedSmugglerHandler, TransactionOperationContext>
    {
        public ShardedSmugglerHandlerProcessorForImport([NotNull] ShardedSmugglerHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask ImportAsync(JsonOperationContext context, long? operationId)
        {
            var databaseContext = RequestHandler.DatabaseContext;
            operationId ??= RequestHandler.DatabaseContext.Operations.GetNextOperationId();
            await Import(context, databaseContext.DatabaseName, databaseContext.Smuggler.GetImportDelegateForHandler(RequestHandler), databaseContext.Operations,
                operationId.Value);
        }
    }
}
