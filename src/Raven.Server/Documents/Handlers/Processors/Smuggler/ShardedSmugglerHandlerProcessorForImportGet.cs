using JetBrains.Annotations;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Smuggler;

internal sealed class ShardedSmugglerHandlerProcessorForImportGet : AbstractSmugglerHandlerProcessorForImportGet<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedSmugglerHandlerProcessorForImportGet([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override ImportDelegate DoImport => RequestHandler.DatabaseContext.Smuggler.GetImportDelegateForHandler(RequestHandler);

    protected override long GetOperationId() => RequestHandler.DatabaseContext.Operations.GetNextOperationId();
}
