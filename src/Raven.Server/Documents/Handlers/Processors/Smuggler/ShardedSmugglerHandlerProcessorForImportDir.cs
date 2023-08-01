using JetBrains.Annotations;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Smuggler;

internal sealed class ShardedSmugglerHandlerProcessorForImportDir : AbstractSmugglerHandlerProcessorForImportDir<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedSmugglerHandlerProcessorForImportDir([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override ImportDelegate DoImport => RequestHandler.DatabaseContext.Smuggler.GetImportDelegateForHandler(RequestHandler);

    protected override long GetOperationId() => RequestHandler.DatabaseContext.Operations.GetNextOperationId();
}
