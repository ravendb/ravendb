using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Identities;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Identities;

internal class ShardedIdentityHandlerProcessorForNextIdentityFor : AbstractIdentityHandlerProcessorForNextIdentityFor<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedIdentityHandlerProcessorForNextIdentityFor([NotNull] ShardedDatabaseRequestHandler requestHandler)
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override char GetDatabaseIdentityPartsSeparator() => RequestHandler.DatabaseContext.IdentityPartsSeparator;

    protected override string GetDatabaseName() => RequestHandler.DatabaseContext.DatabaseName;
}
