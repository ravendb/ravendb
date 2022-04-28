using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Identities;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Identities;

internal class ShardedIdentityHandlerProcessorForNextIdentityFor : AbstractIdentityHandlerProcessorForNextIdentityFor<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedIdentityHandlerProcessorForNextIdentityFor([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override char GetDatabaseIdentityPartsSeparator() => RequestHandler.DatabaseContext.IdentityPartsSeparator;
}
