using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Identities;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Identities
{
    internal sealed class ShardedIdentityDebugHandlerProcessorForGetIdentities : AbstractIdentityDebugHandlerProcessorForGetIdentities<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedIdentityDebugHandlerProcessorForGetIdentities([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
    }
}
