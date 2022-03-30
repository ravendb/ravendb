using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Identities;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Identities
{
    internal class ShardedIdentityHandlerProcessorForPostIdentity : AbstractIdentityHandlerProcessorForPostIdentity<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedIdentityHandlerProcessorForPostIdentity([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override string GetDatabaseName() => RequestHandler.DatabaseContext.DatabaseName;
    }
}
