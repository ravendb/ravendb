using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Expiration
{
    internal sealed class ShardedExpirationHandlerProcessorForGet : AbstractDatabaseHandlerProcessorForGetConfiguration<ShardedDatabaseRequestHandler, TransactionOperationContext, ExpirationConfiguration>
    {
        public ShardedExpirationHandlerProcessorForGet([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override ExpirationConfiguration GetConfiguration()
        {
            return RequestHandler.DatabaseContext.DatabaseRecord.Expiration;
        }
    }
}
