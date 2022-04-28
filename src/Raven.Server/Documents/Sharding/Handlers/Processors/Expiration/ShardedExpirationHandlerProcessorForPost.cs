using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Expiration;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Expiration
{
    internal class ShardedExpirationHandlerProcessorForPost : AbstractExpirationHandlerProcessorForPost<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedExpirationHandlerProcessorForPost([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
    }
}
