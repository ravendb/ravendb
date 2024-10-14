using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.DataArchival;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.DataArchival
{
    internal class ShardedDataArchivalHandlerProcessorForPost : AbstractDataArchivalHandlerProcessorForPost<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedDataArchivalHandlerProcessorForPost([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
    }
}
