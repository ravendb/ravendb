using JetBrains.Annotations;
using Raven.Client.Documents.Operations.DataArchival;
using Raven.Server.Documents.Handlers.Processors.DataArchival;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.DataArchival
{
    internal class ShardedDataArchivalHandlerProcessorForGet : AbstractDataArchivalHandlerProcessorForGet<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedDataArchivalHandlerProcessorForGet([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override DataArchivalConfiguration GetDataArchivalConfiguration()
        {
            return RequestHandler.DatabaseContext.DatabaseRecord.DataArchival;
        }
    }
}
