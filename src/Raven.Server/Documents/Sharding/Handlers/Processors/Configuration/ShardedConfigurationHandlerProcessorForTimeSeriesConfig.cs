using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Configuration;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Configuration
{
    internal class ShardedConfigurationHandlerProcessorForTimeSeriesConfig : AbstractConfigurationHandlerProcessorForTimeSeriesConfig<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedConfigurationHandlerProcessorForTimeSeriesConfig([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override string GetDatabaseName()
        {
            return RequestHandler.DatabaseContext.DatabaseName;
        }
    }
}
