using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Configuration;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Configuration
{
    internal class ShardedConfigurationHandlerProcessorForPostTimeSeriesConfiguration : AbstractConfigurationHandlerProcessorForPostTimeSeriesConfiguration<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedConfigurationHandlerProcessorForPostTimeSeriesConfiguration([NotNull] ShardedDatabaseRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected override async ValueTask WaitForIndexNotificationAsync(TransactionOperationContext context, long index)
        {
            await RequestHandler.WaitForIndexNotificationAsync(index);
        }
    }
}
