using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Configuration;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Configuration
{
    internal class ShardedConfigurationHandlerProcessorForPostTimeSeriesConfiguration : AbstractConfigurationHandlerProcessorForPostTimeSeriesConfiguration<ShardedDatabaseRequestHandler>
    {
        public ShardedConfigurationHandlerProcessorForPostTimeSeriesConfiguration([NotNull] ShardedDatabaseRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected override string GetDatabaseName() => RequestHandler.DatabaseContext.DatabaseName;

        protected override ValueTask WaitForIndexNotificationAsync(long index) => RequestHandler.DatabaseContext.Cluster.WaitForExecutionOnAllNodes(index);
    }
}
