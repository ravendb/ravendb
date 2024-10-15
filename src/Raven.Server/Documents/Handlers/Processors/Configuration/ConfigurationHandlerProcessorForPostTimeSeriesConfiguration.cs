using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Configuration
{
    internal sealed class ConfigurationHandlerProcessorForPostTimeSeriesConfiguration : AbstractConfigurationHandlerProcessorForPostTimeSeriesConfiguration<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public ConfigurationHandlerProcessorForPostTimeSeriesConfiguration([NotNull] DatabaseRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected override async ValueTask WaitForIndexNotificationAsync(TransactionOperationContext context, long index)
        {
            DatabaseTopology topology;
            using (context.OpenReadTransaction())
            {
                topology = ServerStore.Cluster.ReadDatabaseTopology(context, RequestHandler.DatabaseName);
            }

            await ServerStore.WaitForExecutionOnRelevantNodesAsync(context, topology.Members, index);
        }
    }
}
