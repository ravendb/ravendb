using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Configuration
{
    internal class ConfigurationHandlerProcessorForPostTimeSeriesConfiguration : AbstractConfigurationHandlerProcessorForPostTimeSeriesConfiguration<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public ConfigurationHandlerProcessorForPostTimeSeriesConfiguration([NotNull] DatabaseRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected override async ValueTask WaitForIndexNotificationAsync(TransactionOperationContext context, long index)
        {
            DatabaseTopology topology;
            ClusterTopology clusterTopology;
            using (context.OpenReadTransaction())
            {
                topology = ServerStore.Cluster.ReadDatabaseTopology(context, RequestHandler.DatabaseName);
                clusterTopology = ServerStore.GetClusterTopology(context);
            }

            await RequestHandler.WaitForExecutionOnRelevantNodes(context, RequestHandler.DatabaseName, clusterTopology, topology.Members, index);
        }
    }
}
