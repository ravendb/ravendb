using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Configuration
{
    internal abstract class AbstractConfigurationHandlerProcessorForPostTimeSeriesConfiguration<TRequestHandler, TOperationContext> : AbstractHandlerProcessorForUpdateDatabaseConfiguration<BlittableJsonReaderObject, TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractConfigurationHandlerProcessorForPostTimeSeriesConfiguration([NotNull] TRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected override Task<(long Index, object Result)> OnUpdateConfiguration(TransactionOperationContext context, BlittableJsonReaderObject configuration, string raftRequestId)
        {
            return ModifyTimeSeriesConfiguration(context, RequestHandler.DatabaseName, configuration, raftRequestId);
        }

        protected override void OnBeforeUpdateConfiguration(ref BlittableJsonReaderObject configuration, JsonOperationContext context)
        {
            if (configuration == null)
            {
                return;
            }

            var hasCollectionsConfig = configuration.TryGet(nameof(TimeSeriesConfiguration.Collections), out BlittableJsonReaderObject collections) &&
                                       collections?.Count > 0;

            if (hasCollectionsConfig == false)
                return;

            var uniqueKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var prop = new BlittableJsonReaderObject.PropertyDetails();

            for (var i = 0; i < collections.Count; i++)
            {
                collections.GetPropertyByIndex(i, ref prop);

                if (uniqueKeys.Add(prop.Name) == false)
                {
                    throw new InvalidOperationException("Cannot have two different revision configurations on the same collection. " +
                                                        $"Collection name : '{prop.Name}'");
                }
            }
        }

        private async Task<(long, object)> ModifyTimeSeriesConfiguration(TransactionOperationContext context, string name, BlittableJsonReaderObject configurationJson, string raftRequestId)
        {
            var configuration = JsonDeserializationCluster.TimeSeriesConfiguration(configurationJson);
            configuration?.InitializeRollupAndRetention();
            ServerStore.LicenseManager.AssertCanAddTimeSeriesRollupsAndRetention(configuration);
            var editTimeSeries = new EditTimeSeriesConfigurationCommand(configuration, name, raftRequestId);
            var result = await ServerStore.SendToLeaderAsync(editTimeSeries);

            DatabaseTopology topology;
            ClusterTopology clusterTopology;
            using (context.OpenReadTransaction())
            {
                topology = ServerStore.Cluster.ReadDatabaseTopology(context, name);
                clusterTopology = ServerStore.GetClusterTopology(context);
            }

            await RequestHandler.WaitForExecutionOnRelevantNodes(context, name, clusterTopology, topology.Members, result.Index);

            return result;
        }
    }
}
