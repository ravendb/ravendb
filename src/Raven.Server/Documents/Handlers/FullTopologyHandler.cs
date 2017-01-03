using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Replication.Messages;
using Raven.Server.Documents.Replication;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class FullTopologyHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/full-topology", "GET")]
        public async Task GetFullTopology()
        {
            DocumentsOperationContext context;
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            {
                ReplicationDocument replicationDocument;
                using (context.OpenReadTransaction())
                {
                    var configurationDocument = Database.DocumentsStorage.Get(context, Constants.Replication.DocumentReplicationConfiguration);
                    if (configurationDocument == null)
                    {
                        WriteEmptyTopology(context);
                        return;
                    }

                    replicationDocument = JsonDeserializationServer.ReplicationDocument(configurationDocument.Data);
                    if (replicationDocument.Destinations?.Count == 0)
                    {
                        WriteEmptyTopology(context);
                        return;
                    }
                }

                using (var clusterTopologyExplorer = new ReplicationClusterTopologyExplorer(
                    Database,
                    new Dictionary<string, List<string>>(),
                    (long)Database.Configuration.Replication.ReplicationTopologyDiscoveryTimeout.AsTimeSpan.TotalMilliseconds,
                    replicationDocument.Destinations))
                {
                    var discoveredNodeTopologies = await clusterTopologyExplorer.DiscoverTopologyAsync();
                    var topology = ReplicationUtils.Merge(discoveredNodeTopologies);

                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(writer, topology.ToJson());
                        writer.Flush();
                    }
                }
            }
        }

        private void WriteEmptyTopology(JsonOperationContext context)
        {
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, new NodeTopologyInfo().ToJson());
                writer.Flush();
                return;
            }
        }
    }
}
