using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Documents.Replication;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class TopologyHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/live-topology", "GET", IsDebugInformationEndpoint = true)]
        public async Task GetLiveTopology()
        {
            var sp = Stopwatch.StartNew();

            TransactionOperationContext context;
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                var databaseRecord = Server.ServerStore.Cluster.ReadDatabase(context, Database.Name);
                var clusterTopology = ServerStore.GetClusterTopology(context);
                var replicationDiscoveryTimeout = Database.Configuration
                    .Replication
                    .ReplicationTopologyDiscoveryTimeout
                    .AsTimeSpan;
                using (var clusterTopologyExplorer = new ClusterTopologyExplorer(
                    Database,
                    new List<string>
                    {
                        Database.DbId.ToString()
                    },
                    replicationDiscoveryTimeout,
                    databaseRecord?.Topology.GetDestinations(Server.ServerStore.NodeTag, Database.Name, clusterTopology).ToList()))
                {
                    var topology = await clusterTopologyExplorer.DiscoverTopologyAsync();
                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        var json = topology.ToJson();
                        json["Duration"] = sp.Elapsed.ToString();
                        context.Write(writer, json);
                        writer.Flush();
                    }
                }
            }
        }
    }
}