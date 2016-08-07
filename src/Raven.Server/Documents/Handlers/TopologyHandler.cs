using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Server.Routing;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class TopologyHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/topology", "GET")]
        public Task GetTopology()
        {
            JsonOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, new DynamicJsonValue
                {
                    [nameof(Topology.LeaderNode)] = new DynamicJsonValue
                    {
                        [nameof(ServerNode.Url)] = Server.Configuration.Core.ServerUrl,
                        [nameof(ServerNode.Database)] = Database.Name,
                    },
                    [nameof(Topology.Nodes)] = new DynamicJsonArray(),
                    [nameof(Topology.ReadBehavior)] = ReadBehavior.All,
                    [nameof(Topology.WriteBehavior)] =  WriteBehavior.Leader,
                    ["SLA"] = new DynamicJsonValue
                    {
                        ["RequestTimeThresholdInMilliseconds"] = 100,
                    },
                });
            }
            return Task.CompletedTask;
        }
    }
}