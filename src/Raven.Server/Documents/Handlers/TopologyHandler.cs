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
            using ( var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, new DynamicJsonValue
                {
                    [nameof(Topology.LeaderNode)] = new DynamicJsonValue
                    {
                        [nameof(ServerNode.Url)] = GetStringQueryString("url", required: false) ?? Server.Configuration.Core.ServerUrl,
                        [nameof(ServerNode.Database)] = Database.Name,
                    },
                    [nameof(Topology.Nodes)] = new DynamicJsonArray(),
                    [nameof(Topology.ReadBehavior)] = ReadBehavior.LeaderWithFailoverWhenRequestTimeSlaThresholdIsReached.ToString(),
                    [nameof(Topology.WriteBehavior)] =  WriteBehavior.LeaderOnly.ToString(),
                    [nameof(Topology.SLA)] = new DynamicJsonValue
                    {
                        [nameof(TopologySla.RequestTimeThresholdInMilliseconds)] = 100,
                    },
                    [nameof(Topology.Etag)] = -1,
                });
            }
            return Task.CompletedTask;
        }
    }
}