using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class RemoveNodeFromClusterCommand : CommandBase
    {
        public BlittableJsonReaderObject Topology;
        public string RemovedNode;

        public RemoveNodeFromClusterCommand()
        {
        }

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var json = base.ToJson(context);
            json[nameof(Topology)] = Topology;
            json[nameof(RemovedNode)] = RemovedNode;
            return json;
        }
    }
}
