using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Rachis;
using Raven.Server.Routing;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Debugging
{
    class NodeDebugHandler : RequestHandler
    {
        [RavenAction("/admin/debug/node/remote-connections", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public Task ListRemoteConnections()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var write = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(write,
                    new DynamicJsonValue
                    {
                        ["Remote-Connections"] = new DynamicJsonArray(RemoteConnection.RemoteConnectionsList
                            .Select(connection => new DynamicJsonValue
                            {
                                [nameof(RemoteConnection.RemoteConnectionInfo.Caller)] = connection.Caller,
                                [nameof(RemoteConnection.RemoteConnectionInfo.Term)] = connection.Term,
                                [nameof(RemoteConnection.RemoteConnectionInfo.Destination)] = connection.Destination,
                                [nameof(RemoteConnection.RemoteConnectionInfo.StartAt)] = connection.StartAt,
                                ["Duration"] = DateTime.UtcNow - connection.StartAt,
                                [nameof(RemoteConnection.RemoteConnectionInfo.Number)] = connection.Number,
                            }))
                    });
                write.Flush();
            }
            return Task.CompletedTask;
        }
    }
}
