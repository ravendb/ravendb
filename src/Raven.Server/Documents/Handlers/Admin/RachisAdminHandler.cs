using System;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Server.Rachis;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Admin
{
    public class RachisAdminHandler : AdminRequestHandler
    {

        [RavenAction("/rachis/send", "POST", "/rachis/send")]
        public async Task ApplyCommand()
        {
            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                var command = await context.ReadForMemoryAsync(RequestBodyStream(), "ExternalRachisCommand");

                string type;
                if(command.TryGet("Type",out type) == false)
                {
                    // TODO: maybe add further validation?
                    throw new ArgumentException("Received command must contain a Type field");
                }

                var etag = await ServerStore.PutCommandAsync(command);
                HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        ["ETag"] = etag,
                    });
                    writer.Flush();
                }
            }
        }

        [RavenAction("/admin/cluster/topology", "GET", "/admin/cluster/topology")]
        public Task GetClusterTopology()
        {
            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            using(context.OpenReadTransaction())
            {
                var topology = ServerStore.GetClusterTopology(context);
                HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
                var blit = EntityToBlittable.ConvertEntityToBlittable(topology, DocumentConventions.Default, context);
                (var hasUrl, var nodeTag) = topology.HasUrl(ServerStore.LeaderTag);
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        ["Topology"] = blit,
                        ["Leader"] = hasUrl? nodeTag:"No leader"
                    });
                    writer.Flush();
                }
            }
            return Task.CompletedTask;
        }

        [RavenAction("/admin/cluster/add-node", "POST", "/admin/cluster/add-node?url={nodeUrl:string}")]
        public async Task AddNode()
        {
            var serverUrl = GetStringQueryString("url");
            ServerStore.EnsureNotPassive();
            await ServerStore.AddNodeToClusterAsync(serverUrl);
        }

        [RavenAction("/admin/cluster/remove-node", "DELETE", "/admin/cluster/remove-node?nodeTag={nodeTag:string}")]
        public async Task DeleteNode()
        {
            var serverUrl = GetStringQueryString("nodeTag");
            ServerStore.EnsureNotPassive();
            await ServerStore.RemoveFromClusterAsync(serverUrl);
        }
    }
}