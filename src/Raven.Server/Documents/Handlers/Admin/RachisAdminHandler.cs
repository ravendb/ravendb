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

        [RavenAction("/rachis/admin/cluster-topology", "GET", "/rachis/cluster-topology")]
        public Task GetClusterTopology()
        {
            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            using(context.OpenReadTransaction())
            {
                var topology = ServerStore.GetClusterTopology(context);
                HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
                var blit = EntityToBlittable.ConvertEntityToBlittable(topology, DocumentConventions.Default, context);
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        ["Topology"] = blit,
                    });
                    writer.Flush();
                }
            }
            return Task.CompletedTask;
        }

        [RavenAction("/rachis/admin/add-node", "GET", "/rachis/add-node?url={nodeUrl:string}")]
        //[RavenAction("/rachis/add-node", "POST", "/rachis/add-node?url={nodeUrl:string}")]
        public async Task AddNode()
        {
            var serverUrl = GetStringQueryString("url");
            ServerStore.EnsureNotPassive();
            await ServerStore.AddNodeToClusterAsync(serverUrl);
        }

        [RavenAction("/rachis/admin/remove-node", "GET", "/rachis/remove-node?nodeTag={nodeTag:string}")]
        //[RavenAction("/rachis/remove-node", "DELETE", "/rachis/remove-node?url={nodeUrl:string}")]
        public async Task DeleteNode()
        {
            var serverUrl = GetStringQueryString("nodeTag");
            ServerStore.EnsureNotPassive();
            await ServerStore.RemoveFromClusterAsync(serverUrl);
        }
    }
}