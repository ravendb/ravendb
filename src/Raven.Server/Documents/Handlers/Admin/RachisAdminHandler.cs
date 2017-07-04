using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Server.Commands;
using Raven.Server.Rachis;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
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
                var commandJson = await context.ReadForMemoryAsync(RequestBodyStream(), "external/rachis/command");

                var command = CommandBase.CreateFrom(commandJson);

                var (etag, result) = await ServerStore.PutCommandAsync(command);
                HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(ServerStore.PutRaftCommandResult.RaftCommandIndex)] = etag,
                        [nameof(ServerStore.PutRaftCommandResult.Data)] = result
                    });
                    writer.Flush();
                }
            }
        }

        [RavenAction("/admin/cluster/log", "GET", "/admin/cluster/log")]
        public Task GetLogs()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.OpenReadTransaction();
                context.Write(writer, ServerStore.GetLogDetails(context));
                writer.Flush();
            }
            return Task.CompletedTask;
        }

        [RavenAction("/admin/cluster/node-info", "GET", "/admin/cluster/node-info")]
        public Task GetNodeInfo()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var json = new DynamicJsonValue();
                using (context.OpenReadTransaction())
                {
                    json[nameof(NodeInfo.NodeTag)] = ServerStore.NodeTag;
                    json[nameof(NodeInfo.TopologyId)] = ServerStore.GetClusterTopology(context).TopologyId;
                    json[nameof(NodeInfo.PublicKey)] = Convert.ToBase64String(ServerStore.SignPublicKey);
                    json[nameof(ServerStore.ClusterStatus)] = ServerStore.ClusterStatus();
                }
                context.Write(writer, json);
                writer.Flush();
            }
            return Task.CompletedTask;
        }

        [RavenAction("/admin/cluster/topology", "GET", "/admin/cluster/topology")]
        public Task GetClusterTopology()
        {
            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {

                var topology = ServerStore.GetClusterTopology(context);
                var nodeTag = ServerStore.NodeTag;

                if (topology.Members.Count == 0)
                {
                    var tag = ServerStore.NodeTag ?? "A";
                    var serverUrl = ServerStore.NodeHttpServerUrl;
                    var publicKey = ServerStore.SignPublicKey;

                    topology = new ClusterTopology(
                        "dummy",
                        null,
                        new Dictionary<string, string>
                        {
                            [tag] = serverUrl
                        },
                        new Dictionary<string, string>(),
                        new Dictionary<string, string>(),
                        new Dictionary<string, string>
                        {
                            [tag] = Convert.ToBase64String(publicKey)
                        },
                        tag
                    );
                    nodeTag = tag;
                }
                HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

                topology.Members[ServerStore.NodeTag] = ServerStore.NodeHttpServerUrl;
                var blit = EntityToBlittable.ConvertEntityToBlittable(topology, DocumentConventions.Default, context);

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var json = new DynamicJsonValue
                    {
                        ["Topology"] = blit,
                        ["Leader"] = ServerStore.LeaderTag,
                        ["CurrentState"] = ServerStore.CurrentState,
                        ["NodeTag"] = nodeTag,
                        ["CurrentTerm"] = ServerStore.Engine.CurrentTerm,
                        [nameof(ServerStore.ClusterStatus)] = ServerStore.ClusterStatus()
                    };
                    var clusterErrors = ServerStore.GetClusterErrors();
                    if (clusterErrors.Count > 0)
                        json["Errors"] = clusterErrors;

                    var nodesStatues = ServerStore.GetNodesStatuses();
                    json["Status"] = DynamicJsonValue.Convert(nodesStatues);

                    context.Write(writer, json);
                    writer.Flush();
                }
            }

            return Task.CompletedTask;
        }

        [RavenAction("/cluster/maintenance-stats", "GET", "/cluster/maintenance-stats")]
        public Task ClusterMaintenanceStats()
        {
            if (ServerStore.LeaderTag == null)
            {
                return Task.CompletedTask;
            }
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                if (ServerStore.IsLeader())
                {
                    context.Write(writer, DynamicJsonValue.Convert(ServerStore.ClusterMaintenanceSupervisor?.GetStats()));
                    writer.Flush();
                    return Task.CompletedTask;
                }
                RedirectToLeader();
            }
            return Task.CompletedTask;
        }

        private void SetupCORSHeaders()
        {
            // TODO: handle this properly when using https
            // https://developer.mozilla.org/en-US/docs/Web/HTTP/Access_control_CORS
            HttpContext.Response.Headers.Add("Access-Control-Allow-Origin", HttpContext.Request.Headers["Origin"]);
            HttpContext.Response.Headers.Add("Access-Control-Allow-Methods", "POST, GET, OPTIONS, DELETE");
            HttpContext.Response.Headers.Add("Access-Control-Allow-Headers", HttpContext.Request.Headers["Access-Control-Request-Headers"]);
            HttpContext.Response.Headers.Add("Access-Control-Max-Age", "86400");
        }

        [RavenAction("/admin/cluster/add-node", "OPTIONS", "/admin/cluster/add-node?url={nodeUrl:string}")]
        [RavenAction("/admin/cluster/remove-node", "OPTIONS", "/admin/cluster/remove-node?nodeTag={nodeTag:string}")]
        [RavenAction("/admin/cluster/reelect", "OPTIONS", "/admin/cluster/reelect")]
        public Task AllowPreflightRequest()
        {
            SetupCORSHeaders();
            HttpContext.Response.Headers.Remove("Content-Type");
            return Task.CompletedTask;
        }

        [RavenAction("/admin/cluster/add-node", "POST", "/admin/cluster/add-node?url={nodeUrl:string}")]
        public async Task AddNode()
        {
            SetupCORSHeaders();

            var serverUrl = GetStringQueryString("url");
            ServerStore.EnsureNotPassive();
            if (ServerStore.IsLeader())
            {
                using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                {
                    string apiKey;
                    string topologyId;
                    using (ctx.OpenReadTransaction())
                    {
                        var clusterTopology = ServerStore.GetClusterTopology(ctx);
                        apiKey = clusterTopology.ApiKey;
                        topologyId = clusterTopology.TopologyId;
                    }
                    using (var requestExecutor = ClusterRequestExecutor.CreateForSingleNode(serverUrl, apiKey))
                    {
                        requestExecutor.ClusterToken = ServerStore.GetClusterTokenForNode(ctx);
                        var infoCmd = new GetNodeInfoCommand();
                        await requestExecutor.ExecuteAsync(infoCmd, ctx);
                        var nodeInfo = infoCmd.Result;

                        if (nodeInfo.TopologyId != null && topologyId != nodeInfo.TopologyId)
                        {
                            throw new TopologyMismatchException(
                                $"Adding a new node to cluster failed. The new node is already in another cluster. Expected topology id: {topologyId}, but we get {nodeInfo.TopologyId}");
                        }

                        var nodeTag = nodeInfo.NodeTag == "?" 
                            ? null : nodeInfo.NodeTag;
                        await ServerStore.AddNodeToClusterAsync(serverUrl, Convert.FromBase64String(nodeInfo.PublicKey), nodeTag, validateNotInTopology:false);
                        NoContentStatus();
                        return;
                    }
                }
            }
            RedirectToLeader();
        }

        [RavenAction("/admin/cluster/remove-node", "DELETE", "/admin/cluster/remove-node?nodeTag={nodeTag:string}")]
        public async Task DeleteNode()
        {
            SetupCORSHeaders();

            var nodeTag = GetStringQueryString("nodeTag");
            ServerStore.EnsureNotPassive();
            if (ServerStore.IsLeader())
            {
                await ServerStore.RemoveFromClusterAsync(nodeTag);
                NoContentStatus();
                return;
            }
            RedirectToLeader();
        }

        [RavenAction("/admin/cluster/reelect", "POST", "/admin/cluster/reelect")]
        public Task EnforceReelection()
        {
            SetupCORSHeaders();

            if (ServerStore.IsLeader())
            {
                ServerStore.Engine.CurrentLeader.StepDown();
                return Task.CompletedTask;
            }
            RedirectToLeader();
            return Task.CompletedTask;
        }

        private void RedirectToLeader()
        {
            ClusterTopology topology;
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                topology = ServerStore.GetClusterTopology(context);
            }
            var url = topology.GetUrlFromTag(ServerStore.LeaderTag);
            var leaderLocation = url + HttpContext.Request.Path + HttpContext.Request.QueryString;
            HttpContext.Response.StatusCode = (int)HttpStatusCode.TemporaryRedirect;
            HttpContext.Response.Headers.Remove("Content-Type");
            HttpContext.Response.Headers.Add("Location", leaderLocation);
        }
    }
}