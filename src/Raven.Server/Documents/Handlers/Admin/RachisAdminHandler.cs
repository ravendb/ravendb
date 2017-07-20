using System;
using System.Collections.Generic;
using System.Net;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Security;
using Raven.Client.Http;
using Raven.Client.Server.Commands;
using Raven.Client.Server.Operations.Certificates;
using Raven.Server.Rachis;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Admin
{
    public class RachisAdminHandler : RequestHandler
    {
        [RavenAction("/rachis/send", "POST", "/rachis/send", RequiredAuthorization = AuthorizationStatus.ServerAdmin)]
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

        [RavenAction("/admin/cluster/log", "GET", "/admin/cluster/log", RequiredAuthorization = AuthorizationStatus.ServerAdmin)]
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

        [RavenAction("/cluster/node-info", "GET", "/cluster/node-info", RequiredAuthorization = AuthorizationStatus.ValidUser)]
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
                    json[nameof(NodeInfo.Certificate)] = ServerStore.RavenServer.ServerCertificateHolder.CertificateForClients;
                    json[nameof(ServerStore.ClusterStatus)] = ServerStore.ClusterStatus();
                }
                context.Write(writer, json);
                writer.Flush();
            }
            return Task.CompletedTask;
        }

        [RavenAction("/cluster/topology", "GET", "/cluster/topology", RequiredAuthorization = AuthorizationStatus.ValidUser)]
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

                    topology = new ClusterTopology(
                        "dummy",
                        new Dictionary<string, string>
                        {
                            [tag] = serverUrl
                        },
                        new Dictionary<string, string>(),
                        new Dictionary<string, string>(),
                        tag
                    );
                    nodeTag = tag;
                }
                HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

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

        [RavenAction("/admin/cluster/maintenance-stats", "GET", "/cluster/maintenance-stats", RequiredAuthorization = AuthorizationStatus.ServerAdmin)]
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

        [RavenAction("/admin/cluster/add-node", "OPTIONS", "/admin/cluster/add-node?url={nodeUrl:string}", RequiredAuthorization = AuthorizationStatus.ValidUser)]
        [RavenAction("/admin/cluster/remove-node", "OPTIONS", "/admin/cluster/remove-node?nodeTag={nodeTag:string}", RequiredAuthorization = AuthorizationStatus.ValidUser)]
        [RavenAction("/admin/cluster/reelect", "OPTIONS", "/admin/cluster/reelect", RequiredAuthorization = AuthorizationStatus.ValidUser)]
        [RavenAction("/admin/cluster/timeout", "OPTIONS", "/admin/cluster/timeout", RequiredAuthorization = AuthorizationStatus.ValidUser)]
        
        public Task AllowPreflightRequest()
        {
            SetupCORSHeaders();
            HttpContext.Response.Headers.Remove("Content-Type");
            return Task.CompletedTask;
        }

        [RavenAction("/admin/cluster/add-node", "POST", "/admin/cluster/add-node?url={nodeUrl:string}&expectedThumbrpint={thumbprint:string}", RequiredAuthorization = AuthorizationStatus.ServerAdmin)]
        public async Task AddNode()
        {
            SetupCORSHeaders();

            var serverUrl = GetStringQueryString("url");
            ServerStore.EnsureNotPassive();
            if (ServerStore.IsLeader())
            {
                using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                {
                    string topologyId;
                    using (ctx.OpenReadTransaction())
                    {
                        var clusterTopology = ServerStore.GetClusterTopology(ctx);
                        topologyId = clusterTopology.TopologyId;
                    }
                    using (var requestExecutor = ClusterRequestExecutor.CreateForSingleNode(serverUrl, Server.ServerCertificateHolder.Certificate))
                    {
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

                        if (serverUrl.StartsWith("https:", StringComparison.OrdinalIgnoreCase))
                        {
                            if (nodeInfo.Certificate == null)
                                throw  new InvalidOperationException($"Cannot add node {nodeTag} to cluster because it has no certificate while trying to use HTTPS");

                            var certificate = new X509Certificate2(Convert.FromBase64String(nodeInfo.Certificate));
                            
                            if (certificate.NotBefore > DateTime.UtcNow)
                                throw new InvalidOperationException($"Cannot add node {nodeTag} to cluster because its certificate '{certificate.FriendlyName}' is not yet valid. It starts on {certificate.NotBefore}");

                            if (certificate.NotAfter < DateTime.UtcNow)
                                throw new InvalidOperationException($"Cannot add node {nodeTag} to cluster because its certificate '{certificate.FriendlyName}' expired on {certificate.NotAfter}");

                            var expected = GetStringQueryString("expectedThumbrpint", required: false);
                            if (expected != null)
                            {
                                if (certificate.Thumbprint != expected)
                                    throw new InvalidOperationException($"Cannot add node {nodeTag} to cluster because its certificate thumbprint '{certificate.Thumbprint}' doesn't match the expected thumbprint '{expected}'.");
                            }

                            var certificateDefinition = new CertificateDefinition
                            {
                                Certificate = nodeInfo.Certificate,
                                Thumbprint = certificate.Thumbprint
                            };

                            var res = await ServerStore.PutValueInClusterAsync(new PutCertificateCommand(Constants.Certificates.Prefix + certificate.Thumbprint, certificateDefinition));
                            await ServerStore.Cluster.WaitForIndexNotification(res.Etag);
                        }

                        
                        await ServerStore.AddNodeToClusterAsync(serverUrl, nodeTag, validateNotInTopology:false);
                        NoContentStatus();
                        return;
                    }
                }
            }
            RedirectToLeader();
        }

        [RavenAction("/admin/cluster/remove-node", "DELETE", "/admin/cluster/remove-node?nodeTag={nodeTag:string}", RequiredAuthorization = AuthorizationStatus.ServerAdmin)]
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
        
        [RavenAction("/admin/cluster/timeout", "POST", "/admin/cluster/timeout")]
        public Task TimeoutNow()
        {
            SetupCORSHeaders();

            Server.ServerStore.Engine.Timeout.ExecuteTimeoutBehavior();
            return Task.CompletedTask;
        }


        [RavenAction("/admin/cluster/reelect", "POST", "/admin/cluster/reelect", RequiredAuthorization = AuthorizationStatus.ServerAdmin)]
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

        /* Promote a non-voter to a promotable */
        [RavenAction("/admin/cluster/promote", "POST", "/admin/cluster/promote?nodeTag={nodeTag:string}")]
        public async Task PromoteNode()
        {
            if (ServerStore.LeaderTag == null)
            {
                NoContentStatus();
                return;
            }
            
            if (ServerStore.IsLeader() == false)
            {
                RedirectToLeader();
                return;
            }

            var nodeTag = GetStringQueryString("nodeTag");
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var topology = ServerStore.GetClusterTopology(context);
                if (topology.Watchers.ContainsKey(nodeTag) == false)
                {
                    throw new InvalidOperationException(
                        $"Failed to promote node {nodeTag} beacuse {nodeTag} is not a watcher in the cluster topology");
                }

                var url = topology.GetUrlFromTag(nodeTag);
                await ServerStore.Engine.ModifyTopologyAsync(nodeTag, url, Leader.TopologyModification.Promotable);
                NoContentStatus();
            }
        }

        /* Demote a voter (member/promotable) node to a non-voter  */
        [RavenAction("/admin/cluster/demote", "POST", "/admin/cluster/demote?nodeTag={nodeTag:string}")]
        public async Task DemoteNode()
        {
            if (ServerStore.LeaderTag == null)
            {
                NoContentStatus();
                return;
            }

            if (ServerStore.IsLeader() == false)
            {
                RedirectToLeader();
                return;
            }

            var nodeTag = GetStringQueryString("nodeTag");
            if (nodeTag == ServerStore.LeaderTag)
            {
                throw new InvalidOperationException(
                    $"Failed to demote node {nodeTag} beacuse {nodeTag} is the current leader in the cluster topology. In order to demote {nodeTag} perform a Step-Down first");
            }

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var topology = ServerStore.GetClusterTopology(context);
                if (topology.Promotables.ContainsKey(nodeTag) == false && topology.Members.ContainsKey(nodeTag) == false)
                {
                    throw new InvalidOperationException(
                        $"Failed to demote node {nodeTag} beacuse {nodeTag} is not a voter in the cluster topology");
                }

                var url = topology.GetUrlFromTag(nodeTag);
                await ServerStore.Engine.ModifyTopologyAsync(nodeTag, url, Leader.TopologyModification.NonVoter);
                NoContentStatus();
            }           
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