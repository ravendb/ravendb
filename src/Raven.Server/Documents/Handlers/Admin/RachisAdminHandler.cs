using System;
using System.Collections.Generic;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Exceptions.Cluster;
using Raven.Client.Http;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Operations.Certificates;
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
        [RavenAction("/admin/rachis/send", "POST", AuthorizationStatus.ServerAdmin)]
        public async Task ApplyCommand()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
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

        [RavenAction("/rachis/waitfor", "Get", AuthorizationStatus.ValidUser)]
        public async Task WaitForIndex()
        {
            var index = GetLongQueryString("index");
            try
            {
                await ServerStore.Cluster.WaitForIndexNotification(index);
                HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
            }
            catch
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.RequestTimeout;
            }
        }

        [RavenAction("/admin/cluster/observer/suspend", "GET", AuthorizationStatus.ServerAdmin)]
        public Task SuspendObserver()
        {
            SetupCORSHeaders();
            
            if (ServerStore.IsLeader())
            {
                var suspend = GetBoolValueQueryString("value"); // in seconds
                if (suspend.HasValue)
                {
                    Server.ServerStore.Observer.Suspened = suspend.Value;
                }
                return Task.CompletedTask;
            }
            RedirectToLeader();
            return Task.CompletedTask;
        }

        [RavenAction("/admin/cluster/observer/decisions", "GET", AuthorizationStatus.ServerAdmin)]
        public Task GetObserverDecisions()
        {
            SetupCORSHeaders();

            if (ServerStore.IsLeader())
            {
                using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var res = ServerStore.Observer.ReadDecisionsForDatabase();
                    var json = new DynamicJsonValue{
                        ["LeaderNode"] = Server.ServerStore.NodeTag,
                        ["Term"] = Server.ServerStore.Engine.CurrentTerm,
                        ["Suspended"] = Server.ServerStore.Observer.Suspened,
                        ["Iteration"] = res.Iteration,
                        ["ObserverLog"] = new DynamicJsonArray(res.List)
                    };
                    
                    context.Write(writer, json);
                    writer.Flush();
                    return Task.CompletedTask;
                }
            }
            RedirectToLeader();
            return Task.CompletedTask;
        }

        [RavenAction("/admin/cluster/log", "GET",AuthorizationStatus.ServerAdmin)]
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

        [RavenAction("/cluster/node-info", "GET", AuthorizationStatus.ValidUser)]
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

        [RavenAction("/cluster/topology", "GET", AuthorizationStatus.ValidUser)]
        public Task GetClusterTopology()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
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

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var json = new DynamicJsonValue
                    {
                        ["Topology"] = topology.ToSortedJson(),
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

        [RavenAction("/admin/cluster/maintenance-stats", "GET", AuthorizationStatus.ServerAdmin)]
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

        [RavenAction("/admin/cluster/node", "OPTIONS", AuthorizationStatus.ServerAdmin)]
        [RavenAction("/admin/cluster/reelect", "OPTIONS", AuthorizationStatus.ServerAdmin)]
        [RavenAction("/admin/cluster/timeout", "OPTIONS", AuthorizationStatus.ServerAdmin)]
        [RavenAction("/admin/cluster/promote", "OPTIONS", AuthorizationStatus.ServerAdmin)]
        [RavenAction("/admin/cluster/demote", "OPTIONS", AuthorizationStatus.ServerAdmin)]
        [RavenAction("/admin/cluster/observer/suspend", "OPTIONS", AuthorizationStatus.ServerAdmin)]
        [RavenAction("/admin/cluster/observer/decisions", "OPTIONS", AuthorizationStatus.ServerAdmin)]
        public Task AllowPreflightRequest()
        {
            SetupCORSHeaders();
            HttpContext.Response.Headers.Remove("Content-Type");
            return Task.CompletedTask;
        }

        [RavenAction("/admin/cluster/node", "PUT", AuthorizationStatus.ServerAdmin)]
        public async Task AddNode()
        {
            SetupCORSHeaders();

            var nodeUrl = GetStringQueryString("url");
            var watcher = GetBoolValueQueryString("watcher", false);

            ServerStore.EnsureNotPassive();
            if (ServerStore.IsLeader())
            {
                using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                {
                    var remoteIsHttps = nodeUrl.StartsWith("https:", StringComparison.OrdinalIgnoreCase);

                    if (HttpContext.Request.IsHttps != remoteIsHttps)
                    {
                        throw new InvalidOperationException($"Cannot add node '{nodeUrl}' to cluster because it will create invalid mix of HTTPS & HTTP endpoints. A cluster must be only HTTPS or only HTTP.");
                    }

                    string topologyId;
                    using (ctx.OpenReadTransaction())
                    {
                        var clusterTopology = ServerStore.GetClusterTopology(ctx);
                        var possibleNode = clusterTopology.TryGetNodeTagByUrl(nodeUrl);
                        if (possibleNode.HasUrl)
                        {
                            throw new InvalidOperationException($"Can't add a new node on {nodeUrl} to cluster because this url is already used by node {possibleNode.NodeTag}");
                        }
                        topologyId = clusterTopology.TopologyId;
                    }
                    using (var requestExecutor = ClusterRequestExecutor.CreateForSingleNode(nodeUrl, Server.ServerCertificateHolder.Certificate))
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

                        if (remoteIsHttps)
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

                        
                        await ServerStore.AddNodeToClusterAsync(nodeUrl, nodeTag, validateNotInTopology:false, asWatcher:watcher?? false);
                        NoContentStatus();
                        return;
                    }
                }
            }
            RedirectToLeader();
        }

        [RavenAction("/admin/cluster/node", "DELETE", AuthorizationStatus.ServerAdmin)]
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
        
        [RavenAction("/admin/cluster/timeout", "POST", AuthorizationStatus.ServerAdmin)]
        public Task TimeoutNow()
        {
            SetupCORSHeaders();

            Server.ServerStore.Engine.Timeout.ExecuteTimeoutBehavior();
            NoContentStatus();
            return Task.CompletedTask;
        }


        [RavenAction("/admin/cluster/reelect", "POST", AuthorizationStatus.ServerAdmin)]
        public Task EnforceReelection()
        {
            SetupCORSHeaders();

            if (ServerStore.IsLeader())
            {
                ServerStore.Engine.CurrentLeader.StepDown();
                NoContentStatus();
                return Task.CompletedTask;
            }
            RedirectToLeader();
            return Task.CompletedTask;
        }

        /* Promote a non-voter to a promotable */
        [RavenAction("/admin/cluster/promote", "POST", AuthorizationStatus.ServerAdmin)]
        public async Task PromoteNode()
        {
            if (ServerStore.LeaderTag == null)
            {
                NoContentStatus();
                return;
            }

            SetupCORSHeaders();

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
        [RavenAction("/admin/cluster/demote", "POST", AuthorizationStatus.ServerAdmin)]
        public async Task DemoteNode()
        {
            if (ServerStore.LeaderTag == null)
            {
                NoContentStatus();
                return;
            }

            SetupCORSHeaders();

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
            if(ServerStore.LeaderTag == null)
                throw new NoLeaderException();

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