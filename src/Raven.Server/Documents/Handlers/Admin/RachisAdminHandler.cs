﻿using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Conventions;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Cluster;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.Util;
using Raven.Server.Commercial;
using Raven.Server.Extensions;
using Raven.Server.Json;
using Raven.Server.Rachis;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Maintenance;
using Raven.Server.Storage.Schema;
using Raven.Server.TrafficWatch;
using Raven.Server.Utils;
using Raven.Server.Web;
using Raven.Server.Web.System;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Documents.Handlers.Admin
{
    public sealed class RachisAdminHandler : ServerRequestHandler
    {
        [RavenAction("/admin/rachis/send", "POST", AuthorizationStatus.Operator)]
        public async Task ApplyCommand()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                if (ServerStore.IsLeader() == false)
                {
                    throw new NoLeaderException("Not a leader, cannot accept commands.");
                }

                HttpContext.Response.Headers["Reached-Leader"] = "true";

                var commandJson = await context.ReadForMemoryAsync(RequestBodyStream(), "external/rachis/command");
                try
                {
                    var command = CommandBase.CreateFrom(commandJson);
                    if (command is IBlittableResultCommand blittableResultCommand)
                        blittableResultCommand.ContextToWriteResult = context;
                    
                    if (TrafficWatchManager.HasRegisteredClients)
                        AddStringToHttpContext(commandJson.ToString(), TrafficWatchChangeType.ClusterCommands);

                    var isClusterAdmin = IsClusterAdmin();
                    command.VerifyCanExecuteCommand(ServerStore, context, isClusterAdmin);

                    var (etag, result) = await ServerStore.Engine.PutToLeaderAsync(command);
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
                    var ms = context.CheckoutMemoryStream();
                    try
                    {
                        await using (var writer = new AsyncBlittableJsonTextWriter(context, ms))
                        {
                            context.Write(writer, new DynamicJsonValue
                            {
                                [nameof(PutRaftCommand.PutRaftCommandResult.RaftCommandIndex)] = etag,
                                [nameof(PutRaftCommand.PutRaftCommandResult.Data)] = result,
                            });
                        }

                        // now that we know that we properly serialized it
                        ms.Position = 0;
                        await ms.CopyToAsync(ResponseBodyStream());
                    }
                    finally
                    {
                        context.ReturnMemoryStream(ms);
                    }
                }
                catch (TermValidationException)
                {
                    HttpContext.Response.Headers["Reached-Leader"] = "false";
                    throw;
                }
                catch (NotLeadingException e)
                {
                    HttpContext.Response.Headers["Reached-Leader"] = "false";
                    throw new NoLeaderException("Lost the leadership, cannot accept commands.", e);
                }
                catch (InvalidOperationException e)
                {
                    RequestRouter.AssertClientVersion(HttpContext, e);
                    throw;
                }
            }
        }

        [RavenAction("/rachis/waitfor", "Get", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task WaitForIndex()
        {
            var index = GetLongQueryString("index");
            await ServerStore.Cluster.WaitForIndexNotification(index);
        }

        [RavenAction("/admin/cluster/observer/suspend", "POST", AuthorizationStatus.Operator, CorsMode = CorsMode.Cluster)]
        public Task SuspendObserver()
        {
            if (ServerStore.IsLeader())
            {
                var suspend = GetBoolValueQueryString("value");
                if (suspend.HasValue)
                {
                    Server.ServerStore.Observer.Suspended = suspend.Value;
                }

                NoContentStatus();
                return Task.CompletedTask;
            }

            RedirectToLeader();

            return Task.CompletedTask;
        }

        [RavenAction("/admin/cluster/observer/decisions", "GET", AuthorizationStatus.Operator, CorsMode = CorsMode.Cluster, IsDebugInformationEndpoint = true)]
        public async Task GetObserverDecisions()
        {
            if (ServerStore.IsLeader())
            {
                using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, ResponseBodyStream()))
                {
                    var res = ServerStore.Observer.ReadDecisionsForDatabase();
                    var json = new DynamicJsonValue
                    {
                        [nameof(ClusterObserverDecisions.LeaderNode)] = Server.ServerStore.NodeTag,
                        [nameof(ClusterObserverDecisions.Term)] = Server.ServerStore.Engine.CurrentLeader?.Term,
                        [nameof(ClusterObserverDecisions.Suspended)] = Server.ServerStore.Observer.Suspended,
                        [nameof(ClusterObserverDecisions.Iteration)] = res.Iteration,
                        [nameof(ClusterObserverDecisions.ObserverLog)] = new DynamicJsonArray(res.List)
                    };

                    context.Write(writer, json);
                    return;
                }
            }
            RedirectToLeader();
        }

        [RavenAction("/admin/cluster/log", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public async Task GetLogs()
        {
            using (var processor = new RachisAdminHandlerProcessorForGetClusterLogs(this)) 
                await processor.ExecuteAsync();
        }

        [RavenAction("/admin/debug/cluster/history-logs", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public async Task GetHistoryLogs()
        {
            using (ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                context.OpenReadTransaction();
                writer.WriteArray("RachisLogHistory", ServerStore.Engine.LogHistory.GetHistoryLogs(context), context);
                writer.WriteEndObject();
            }
        }

        [RavenAction("/cluster/node-info", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetNodeInfo()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var nodeInfo = ServerStore.GetNodeInfo();
                var json = nodeInfo.ToJson();
                json[nameof(ServerStore.Engine.LastStateChangeReason)] = ServerStore.LastStateChangeReason();

                context.Write(writer, json);
            }
        }

        [RavenAction("/cluster/topology", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true, CheckForChanges = false)]
        public async Task GetClusterTopology()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var topology = ServerStore.GetClusterTopology(context);
                var nodeTag = ServerStore.NodeTag;

                if (topology.AllNodes.Count == 0)
                {
                    var tag = ServerStore.NodeTag ?? "A";
                    var serverUrl = ServerStore.GetNodeHttpServerUrl(HttpContext.Request.GetClientRequestedNodeUrl());

                    topology = new ClusterTopology(
                        topology.TopologyId ?? "dummy",
                        new Dictionary<string, string>
                        {
                            [tag] = serverUrl
                        },
                        new Dictionary<string, string>(),
                        new Dictionary<string, string>(),
                        tag,
                        -1L
                    );
                    nodeTag = tag;
                }
                else
                {
                    var isClientIndependent = GetBoolValueQueryString("clientIndependent", false) ?? false;
                    if (isClientIndependent == false && HttpContext.Items.TryGetValue(nameof(LocalEndpointClient.DebugPackage), out var _) == false)
                        topology.ReplaceCurrentNodeUrlWithClientRequestedNodeUrlIfNecessary(ServerStore, HttpContext);
                }

                HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

                await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, ResponseBodyStream()))
                {
                    var loadLicenseLimits = ServerStore.LoadLicenseLimits();
                    var nodeLicenseDetails = loadLicenseLimits == null ?
                        null : DynamicJsonValue.Convert(loadLicenseLimits.NodeLicenseDetails);
                    var json = new DynamicJsonValue
                    {
                        [nameof(ClusterTopologyResponse.Topology)] = topology.ToSortedJson(),
                        [nameof(ClusterTopologyResponse.Etag)] = topology.Etag,
                        [nameof(ClusterTopologyResponse.Leader)] = ServerStore.LeaderTag,
                        ["LeaderShipDuration"] = ServerStore.Engine.CurrentLeader?.LeaderShipDuration,
                        ["CurrentState"] = ServerStore.CurrentRachisState,
                        [nameof(ClusterTopologyResponse.NodeTag)] = nodeTag,
                        [nameof(ClusterTopologyResponse.ServerRole)] = topology.GetServerRoleForTag(nodeTag),
                        ["CurrentTerm"] = ServerStore.Engine.CurrentCommittedState.Term,
                        ["NodeLicenseDetails"] = nodeLicenseDetails,
                        [nameof(ServerStore.Engine.LastStateChangeReason)] = ServerStore.LastStateChangeReason()
                    };
                    var clusterErrors = ServerStore.GetClusterErrors();
                    if (clusterErrors.Count > 0)
                        json["Errors"] = clusterErrors;

                    var nodesStatues = ServerStore.GetNodesStatuses();
                    json["Status"] = DynamicJsonValue.Convert(nodesStatues);

                    context.Write(writer, json);
                }
            }
        }

        [RavenAction("/admin/cluster/maintenance-stats", "GET", AuthorizationStatus.Operator)]
        public async Task ClusterMaintenanceStats()
        {
            if (ServerStore.LeaderTag == null)
            {
                return;
            }

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                if (ServerStore.IsLeader())
                {
                    context.Write(writer, DynamicJsonValue.Convert(ServerStore.ClusterMaintenanceSupervisor?.GetStats()));
                    await writer.FlushAsync();
                    return;
                }
                RedirectToLeader();
            }
        }

        [RavenAction("/admin/cluster/bootstrap", "POST", AuthorizationStatus.ClusterAdmin)]
        public async Task Bootstrap()
        {
            var tag = GetStringQueryString("tag", false);
            if (tag == null)
            {
                await ServerStore.EnsureNotPassiveAsync();
            }
            else
            {
                await ServerStore.EnsureNotPassiveAsync(nodeTag: tag);
            }
            NoContentStatus();
        }

        [RavenAction("/admin/cluster/node", "PUT", AuthorizationStatus.ClusterAdmin, CorsMode = CorsMode.Cluster)]
        public async Task AddNode()
        {
            var nodeUrl = GetQueryStringValueAndAssertIfSingleAndNotEmpty("url");
            var tag = GetStringQueryString("tag", false);
            var watcher = GetBoolValueQueryString("watcher", false);
            var raftRequestId = GetRaftRequestIdFromQuery();

            var maxUtilizedCores = GetIntValueQueryString("maxUtilizedCores", false);
            if (maxUtilizedCores != null && maxUtilizedCores <= 0)
                throw new ArgumentException("Max utilized cores cores must be greater than 0");

            nodeUrl = nodeUrl.Trim();
            if (Uri.IsWellFormedUriString(nodeUrl, UriKind.Absolute) == false)
                throw new InvalidOperationException($"Given node URL '{nodeUrl}' is not in a correct format.");

            nodeUrl = UrlHelper.TryGetLeftPart(nodeUrl);
            var remoteIsHttps = nodeUrl.StartsWith("https:", StringComparison.OrdinalIgnoreCase);

            if (HttpContext.Request.IsHttps != remoteIsHttps)
            {
                throw new InvalidOperationException($"Cannot add node '{nodeUrl}' to cluster because it will create invalid mix of HTTPS & HTTP endpoints. A cluster must be only HTTPS or only HTTP.");
            }

            tag = tag?.Trim();

            Client.ServerWide.Commands.NodeInfo nodeInfo;
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (var requestExecutor = ClusterRequestExecutor.CreateForShortTermUse(nodeUrl, Server.Certificate.Certificate, DocumentConventions.DefaultForServer))
            {
                requestExecutor.DefaultTimeout = ServerStore.Engine.OperationTimeout;

                // test connection to remote.
                var result = await ServerStore.TestConnectionToRemote(nodeUrl, database: null);
                if (result.Success == false)
                {
                    throw new InvalidOperationException(result.Error);
                }

                // test connection from remote to destination
                result = await ServerStore.TestConnectionFromRemote(requestExecutor, ctx, nodeUrl);
                if (result.Success == false)
                    throw new InvalidOperationException(result.Error);

                var infoCmd = new GetNodeInfoCommand();
                try
                {
                    await requestExecutor.ExecuteAsync(infoCmd, ctx);
                }
                catch (AllTopologyNodesDownException e)
                {
                    throw new InvalidOperationException($"Couldn't contact node at {nodeUrl}", e);
                }

                nodeInfo = infoCmd.Result;

                if (SchemaUpgrader.CurrentVersion.ServerVersion != nodeInfo.ServerSchemaVersion)
                {
                    var nodesVersion = nodeInfo.ServerSchemaVersion == 0 ? "Pre 4.2 version" : nodeInfo.ServerSchemaVersion.ToString();
                    throw new InvalidOperationException($"Can't add node with mismatched storage schema version.{Environment.NewLine}" +
                                                        $"My version is {SchemaUpgrader.CurrentVersion.ServerVersion}, while node's version is {nodesVersion}");
                }

                if (ServerStore.IsPassive() && nodeInfo.TopologyId != null)
                {
                    throw new TopologyMismatchException("You can't add new node to an already existing cluster");
                }
            }

            if (ServerStore.ValidateFixedPort && nodeInfo.HasFixedPort == false)
            {
                throw new InvalidOperationException($"Failed to add node '{nodeUrl}' to cluster. " +
                                                    $"Node '{nodeUrl}' has port '0' in 'Configuration.Core.ServerUrls' setting. " +
                                                    "Adding a node with non fixed port is forbidden. Define a fixed port for the node to enable cluster creation.");
            }

            await ServerStore.EnsureNotPassiveAsync();

            ServerStore.LicenseManager.AssertCanAddNode();

            if (ServerStore.IsLeader())
            {
                using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                {
                    var clusterTopology = ServerStore.GetClusterTopology();

                    var possibleNode = clusterTopology.TryGetNodeTagByUrl(nodeUrl);
                    if (possibleNode.HasUrl)
                    {
                        throw new InvalidOperationException($"Can't add a new node on {nodeUrl} to cluster because this url is already used by node {possibleNode.NodeTag}");
                    }

                    if (nodeInfo.ServerId == ServerStore.GetServerId())
                        throw new InvalidOperationException($"Can't add a new node on {nodeUrl} to cluster because it's a synonym of the current node URL:{ServerStore.GetNodeHttpServerUrl()}");

                    if (nodeInfo.TopologyId != null)
                    {
                        AssertCanAddNodeWithTopologyId(clusterTopology, nodeInfo, nodeUrl);
                    }

                    var nodeTag = nodeInfo.NodeTag == RachisConsensus.InitialTag ? tag : nodeInfo.NodeTag;
                    CertificateDefinition oldServerCert = null;
                    X509Certificate2 certificate = null;

                    if (remoteIsHttps)
                    {
                        if (nodeInfo.Certificate == null)
                            throw new InvalidOperationException($"Cannot add node {nodeTag} with url {nodeUrl} to cluster because it has no certificate while trying to use HTTPS");

                        certificate = CertificateLoaderUtil.CreateCertificate(Convert.FromBase64String(nodeInfo.Certificate));

                        var now = DateTime.UtcNow;
                        if (certificate.NotBefore.ToUniversalTime() > now)
                        {
                            // Because of time zone and time drift issues, we can't assume that the certificate generation will be
                            // proper. Because of that, we allow tolerance of the NotBefore to be a bit earlier / later than the
                            // current time. Clients may still fail to work with our certificate because of timing issues,
                            // but the admin needs to setup time sync properly and there isn't much we can do at that point
                            if ((certificate.NotBefore.ToUniversalTime() - now).TotalDays > 1)
                                throw new InvalidOperationException(
                                    $"Cannot add node {nodeTag} with url {nodeUrl} to cluster because its certificate '{certificate.FriendlyName}' is not yet valid. It starts on {certificate.NotBefore}");
                        }

                        if (certificate.NotAfter.ToUniversalTime() < now)
                            throw new InvalidOperationException($"Cannot add node {nodeTag} with url {nodeUrl} to cluster because its certificate '{certificate.FriendlyName}' expired on {certificate.NotAfter}");

                        var expected = GetStringQueryString("expectedThumbprint", required: false);
                        if (expected != null)
                        {
                            if (certificate.Thumbprint != expected)
                                throw new InvalidOperationException($"Cannot add node {nodeTag} with url {nodeUrl} to cluster because its certificate thumbprint '{certificate.Thumbprint}' doesn't match the expected thumbprint '{expected}'.");
                        }

                        // if it's the same server certificate as our own, we don't want to add it to the cluster
                        if (certificate.Thumbprint != Server.Certificate.Certificate.Thumbprint)
                        {
                            using (ctx.OpenReadTransaction())
                            {
                                var readCert = ServerStore.Cluster.GetCertificateByThumbprint(ctx, certificate.Thumbprint);
                                if (readCert != null)
                                    oldServerCert = JsonDeserializationServer.CertificateDefinition(readCert);
                            }

                            if (oldServerCert == null)
                            {
                                var certificateDefinition = new CertificateDefinition
                                {
                                    Certificate = nodeInfo.Certificate,
                                    Thumbprint = certificate.Thumbprint,
                                    PublicKeyPinningHash = certificate.GetPublicKeyPinningHash(),
                                    NotAfter = certificate.NotAfter,
                                    NotBefore = certificate.NotBefore,
                                    Name = "Server Certificate for " + nodeUrl,
                                    SecurityClearance = SecurityClearance.ClusterNode
                                };

                                var res = await ServerStore.PutValueInClusterAsync(new PutCertificateCommand(certificate.Thumbprint, certificateDefinition,
                                    $"{raftRequestId}/put-new-certificate"));
                                await ServerStore.Cluster.WaitForIndexNotification(res.Index);
                            }
                        }
                    }

                    await ServerStore.AddNodeToClusterAsync(nodeUrl, nodeTag, validateNotInTopology: true, asWatcher: watcher ?? false);

                    if (RavenLogManager.Instance.IsAuditEnabled)
                        LogAuditFor("Server", "ADD", $"Node {nodeTag} to cluster. Term: {ServerStore.Engine.CurrentCommittedState.Term}.");



                    using (ctx.OpenReadTransaction())
                    {
                        clusterTopology = ServerStore.GetClusterTopology(ctx);
                        possibleNode = clusterTopology.TryGetNodeTagByUrl(nodeUrl);
                        nodeTag = possibleNode.HasUrl ? possibleNode.NodeTag : null;

                        if (certificate != null && certificate.Thumbprint != Server.Certificate.Certificate.Thumbprint)
                        {
                            var modifiedServerCert = JsonDeserializationServer.CertificateDefinition(ServerStore.Cluster.GetCertificateByThumbprint(ctx, certificate.Thumbprint));

                            if (modifiedServerCert == null)
                                throw new ConcurrencyException("After adding the certificate, it was removed, shouldn't happen unless another admin removed it midway through.");

                            if (oldServerCert == null)
                            {
                                modifiedServerCert.Name = "Server certificate for Node " + nodeTag;
                            }
                            else
                            {
                                var value = "Node " + nodeTag;
                                if (modifiedServerCert.Name.Contains(value) == false)
                                    modifiedServerCert.Name += ", " + value;
                            }

                            var res = await ServerStore.PutValueInClusterAsync(new PutCertificateCommand(certificate.Thumbprint, modifiedServerCert, $"{raftRequestId}/put-modified-certificate"));
                            await ServerStore.Cluster.WaitForIndexNotification(res.Index);
                        }

                        var detailsPerNode = new DetailsPerNode
                        {
                            MaxUtilizedCores = maxUtilizedCores,
                            NumberOfCores = nodeInfo.NumberOfCores,
                            InstalledMemoryInGb = nodeInfo.InstalledMemoryInGb,
                            UsableMemoryInGb = nodeInfo.UsableMemoryInGb,
                            BuildInfo = nodeInfo.BuildInfo,
                            OsInfo = nodeInfo.OsInfo
                        };

                        try
                        {
                            await ServerStore.PutNodeLicenseLimitsAsync(nodeTag, detailsPerNode, ServerStore.LicenseManager.LicenseStatus, $"{raftRequestId}/put-license-limits");
                        }
                        catch
                        {
                            // we'll retry this again later
                        }
                    }

                    NoContentStatus();
                    return;
                }
            }
            RedirectToLeader();
        }

        private static void AssertCanAddNodeWithTopologyId(ClusterTopology clusterTopology, Client.ServerWide.Commands.NodeInfo nodeInfo, string nodeUrl)
        {
            if (clusterTopology.TopologyId != nodeInfo.TopologyId)
            {
                throw new TopologyMismatchException(
                    $"Adding a new node to cluster failed. The new node is already in another cluster. " +
                    $"Expected topology id: {clusterTopology.TopologyId}, but we get {nodeInfo.TopologyId}");
            }

            if (nodeInfo.NodeTag != RachisConsensus.InitialTag && clusterTopology.Contains(nodeInfo.NodeTag) == false)
            {
                // this is fine, since we probably adding back a node that we just removed
                return;
            }

            if (nodeInfo.CurrentState != RachisState.Passive)
            {
                throw new InvalidOperationException($"Can't add a new node on {nodeUrl} to cluster " +
                                                    $"because it's already in the cluster under tag :{nodeInfo.NodeTag} " +
                                                    $"and URL: {clusterTopology.GetUrlFromTag(nodeInfo.NodeTag)}");
            }
        }

        [RavenAction("/admin/cluster/node", "DELETE", AuthorizationStatus.ClusterAdmin, CorsMode = CorsMode.Cluster)]
        public async Task DeleteNode()
        {
            var nodeTag = GetStringQueryString("nodeTag");
            await ServerStore.EnsureNotPassiveAsync();

            if (ServerStore.IsLeader())
            {
                if (nodeTag == ServerStore.Engine.Tag)
                {
                    using (var token = CreateHttpRequestBoundOperationToken())
                    {
                        // cannot remove the leader, let's change the leader
                        ServerStore.Engine.CurrentLeader?.StepDown();
                        await ServerStore.Engine.WaitForState(RachisState.Follower, token.Token);
                        RedirectToLeader();
                        return;
                    }
                }

                await ServerStore.RemoveFromClusterAsync(nodeTag);

                if (RavenLogManager.Instance.IsAuditEnabled)
                    LogAuditFor("Server", "DELETE", $"Node {nodeTag} from cluster. Term: {ServerStore.Engine.CurrentCommittedState.Term}.");

                NoContentStatus();
                return;
            }

            RedirectToLeader();
        }

        [RavenAction("/admin/license/set-limit", "POST", AuthorizationStatus.ClusterAdmin, CorsMode = CorsMode.Cluster)]
        public async Task SetLicenseLimit()
        {
            var nodeTag = GetStringQueryString("nodeTag");
            var maxUtilizedCores = GetIntValueQueryString("maxUtilizedCores", required: false);
            if (maxUtilizedCores != null && maxUtilizedCores <= 0)
                throw new ArgumentException("Max utilized cores must be greater than 0");

            await ServerStore.LicenseManager.ChangeLicenseLimits(nodeTag, maxUtilizedCores, GetRaftRequestIdFromQuery());
            NoContentStatus();
        }

        [RavenAction("/admin/cluster/timeout", "POST", AuthorizationStatus.Operator, CorsMode = CorsMode.Cluster)]
        public Task TimeoutNow()
        {
            Server.ServerStore.Engine.Timeout.ExecuteTimeoutBehavior();
            NoContentStatus();
            return Task.CompletedTask;
        }

        [RavenAction("/admin/cluster/reelect", "POST", AuthorizationStatus.Operator, CorsMode = CorsMode.Cluster)]
        public Task EnforceReelection()
        {
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

        [RavenAction("/admin/cluster/promote", "POST", AuthorizationStatus.ClusterAdmin, CorsMode = CorsMode.Cluster)]
        public async Task PromoteNode()
        {
            var nodeTag = GetStringQueryString("nodeTag");

            if (ServerStore.LeaderTag == null)
            {
                throw new NoLeaderException(
                    $"Failed to promote node {nodeTag} because there is no leader in the cluster topology");
            }

            if (ServerStore.IsLeader() == false)
            {
                RedirectToLeader();
                return;
            }

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var topology = ServerStore.GetClusterTopology(context);
                if (topology.Watchers.ContainsKey(nodeTag) == false)
                {
                    throw new InvalidOperationException(
                        $"Failed to promote node {nodeTag} because {nodeTag} is not a watcher in the cluster topology");
                }

                var url = topology.GetUrlFromTag(nodeTag);
                await ServerStore.Engine.ModifyTopologyAsync(nodeTag, url, Leader.TopologyModification.Promotable);
                NoContentStatus();
            }
        }

        /* Demote a voter (member/promotable) node to a non-voter  */

        [RavenAction("/admin/cluster/demote", "POST", AuthorizationStatus.ClusterAdmin, CorsMode = CorsMode.Cluster)]
        public async Task DemoteNode()
        {
            var nodeTag = GetStringQueryString("nodeTag");

            if (ServerStore.LeaderTag == null)
            {
                throw new NoLeaderException(
                    $"Failed to demote node {nodeTag} because there is no leader in the cluster topology");
            }

            if (ServerStore.IsLeader() == false)
            {
                RedirectToLeader();
                return;
            }

            if (nodeTag == ServerStore.LeaderTag)
            {
                throw new InvalidOperationException(
                    $"Failed to demote node {nodeTag} because {nodeTag} is the current leader in the cluster topology. In order to demote {nodeTag} perform a Step-Down first");
            }

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var topology = ServerStore.GetClusterTopology(context);
                if (topology.Promotables.ContainsKey(nodeTag) == false && topology.Members.ContainsKey(nodeTag) == false)
                {
                    throw new InvalidOperationException(
                        $"Failed to demote node {nodeTag} because {nodeTag} is not a voter in the cluster topology");
                }

                var url = topology.GetUrlFromTag(nodeTag);
                await ServerStore.Engine.ModifyTopologyAsync(nodeTag, url, Leader.TopologyModification.NonVoter);
                NoContentStatus();
            }
        }

        [RavenAction("/admin/cluster/remove-entry-from-log", "POST", AuthorizationStatus.ClusterAdmin, CorsMode = CorsMode.Cluster)]
        public async Task RemoveEntryFromLog()
        {
            var index = GetLongQueryString("index");
            var first = GetBoolValueQueryString("first", false) ?? true;
            var nodeList = new List<string>();
            var removed = await ServerStore.Engine.RemoveEntryFromRaftLogAsync(index);
            if (removed)
                nodeList.Add(ServerStore.NodeTag);
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                if (first)
                {
                    Dictionary<string, string> allNodes = null;
                    using (context.OpenReadTransaction())
                    {
                        allNodes = ServerStore.GetClusterTopology(context).AllNodes;
                    }
                    foreach (var node in allNodes)
                    {
                        if (node.Value == Server.WebUrl)
                        {
                            continue;
                        }

                        var cmd = new RemoveEntryFromRaftLogCommand(index);
                        using (var requestExecutor = ClusterRequestExecutor.CreateForShortTermUse(node.Value, Server.Certificate.Certificate, DocumentConventions.DefaultForServer))
                        {
                            await requestExecutor.ExecuteAsync(cmd, context);
                            nodeList.AddRange(cmd.Result);
                        }
                    }
                }

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WriteArray("Nodes", nodeList);
                    writer.WriteEndObject();
                }
            }
        }

        private sealed class RemoveEntryFromRaftLogCommand : RavenCommand<List<string>>
        {
            private readonly long _index;

            public RemoveEntryFromRaftLogCommand(long index)
            {
                _index = index;
            }

            public override bool IsReadRequest { get; }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/cluster/remove-entry-from-log?index={_index}&first=false";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                Result = new List<string>();

                response.TryGet("Nodes", out BlittableJsonReaderArray array);

                foreach (var item in array)
                    Result.Add(item.ToString());
            }
        }
    }
}
