using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Exceptions.Database;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Commercial;
using Raven.Server.Extensions;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Migration;
using Raven.Server.Utils;
using Raven.Server.Web.System.Processors.Databases;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Web.System
{
    public sealed class DatabasesHandler : ServerRequestHandler
    {
        [RavenAction("/databases", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Databases()
        {
            using (var processor = new DatabasesHandlerProcessorForGet(this))
                await processor.ExecuteAsync().ConfigureAwait(false);
        }

        [RavenAction("/admin/databases/topology/modify", "POST", AuthorizationStatus.Operator)]
        public async Task ModifyTopology()
        {
            var dbName = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name").Trim();
            var raftRequestId = GetRaftRequestIdFromQuery();

            await ServerStore.EnsureNotPassiveAsync();
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var json = await context.ReadForDiskAsync(RequestBodyStream(), "Database Topology");
                var databaseTopology = JsonDeserializationCluster.DatabaseTopology(json);

                // Validate Database Name
                DatabaseRecord databaseRecord;
                ClusterTopology clusterTopology;
                using (context.OpenReadTransaction())
                {
                    databaseRecord = ServerStore.Cluster.ReadDatabase(context, dbName, out var index);
                    if (databaseRecord == null)
                    {
                        DatabaseDoesNotExistException.ThrowWithMessage(dbName, $"Database Record was not found when trying to modify database topology");
                        return;
                    }
                    clusterTopology = ServerStore.GetClusterTopology(context);
                }

                if (LoggingSource.AuditLog.IsInfoEnabled)
                {
                    LogAuditFor("DbMgmt", "CHANGE", $"Database '{dbName}' topology. " +
                                                    $"Old topology: {databaseRecord.Topology} " +
                                                    $"New topology: {databaseTopology}.");
                }

                // Validate Topology
                var databaseAllNodes = databaseTopology.AllNodes;
                foreach (var node in databaseAllNodes)
                {
                    if (clusterTopology.Contains(node) == false)
                        throw new ArgumentException($"Failed to modify database {dbName} topology, because we don't have node {node} (which is in the new topology) in the cluster.");

                    if (databaseRecord.Topology.RelevantFor(node) == false)
                    {
                        ValidateNodeForAddingToDb(dbName, node, databaseRecord, clusterTopology, Server, baseMessage: $"Can't modify database {dbName} topology");
                    }
                }
                databaseTopology.ReplicationFactor = Math.Min(databaseTopology.Count, clusterTopology.AllNodes.Count);

                // Update Topology
                var update = new UpdateTopologyCommand(dbName, SystemTime.UtcNow, raftRequestId)
                {
                    Topology = databaseTopology
                };

                var (newIndex, _) = await ServerStore.SendToLeaderAsync(update);

                // Return Raft Index
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(ModifyDatabaseTopologyResult.RaftCommandIndex)] = newIndex
                    });
                }
            }
        }

        [RavenAction("/topology", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, CheckForChanges = false)]
        public async Task GetTopology()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            var applicationIdentifier = GetStringQueryString("applicationIdentifier", required: false);
            var usePrivate = GetBoolValueQueryString("private", required: false) ?? false;
            var includePromotables = GetBoolValueQueryString("includePromotables", required: false) ?? false;

            if (applicationIdentifier != null)
            {
                AlertIfDocumentStoreCreationRateIsNotReasonable(applicationIdentifier, name);
            }

            using (ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            {
                using (context.OpenReadTransaction())
                using (var rawRecord = ServerStore.Cluster.ReadRawDatabaseRecord(context, name))
                {
                    if (await CanAccessDatabaseAsync(name, requireAdmin: false, requireWrite: false) == false)
                        return;

                    if (rawRecord == null)
                    {
                        // here we return 503 so clients will try to failover to another server
                        // if this is a newly created db that we haven't been notified about it yet
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.ServiceUnavailable;
                        HttpContext.Response.Headers[Constants.Headers.DatabaseMissing] = name;
                        await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                        {
                            context.Write(writer,
                                new DynamicJsonValue { ["Type"] = "Error", ["Message"] = "Database " + name + " wasn't found" });
                        }

                        return;
                    }

                    var clusterTopology = ServerStore.GetClusterTopology(context);
                    if (ServerStore.IsPassive() && clusterTopology.TopologyId != null)
                    {
                        // we were kicked-out from the cluster
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.ServiceUnavailable;
                        return;
                    }

                    if (rawRecord.DeletionInProgress.Count > 0 &&
                        rawRecord.Topologies.Sum(t => t.Topology.Count) == 0)
                    {
                        // The database at deletion progress from all nodes
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.ServiceUnavailable;
                        HttpContext.Response.Headers[Constants.Headers.DatabaseMissing] = name;
                        await using (var writer = new AsyncBlittableJsonTextWriter(context, HttpContext.Response.Body))
                        {
                            context.Write(writer, new DynamicJsonValue { ["Type"] = "Error", ["Message"] = "Database " + name + " was deleted" });
                        }

                        return;
                    }

                    clusterTopology.ReplaceCurrentNodeUrlWithClientRequestedNodeUrlIfNecessary(ServerStore, HttpContext);
                    var license = ServerStore.LoadLicenseLimits();

                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        long stampIndex;
                        DatabaseTopology topology;
                        IEnumerable<DynamicJsonValue> dbNodes;
                        IEnumerable<DynamicJsonValue> promotables = null;
                        if (rawRecord.IsSharded)
                        {
                            topology = rawRecord.Sharding.Orchestrator.Topology;
                            stampIndex = rawRecord.Sharding.Shards.Max(x => x.Value.Stamp?.Index ?? -1);
                        }
                        else
                        {
                            topology = rawRecord.Topology;
                            stampIndex = topology.Stamp?.Index ?? -1;
                        }

                        dbNodes = GetNodes(topology.Members, ServerNode.Role.Member).Concat(GetNodes(topology.Rehabs, ServerNode.Role.Rehab));

                        if (includePromotables)
                            promotables = GetNodes(topology.Promotables, ServerNode.Role.Promotable);

                        context.Write(writer,
                            new DynamicJsonValue
                            {
                                [nameof(Topology.Promotables)] = new DynamicJsonArray(promotables ?? new List<DynamicJsonValue>()),
                                [nameof(Topology.Nodes)] = new DynamicJsonArray(dbNodes),
                                [nameof(Topology.Etag)] = stampIndex
                            });


                        IEnumerable<DynamicJsonValue> GetNodes(List<string> nodes, ServerNode.Role serverRole)
                        {
                            foreach (var node in nodes)
                            {
                                var url = GetUrl(node, clusterTopology, usePrivate);
                                if (url == null)
                                    continue;

                                if (license == null || license.NodeLicenseDetails.TryGetValue(node, out DetailsPerNode nodeDetails) == false)
                                {
                                    nodeDetails = null;
                                }

                                yield return TopologyNodeToJson(node, url, name, serverRole, nodeDetails);
                            }
                        }
                    }
                }
            }
        }

        private DynamicJsonValue TopologyNodeToJson(string tag, string url, string name, ServerNode.Role role, DetailsPerNode details)
        {
            var json = new DynamicJsonValue
            {
                [nameof(ServerNode.Url)] = url,
                [nameof(ServerNode.ClusterTag)] = tag,
                [nameof(ServerNode.ServerRole)] = role,
                [nameof(ServerNode.Database)] = name
            };

            if(details != null)
            {
                json[nameof(ServerNode.ServerVersion)] =
                    details.BuildInfo.AssemblyVersion ?? details.BuildInfo.ProductVersion;
            }

            return json;
        }

        private void AlertIfDocumentStoreCreationRateIsNotReasonable(string applicationIdentifier, string name)
        {
            var q = ServerStore.ClientCreationRate.GetOrCreate(applicationIdentifier);
            var now = DateTime.UtcNow;
            q.Enqueue(now);
            while (q.Count > 20)
            {
                if (q.TryDequeue(out var last) && (now - last).TotalMinutes < 1)
                {
                    q.Clear();

                    ServerStore.NotificationCenter.Add(
                        AlertRaised.Create(
                            name,
                            "Too many clients creations",
                            "There has been a lot of topology updates (more than 20) for the same client id in less than a minute. " +
                            $"Last one from ({HttpContext.Connection.RemoteIpAddress} as ({GetCertificateInfo()}) " +
                            "This is usually an indication that you are creating a large number of DocumentStore instance. " +
                            "Are you creating a Document Store per request, instead of using DocumentStore as a singleton? ",
                            AlertType.HighClientCreationRate,
                            NotificationSeverity.Warning
                        ));

                    string GetCertificateInfo()
                    {
                        if (HttpContext.Connection.ClientCertificate == null)
                            return "<unsecured>";

                        return string.IsNullOrEmpty(HttpContext.Connection.ClientCertificate.FriendlyName) == false
                            ? HttpContext.Connection.ClientCertificate.FriendlyName
                            : HttpContext.Connection.ClientCertificate.Thumbprint;
                    }
                }
            }
        }

        // we can't use '/database/is-loaded` because that conflict with the `/databases/<db-name>`
        // route prefix
        [RavenAction("/debug/is-loaded", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task IsDatabaseLoaded()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

            if (await CanAccessDatabaseAsync(name, requireAdmin: false, requireWrite: false) == false)
                return;

            var isLoaded = ServerStore.DatabasesLandlord.IsDatabaseLoaded(name);
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(IsDatabaseLoadedCommand.CommandResult.DatabaseName)] = name,
                        [nameof(IsDatabaseLoadedCommand.CommandResult.IsLoaded)] = isLoaded
                    });
                }
            }
        }

        [RavenAction("/admin/remote-server/build/version", "GET", AuthorizationStatus.Operator)]
        public async Task GetRemoteServerBuildInfoWithDatabases()
        {
            var serverUrl = GetQueryStringValueAndAssertIfSingleAndNotEmpty("serverUrl");
            var userName = GetStringQueryString("userName", required: false);
            var password = GetStringQueryString("password", required: false);
            var domain = GetStringQueryString("domain", required: false);
            var apiKey = GetStringQueryString("apiKey", required: false);
            var enableBasicAuthenticationOverUnsecuredHttp = GetBoolValueQueryString("enableBasicAuthenticationOverUnsecuredHttp", required: false);
            var skipServerCertificateValidation = GetBoolValueQueryString("skipServerCertificateValidation", required: false);
            var migrator = new Migrator(new SingleDatabaseMigrationConfiguration
            {
                ServerUrl = serverUrl,
                UserName = userName,
                Password = password,
                Domain = domain,
                ApiKey = apiKey,
                EnableBasicAuthenticationOverUnsecuredHttp = enableBasicAuthenticationOverUnsecuredHttp ?? false,
                SkipServerCertificateValidation = skipServerCertificateValidation ?? false
            }, ServerStore);

            var buildInfo = await migrator.GetBuildInfo();
            var authorized = new Reference<bool>();
            var isLegacyOAuthToken = new Reference<bool>();
            var databaseNames = await migrator.GetDatabaseNames(buildInfo.MajorVersion, authorized, isLegacyOAuthToken);
            var fileSystemNames = await migrator.GetFileSystemNames(buildInfo.MajorVersion);

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var json = new DynamicJsonValue
                {
                    [nameof(BuildInfoWithResourceNames.BuildVersion)] = buildInfo.BuildVersion,
                    [nameof(BuildInfoWithResourceNames.ProductVersion)] = buildInfo.ProductVersion,
                    [nameof(BuildInfoWithResourceNames.MajorVersion)] = buildInfo.MajorVersion,
                    [nameof(BuildInfoWithResourceNames.FullVersion)] = buildInfo.FullVersion,
                    [nameof(BuildInfoWithResourceNames.DatabaseNames)] = TypeConverter.ToBlittableSupportedType(databaseNames),
                    [nameof(BuildInfoWithResourceNames.FileSystemNames)] = TypeConverter.ToBlittableSupportedType(fileSystemNames),
                    [nameof(BuildInfoWithResourceNames.Authorized)] = authorized.Value,
                    [nameof(BuildInfoWithResourceNames.IsLegacyOAuthToken)] = isLegacyOAuthToken.Value
                };

                context.Write(writer, json);
            }
        }

        private string GetUrl(string tag, ClusterTopology clusterTopology, bool usePrivate)
        {
            string url = null;
            if (usePrivate)
            {
                url = ServerStore.PublishedServerUrls?.SelectUrl(tag, clusterTopology);
                if (url != null)
                    return url;
            }
            
            if (Server.ServerStore.NodeTag == tag)
                url = ServerStore.GetNodeHttpServerUrl(HttpContext.Request.GetClientRequestedNodeUrl());

            if (url == null)
                url = clusterTopology.GetUrlFromTag(tag);

            return url;
        }
    }
}
