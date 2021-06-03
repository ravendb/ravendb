using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Extensions;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Migration;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Web.System
{
    public sealed class DatabasesHandler : RequestHandler
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<DatabasesHandler>("Server");

        [RavenAction("/databases", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Databases()
        {
            // if Studio requested information about single resource - handle it
            var dbName = GetStringQueryString("name", false);
            if (dbName != null)
            {
                await DbInfo(dbName);
                return;
            }

            var namesOnly = GetBoolValueQueryString("namesOnly", required: false) ?? false;

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                context.OpenReadTransaction();
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    var items = ServerStore.Cluster.GetAllRawDatabases(context,  GetStart(), GetPageSize());

                    var allowedDbs = await GetAllowedDbsAsync(null, requireAdmin: false, requireWrite: false);

                    if (allowedDbs.HasAccess == false)
                        return;

                    if (allowedDbs.AuthorizedDatabases != null)
                    {
                        items = items.Where(item => allowedDbs.AuthorizedDatabases.ContainsKey(item.DatabaseName));
                    }

                    writer.WriteArray(context, nameof(DatabasesInfo.Databases), items.SelectMany(x => x.AsShardsOrNormal()), (w, c, dbDoc) =>
                    {
                        var databaseName = dbDoc.DatabaseName;
                        if (namesOnly)
                        {
                            w.WriteString(databaseName);
                            return;
                        }

                        WriteDatabaseInfo(databaseName, dbDoc.Raw, context, w);
                    });
                    writer.WriteEndObject();
                }
            }
        }

        [RavenAction("/topology", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetTopology()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            var applicationIdentifier = GetStringQueryString("applicationIdentifier", required: false);

            if (applicationIdentifier != null)
            {
                AlertIfDocumentStoreCreationRateIsNotReasonable(applicationIdentifier, name);
            }

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
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
                        HttpContext.Response.Headers["Database-Missing"] = name;
                        await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                        {
                            context.Write(writer,
                                new DynamicJsonValue
                                {
                                    ["Type"] = "Error",
                                    ["Message"] = "Database " + name + " wasn't found"
                                });
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
                        rawRecord.Topologies.Sum(t => t.Topology.Count) == rawRecord.DeletionInProgress.Count)
                    {
                        // The database at deletion progress from all nodes
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.ServiceUnavailable;
                        HttpContext.Response.Headers["Database-Missing"] = name;
                        await using (var writer = new AsyncBlittableJsonTextWriter(context, HttpContext.Response.Body))
                        {
                            context.Write(writer, new DynamicJsonValue
                            {
                                ["Type"] = "Error", 
                                ["Message"] = "Database " + name + " was deleted"
                            });
                        }

                        return;
                    }


                    clusterTopology.ReplaceCurrentNodeUrlWithClientRequestedNodeUrlIfNecessary(ServerStore, HttpContext);

                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        long stampIndex;
                        IEnumerable<DynamicJsonValue> dbNodes;
                        if (rawRecord.Shards?.Length > 0)
                        {
                            dbNodes = clusterTopology.Members.Keys.Select(x => 
                                TopologyNodeToJson(x, clusterTopology, name, ServerNode.Role.Member));
                            stampIndex = rawRecord.Shards.Max(x => x.Stamp?.Index ?? -1);
                        }
                        else
                        {
                            dbNodes = rawRecord.Topology.Members.Select(x => 
                                    TopologyNodeToJson(x, clusterTopology, name, ServerNode.Role.Member))
                                .Concat(rawRecord.Topology.Rehabs.Select(x =>
                                    TopologyNodeToJson(x, clusterTopology, name, ServerNode.Role.Rehab))
                                );
                            stampIndex = rawRecord.Topology.Stamp?.Index ?? -1;
                        }

                        context.Write(writer, new DynamicJsonValue
                        {
                            [nameof(Topology.Nodes)] = new DynamicJsonArray(dbNodes),
                            [nameof(Topology.Etag)] = stampIndex
                        });
                    }
                }
            }
        }
        
        private DynamicJsonValue TopologyNodeToJson(string tag, ClusterTopology clusterTopology, string name, ServerNode.Role role)
        {
            return new DynamicJsonValue
            {
                [nameof(ServerNode.Url)] = GetUrl(tag, clusterTopology),
                [nameof(ServerNode.ClusterTag)] = tag,
                [nameof(ServerNode.ServerRole)] = role,
                [nameof(ServerNode.Database)] = name
            };
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
                            $"Last one from ({HttpContext.Connection.RemoteIpAddress} as " +
                            $"{HttpContext.Connection.ClientCertificate?.FriendlyName ?? HttpContext.Connection.ClientCertificate?.Thumbprint ?? "<unsecured>"})" +
                            "This is usually an indication that you are creating a large number of DocumentStore instance. " +
                            "Are you creating a Document Store per request, instead of using DocumentStore as a singleton? ",
                            AlertType.HighClientCreationRate,
                            NotificationSeverity.Warning
                        ));
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

        private string GetUrl(string tag, ClusterTopology clusterTopology)
        {
            string url = null;

            if (Server.ServerStore.NodeTag == tag)
                url = ServerStore.GetNodeHttpServerUrl(HttpContext.Request.GetClientRequestedNodeUrl());

            if (url == null)
                url = clusterTopology.GetUrlFromTag(tag);

            return url;
        }

        private async Task DbInfo(string dbName)
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                context.OpenReadTransaction();
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var dbId = Constants.Documents.Prefix + dbName;
                    using (var dbRecord = ServerStore.Cluster.Read(context, dbId, out long _))
                    {
                        if (dbRecord == null)
                        {
                            HttpContext.Response.Headers.Remove("Content-Type");
                            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                            HttpContext.Response.Headers["Database-Missing"] = dbName;
                            return;
                        }

                        WriteDatabaseInfo(dbName, dbRecord, context, writer);
                    }
                }
            }
        }

        private void WriteDatabaseInfo(string databaseName, BlittableJsonReaderObject dbRecordBlittable,
            TransactionOperationContext context, AbstractBlittableJsonTextWriter writer)
        {
            var nodesTopology = new NodesTopology();

            try
            {
                var online = ServerStore.DatabasesLandlord.DatabasesCache.TryGetValue(databaseName, out Task<DocumentDatabase> dbTask) &&
                             dbTask != null &&
                             dbTask.IsCompleted;

                var dbRecord = JsonDeserializationCluster.DatabaseRecord(dbRecordBlittable);
                var topology = dbRecord.Topology;

                var statuses = ServerStore.GetNodesStatuses();
                if (topology != null)
                {
                    nodesTopology.PriorityOrder = topology.PriorityOrder;

                    var clusterTopology = ServerStore.GetClusterTopology(context);
                    clusterTopology.ReplaceCurrentNodeUrlWithClientRequestedNodeUrlIfNecessary(ServerStore, HttpContext);

                    foreach (var member in topology.Members)
                    {
                        if (dbRecord.DeletionInProgress != null && dbRecord.DeletionInProgress.ContainsKey(member))
                            continue;

                        var url = clusterTopology.GetUrlFromTag(member);
                        var node = new InternalReplication
                        {
                            Database = databaseName,
                            NodeTag = member,
                            Url = url
                        };
                        nodesTopology.Members.Add(GetNodeId(node));
                        SetNodeStatus(topology, member, nodesTopology, statuses);
                    }

                    foreach (var promotable in topology.Promotables)
                    {
                        if (dbRecord.DeletionInProgress != null && dbRecord.DeletionInProgress.ContainsKey(promotable))
                            continue;

                        topology.PredefinedMentors.TryGetValue(promotable, out var mentorCandidate);
                        var node = GetNode(databaseName, clusterTopology, promotable, mentorCandidate, out var promotableTask);
                        var mentor = topology.WhoseTaskIsIt(ServerStore.Engine.CurrentState, promotableTask, null);
                        nodesTopology.Promotables.Add(GetNodeId(node, mentor));
                        SetNodeStatus(topology, promotable, nodesTopology, statuses);
                    }

                    foreach (var rehab in topology.Rehabs)
                    {
                        if (dbRecord.DeletionInProgress != null && dbRecord.DeletionInProgress.ContainsKey(rehab))
                            continue;

                        var node = GetNode(databaseName, clusterTopology, rehab, null, out var promotableTask);
                        var mentor = topology.WhoseTaskIsIt(ServerStore.Engine.CurrentState, promotableTask, null);
                        nodesTopology.Rehabs.Add(GetNodeId(node, mentor));
                        SetNodeStatus(topology, rehab, nodesTopology, statuses);
                    }
                }

                // Check for exceptions
                if (dbTask != null && dbTask.IsFaulted)
                {
                    var exception = dbTask.Exception.ExtractSingleInnerException();
                    WriteFaultedDatabaseInfo(databaseName, nodesTopology, exception, context, writer);
                    return;
                }

                var db = online ? dbTask.Result : null;

                var indexingStatus = db?.IndexStore?.Status;
                if (indexingStatus == null)
                {
                    // Looking for disabled indexing flag inside the database settings for offline database status
                    if (dbRecord.Settings.TryGetValue(RavenConfiguration.GetKey(x => x.Indexing.Disabled), out var val) &&
                        bool.TryParse(val, out var indexingDisabled) && indexingDisabled)
                        indexingStatus = IndexRunningStatus.Disabled;
                }

                var disabled = dbRecord.Disabled;
                var lockMode = dbRecord.LockMode;

                var studioEnvironment = StudioConfiguration.StudioEnvironment.None;
                if (dbRecord.Studio != null && !dbRecord.Studio.Disabled)
                {
                    studioEnvironment = dbRecord.Studio.Environment;
                }

                if (online == false)
                {
                    // if state of database is found in the cache we can continue
                    if (ServerStore.DatabaseInfoCache.TryGet(databaseName, databaseInfoJson =>
                    {
                        databaseInfoJson.Modifications = new DynamicJsonValue(databaseInfoJson)
                        {
                            [nameof(DatabaseInfo.Disabled)] = disabled,
                            [nameof(DatabaseInfo.LockMode)] = lockMode,
                            [nameof(DatabaseInfo.IndexingStatus)] = indexingStatus,
                            [nameof(DatabaseInfo.NodesTopology)] = nodesTopology.ToJson(),
                            [nameof(DatabaseInfo.DeletionInProgress)] = DynamicJsonValue.Convert(dbRecord.DeletionInProgress),
                            [nameof(DatabaseInfo.Environment)] = studioEnvironment
                        };

                        context.Write(writer, databaseInfoJson);
                    }))
                    {
                        return;
                    }

                    // we won't find it if it is a new database or after a dirty shutdown,
                    // so just report empty values then
                }

                var size = db?.GetSizeOnDisk() ?? (new Size(0), new Size(0));

                var databaseInfo = new DatabaseInfo
                {
                    Name = databaseName,
                    Disabled = disabled,
                    LockMode = lockMode,
                    TotalSize = size.Data,
                    TempBuffersSize = size.TempBuffers,

                    IsAdmin = true,
                    IsEncrypted = dbRecord.Encrypted,
                    UpTime = online ? (TimeSpan?)GetUptime(db) : null,
                    BackupInfo = GetBackupInfo(db, context),

                    Alerts = db?.NotificationCenter.GetAlertCount() ?? 0,
                    PerformanceHints = db?.NotificationCenter.GetPerformanceHintCount() ?? 0,
                    RejectClients = false,
                    LoadError = null,
                    IndexingErrors = db?.IndexStore?.GetIndexes()?.Sum(index => index.GetErrorCount()) ?? 0,

                    DocumentsCount = db?.DocumentsStorage.GetNumberOfDocuments() ?? 0,
                    HasRevisionsConfiguration = db?.DocumentsStorage.RevisionsStorage.Configuration != null,
                    HasExpirationConfiguration = (db?.ExpiredDocumentsCleaner?.ExpirationConfiguration?.Disabled ?? true) == false,
                    HasRefreshConfiguration = (db?.ExpiredDocumentsCleaner?.RefreshConfiguration?.Disabled ?? true) == false,
                    IndexesCount = db?.IndexStore?.GetIndexes()?.Count() ?? 0,
                    IndexingStatus = indexingStatus ?? IndexRunningStatus.Running,
                    Environment = studioEnvironment,

                    NodesTopology = nodesTopology,
                    ReplicationFactor = topology?.ReplicationFactor ?? -1,
                    DynamicNodesDistribution = topology?.DynamicNodesDistribution ?? false,
                    DeletionInProgress = dbRecord.DeletionInProgress
                };

                var doc = databaseInfo.ToJson();
                context.Write(writer, doc);
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failed to get database info for: {databaseName}", e);

                WriteFaultedDatabaseInfo(databaseName, nodesTopology, e, context, writer);
            }
        }

        private static void SetNodeStatus(
            DatabaseTopology topology,
            string nodeTag,
            NodesTopology nodesTopology,
            Dictionary<string, NodeStatus> nodeStatuses)
        {
            var nodeStatus = new DatabaseGroupNodeStatus
            {
                LastStatus = DatabasePromotionStatus.Ok
            };
            if (topology.PromotablesStatus.TryGetValue(nodeTag, out var status))
            {
                nodeStatus.LastStatus = status;
            }
            if (topology.DemotionReasons.TryGetValue(nodeTag, out var reason))
            {
                nodeStatus.LastError = reason;
            }

            if (nodeStatus.LastStatus == DatabasePromotionStatus.Ok &&
                nodeStatuses.TryGetValue(nodeTag, out var serverNodeStatus) &&
                serverNodeStatus.Connected == false)
            {
                nodeStatus.LastError = serverNodeStatus.ErrorDetails;
                nodeStatus.LastStatus = DatabasePromotionStatus.NotResponding;
            }

            nodesTopology.Status[nodeTag] = nodeStatus;
        }

        private static InternalReplication GetNode(string databaseName, ClusterTopology clusterTopology, string rehab, string mentor, out PromotableTask promotableTask)
        {
            var url = clusterTopology.GetUrlFromTag(rehab);
            var node = new InternalReplication
            {
                Database = databaseName,
                NodeTag = rehab,
                Url = url
            };
            promotableTask = new PromotableTask(rehab, url, databaseName, mentor);
            return node;
        }

        private void WriteFaultedDatabaseInfo(string databaseName,
            NodesTopology nodesTopology,
            Exception exception,
            JsonOperationContext context,
            AbstractBlittableJsonTextWriter writer)
        {
            var doc = new DynamicJsonValue
            {
                [nameof(DatabaseInfo.Name)] = databaseName,
                [nameof(DatabaseInfo.NodesTopology)] = nodesTopology,
                [nameof(DatabaseInfo.LoadError)] = exception.Message
            };

            context.Write(writer, doc);
        }

        private static BackupInfo GetBackupInfo(DocumentDatabase db, TransactionOperationContext context)
        {
            var periodicBackupRunner = db?.PeriodicBackupRunner;
            return periodicBackupRunner?.GetBackupInfo(context);
        }

        private static TimeSpan GetUptime(DocumentDatabase db)
        {
            return SystemTime.UtcNow - db.StartTime;
        }

        private static NodeId GetNodeId(InternalReplication node, string responsible = null)
        {
            var nodeId = new NodeId
            {
                NodeTag = node.NodeTag,
                NodeUrl = node.Url,
                ResponsibleNode = responsible
            };

            return nodeId;
        }
    }
}
