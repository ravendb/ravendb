using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using CsvHelper;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Commercial;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Extensions;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Migration;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Web.System
{
    public sealed class DatabasesHandler : ServerRequestHandler
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

                    var items = ServerStore.Cluster.ItemsStartingWith(context, Constants.Documents.Prefix, GetStart(), GetPageSize());

                    var allowedDbs = await GetAllowedDbsAsync(null, requireAdmin: false, requireWrite: false);

                    if (allowedDbs.HasAccess == false)
                        return;

                    if (allowedDbs.AuthorizedDatabases != null)
                    {
                        items = items.Where(item => allowedDbs.AuthorizedDatabases.ContainsKey(item.Item1.Substring(Constants.Documents.Prefix.Length)));
                    }

                    writer.WriteArray(context, nameof(DatabasesInfo.Databases), items, (w, c, dbDoc) =>
                    {
                        var databaseName = dbDoc.Item1.Substring(Constants.Documents.Prefix.Length);
                        if (namesOnly)
                        {
                            w.WriteString(databaseName);
                            return;
                        }

                        WriteDatabaseInfo(databaseName, dbDoc.Value, context, w);
                    });
                    writer.WriteEndObject();
                }
            }
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

                    if (rawRecord.Topology.Members.Count == 0 && rawRecord.Topology.Rehabs.Count == 0 && rawRecord.DeletionInProgress.Any())
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
                    var license = ServerStore.LoadLicenseLimits();
                    var dbNodes = GetNodes(rawRecord.Topology.Members, ServerNode.Role.Member, license).Concat(GetNodes(rawRecord.Topology.Rehabs, ServerNode.Role.Rehab, license));
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(writer,
                            new DynamicJsonValue
                            {
                                [nameof(Topology.Nodes)] = new DynamicJsonArray(dbNodes),
                                [nameof(Topology.Etag)] = rawRecord.Topology.Stamp?.Index ?? -1
                            });
                    }



                    IEnumerable<DynamicJsonValue> GetNodes(List<string> nodes, ServerNode.Role serverRole, LicenseLimits license)
                    {
                        foreach (var node in nodes)
                        {
                            var url = GetUrl(node, clusterTopology);
                            if (url == null)
                                continue;

                            if (license == null || license.NodeLicenseDetails.TryGetValue(node, out DetailsPerNode nodeDetails)==false)
                            {
                                nodeDetails = null;
                            }

                            yield return TopologyNodeToJson(node, url, name, serverRole, nodeDetails);
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
                    if (ServerStore.DatabaseInfoCache.TryGet(databaseName, databaseInfoJson => { 
                            
                        var periodicBackups = new List<PeriodicBackup>();

                        foreach (var periodicBackupConfiguration in dbRecord.PeriodicBackups)
                        {
                            periodicBackups.Add(new PeriodicBackup
                            {
                                Configuration = periodicBackupConfiguration,
                                BackupStatus = BackupUtils.GetBackupStatusFromCluster(ServerStore, context, databaseName, periodicBackupConfiguration.TaskId)
                            });
                        }

                        databaseInfoJson.Modifications = new DynamicJsonValue(databaseInfoJson)
                        {
                            [nameof(DatabaseInfo.Disabled)] = disabled,
                            [nameof(DatabaseInfo.LockMode)] = lockMode,
                            [nameof(DatabaseInfo.IndexingStatus)] = indexingStatus,
                            [nameof(DatabaseInfo.NodesTopology)] = nodesTopology.ToJson(),
                            [nameof(DatabaseInfo.DeletionInProgress)] = DynamicJsonValue.Convert(dbRecord.DeletionInProgress),
                            [nameof(DatabaseInfo.Environment)] = studioEnvironment,
                            [nameof(DatabaseInfo.BackupInfo)] = BackupUtils.GetBackupInfo(
                                new BackupUtils.BackupInfoParameters()
                                {
                                    ServerStore = ServerStore,
                                    PeriodicBackups = periodicBackups,
                                    DatabaseName = databaseName,
                                    Context = context
                                })
                        };

                        if (HttpContext.Request.IsFromStudio())
                        {
                            // remove this as this isn't needed for the studio
                            // cannot remove this entirely since this is used by the client API
                            databaseInfoJson.Modifications.Remove(nameof(DatabaseInfo.MountPointsUsage));
                        }

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
            var results = periodicBackupRunner?.GetBackupInfo(context);
            return results;
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
