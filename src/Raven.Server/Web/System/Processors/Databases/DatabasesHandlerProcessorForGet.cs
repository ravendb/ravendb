using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Extensions;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.Http;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Web.System.Processors.Databases;

internal class DatabasesHandlerProcessorForGet : AbstractServerHandlerProxyReadProcessor<DatabasesInfo>
{
    private static readonly Logger Logger = LoggingSource.Instance.GetLogger<DatabasesHandler>("Server");

    public DatabasesHandlerProcessorForGet([NotNull] RequestHandler requestHandler)
        : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected bool GetNamesOnly() => RequestHandler.GetBoolValueQueryString("namesOnly", required: false) ?? false;

    protected string GetName() => RequestHandler.GetStringQueryString("name", required: false);

    protected int GetStart() => RequestHandler.GetStart();

    protected int GetPageSize() => RequestHandler.GetPageSize();

    protected override RavenCommand<DatabasesInfo> CreateCommandForNode(string nodeTag)
    {
        var name = GetName();
        if (name != null)
            return new GetDatabasesInfoCommand(name, nodeTag);

        var namesOnly = GetNamesOnly();

        var start = GetStart();
        var pageSize = GetPageSize();

        return new GetDatabasesInfoCommand(namesOnly, start, pageSize, nodeTag);
    }

    protected override async ValueTask HandleCurrentNodeAsync()
    {
        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        {
            IEnumerable<RawDatabaseRecord> items;
            var name = GetName();
            if (name != null)
            {
                var databaseRecord = ServerStore.Cluster.ReadRawDatabaseRecord(context, name, out long _);
                if (databaseRecord == null)
                {
                    HttpContext.Response.Headers.Remove(Constants.Headers.ContentType);
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    HttpContext.Response.Headers["Database-Missing"] = name;
                    return;
                }

                items = new[] { databaseRecord };
            }
            else
            {
                items = ServerStore.Cluster.GetAllRawDatabases(context, GetStart(), GetPageSize());
            }

            var allowedDbs = await RequestHandler.GetAllowedDbsAsync(null, requireAdmin: false, requireWrite: false);

            if (allowedDbs.HasAccess == false)
                return;

            if (allowedDbs.AuthorizedDatabases != null)
            {
                items = items.Where(item => allowedDbs.AuthorizedDatabases.ContainsKey(item.DatabaseName));
            }

            var namesOnly = GetNamesOnly();

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WriteArray(context, nameof(DatabasesInfo.Databases), items.SelectMany(x => x.AsShardsOrNormal()), (w, c, record) =>
                {
                    var databaseName = record.DatabaseName;
                    if (namesOnly)
                    {
                        w.WriteString(databaseName);
                        return;
                    }

                    WriteDatabaseInfo(databaseName, record, context, w);
                });

                writer.WriteEndObject();
            }
        }
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<DatabasesInfo> command, JsonOperationContext context, OperationCancelToken token)
    {
        return RequestHandler.ServerStore.ClusterRequestExecutor.ExecuteAsync(command, context, token: token.Token);
    }

    public static void FillNodesTopology(ref NodesTopology nodesTopology, DatabaseTopology topology, RawDatabaseRecord databaseRecord, TransactionOperationContext context, ServerStore serverStore, HttpContext httpContext)
    {
        if (topology == null)
            return;

        var statuses = serverStore.GetNodesStatuses();

        nodesTopology.PriorityOrder = topology.PriorityOrder;

        var clusterTopology = serverStore.GetClusterTopology(context);
        clusterTopology.ReplaceCurrentNodeUrlWithClientRequestedNodeUrlIfNecessary(serverStore, httpContext);

        foreach (var member in topology.Members)
        {
            if (databaseRecord.DeletionInProgress != null && databaseRecord.DeletionInProgress.ContainsKey(member))
                continue;

            var url = clusterTopology.GetUrlFromTag(member);
            var node = new InternalReplication
            {
                Database = databaseRecord.DatabaseName,
                NodeTag = member,
                Url = url
            };
            nodesTopology.Members.Add(GetNodeId(node));
            SetNodeStatus(topology, member, nodesTopology, statuses);
        }

        foreach (var promotable in topology.Promotables)
        {
            if (databaseRecord.DeletionInProgress != null && databaseRecord.DeletionInProgress.ContainsKey(promotable))
                continue;

            topology.PredefinedMentors.TryGetValue(promotable, out var mentorCandidate);
            var node = GetNode(databaseRecord.DatabaseName, clusterTopology, promotable, mentorCandidate, out var promotableTask);
            var mentor = topology.WhoseTaskIsIt(serverStore.Engine.CurrentState, promotableTask, null);
            nodesTopology.Promotables.Add(GetNodeId(node, mentor));
            SetNodeStatus(topology, promotable, nodesTopology, statuses);
        }

        foreach (var rehab in topology.Rehabs)
        {
            if (databaseRecord.DeletionInProgress != null && databaseRecord.DeletionInProgress.ContainsKey(rehab))
                continue;

            var node = GetNode(databaseRecord.DatabaseName, clusterTopology, rehab, null, out var promotableTask);
            var mentor = topology.WhoseTaskIsIt(serverStore.Engine.CurrentState, promotableTask, null);
            nodesTopology.Rehabs.Add(GetNodeId(node, mentor));
            SetNodeStatus(topology, rehab, nodesTopology, statuses);
        }
    }

    private void WriteDatabaseInfo(string databaseName, RawDatabaseRecord rawDatabaseRecord,
        TransactionOperationContext context, AbstractBlittableJsonTextWriter writer)
    {
        NodesTopology nodesTopology = new();

        try
        {
            var online = ServerStore.DatabasesLandlord.DatabasesCache.TryGetValue(databaseName, out Task<DocumentDatabase> dbTask)
                         && dbTask is { IsCompleted: true };

            var topology = rawDatabaseRecord.IsSharded ? rawDatabaseRecord.Sharding.Orchestrator.Topology : rawDatabaseRecord.Topology;

            FillNodesTopology(ref nodesTopology, topology, rawDatabaseRecord, context, ServerStore, HttpContext);

            // Check for exceptions
            if (dbTask is { IsFaulted: true })
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
                if (rawDatabaseRecord.Settings.TryGetValue(RavenConfiguration.GetKey(x => x.Indexing.Disabled), out var val) &&
                    bool.TryParse(val, out var indexingDisabled) && indexingDisabled)
                    indexingStatus = IndexRunningStatus.Disabled;
            }

            var disabled = rawDatabaseRecord.IsDisabled;
            var lockMode = rawDatabaseRecord.LockMode;

            var studioEnvironment = StudioConfiguration.StudioEnvironment.None;
            if (rawDatabaseRecord.StudioConfiguration is { Disabled: false })
            {
                studioEnvironment = rawDatabaseRecord.StudioConfiguration.Environment;
            }

            if (online == false)
            {
                // if state of database is found in the cache we can continue
                if (ServerStore.DatabaseInfoCache.TryGet(databaseName, databaseInfoJson =>
                {

                    var periodicBackups = new List<PeriodicBackup>();

                    foreach (var periodicBackupConfiguration in rawDatabaseRecord.PeriodicBackupsConfiguration)
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
                        [nameof(DatabaseInfo.DeletionInProgress)] = DynamicJsonValue.Convert(rawDatabaseRecord.DeletionInProgress),
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
                IsEncrypted = rawDatabaseRecord.IsEncrypted,
                UpTime = online ? GetUptime(db) : null,
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
                DeletionInProgress = rawDatabaseRecord.DeletionInProgress
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

    private static void WriteFaultedDatabaseInfo(string databaseName,
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

    internal class GetDatabasesInfoCommand : RavenCommand<DatabasesInfo>
    {
        private readonly bool _namesOnly;
        private readonly int? _start;
        private readonly int? _pageSize;
        private readonly string _name;

        public GetDatabasesInfoCommand([NotNull] string name, string nodeTag)
        {
            _name = name ?? throw new ArgumentNullException(nameof(name));
            SelectedNodeTag = nodeTag;
        }

        public GetDatabasesInfoCommand(bool namesOnly, int start, int pageSize, string nodeTag)
        {
            _namesOnly = namesOnly;
            _start = start;
            _pageSize = pageSize;
            SelectedNodeTag = nodeTag;
        }

        public override bool IsReadRequest => false;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases?";

            if (_name != null)
                url += $"name={Uri.EscapeDataString(_name)}";

            if (_namesOnly)
                url += "&namesOnly=true";

            if (_start > 0)
                url += $"&start={_start}";

            if (_pageSize != int.MaxValue)
                url += $"&pageSize={_pageSize}";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                return;

            Result = JsonDeserializationServer.DatabasesInfo(response);
        }
    }

}
