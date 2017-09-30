using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using NCrontab.Advanced;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.PeriodicBackup;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Extensions;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron.Exceptions;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Web.System
{
    public sealed class DatabasesHandler : RequestHandler
    {
        [RavenAction("/databases", "GET", AuthorizationStatus.ValidUser)]
        public Task Databases()
        {
            // if Studio requested information about single resource - handle it
            var dbName = GetStringQueryString("name", false);
            if (dbName != null)
                return DbInfo(dbName);

            var namesOnly = GetBoolValueQueryString("namesOnly", required: false) ?? false;


            //TODO: fill all required information (see: RavenDB-5438) - return Raven.Client.Data.DatabasesInfo
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                context.OpenReadTransaction();
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    var items = ServerStore.Cluster.ItemsStartingWith(context, Constants.Documents.Prefix, GetStart(), GetPageSize());

                    if (TryGetAllowedDbs(null, out var allowedDbs, requireAdmin: false) == false)
                        return Task.CompletedTask;

                    if (allowedDbs != null)
                    {
                        items = items.Where(item => allowedDbs.ContainsKey(item.Item1.Substring(Constants.Documents.Prefix.Length)));
                    }

                    writer.WriteArray(context, nameof(DatabasesInfo.Databases), items, (w, c, dbDoc) =>
                    {
                        var databaseName = dbDoc.Item1.Substring(Constants.Documents.Prefix.Length);
                        if (namesOnly)
                        {
                            w.WriteString(databaseName);
                            return;
                        }

                        WriteDatabaseInfo(databaseName, dbDoc.Item2, context, w);
                    });
                    writer.WriteEndObject();
                }
            }

            return Task.CompletedTask;
        }

        [RavenAction("/topology", "GET", AuthorizationStatus.ValidUser)]
        public Task GetTopology()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var dbId = Constants.Documents.Prefix + name;
                using (context.OpenReadTransaction())
                using (var dbBlit = ServerStore.Cluster.Read(context, dbId, out long _))
                {
                    if (TryGetAllowedDbs(name, out var _, requireAdmin: false) == false)
                        return Task.CompletedTask;

                    if (dbBlit == null)
                    {
                        // here we return 503 so clients will try to failover to another server
                        // if this is a newly created db that we haven't been notified about it yet
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.ServiceUnavailable;
                        HttpContext.Response.Headers["Database-Missing"] = name;
                        using (var writer = new BlittableJsonTextWriter(context, HttpContext.Response.Body))
                        {
                            context.Write(writer,
                                new DynamicJsonValue
                                {
                                    ["Type"] = "Error",
                                    ["Message"] = "Database " + name + " wasn't found"
                                });
                        }
                        return Task.CompletedTask;
                    }

                    var clusterTopology = ServerStore.GetClusterTopology(context);
                    var dbRecord = JsonDeserializationCluster.DatabaseRecord(dbBlit);
                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(writer, new DynamicJsonValue
                        {
                            [nameof(Topology.Nodes)] = new DynamicJsonArray(
                                dbRecord.Topology.Members.Select(x => new DynamicJsonValue
                                {
                                    [nameof(ServerNode.Url)] = GetUrl(x, clusterTopology),
                                    [nameof(ServerNode.ClusterTag)] = x,
                                    [nameof(ServerNode.ServerRole)] = ServerNode.Role.Member,
                                    [nameof(ServerNode.Database)] = dbRecord.DatabaseName
                                })
                                .Concat(dbRecord.Topology.Rehabs.Select(x => new DynamicJsonValue
                                {
                                    [nameof(ServerNode.Url)] = GetUrl(x, clusterTopology),
                                    [nameof(ServerNode.ClusterTag)] = x,
                                    [nameof(ServerNode.Database)] = dbRecord.DatabaseName,
                                    [nameof(ServerNode.ServerRole)] = ServerNode.Role.Rehab
                                })
                                )
                            ),
                            [nameof(Topology.Etag)] = dbRecord.Topology.Stamp.Index
                        });
                    }
                }
            }
            return Task.CompletedTask;
        }

        // we can't use '/database/is-loaded` because that conflict with the `/databases/<db-name>`
        // route prefix
        [RavenAction("/debug/is-loaded", "GET", AuthorizationStatus.ValidUser)]
        public Task IsDatabaseLoaded()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

            if (TryGetAllowedDbs(name, out var _, requireAdmin: false) == false)
                return Task.CompletedTask;

            var isLoaded = ServerStore.DatabasesLandlord.IsDatabaseLoaded(name);
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(IsDatabaseLoadedCommand.CommandResult.DatabaseName)] = name,
                        [nameof(IsDatabaseLoadedCommand.CommandResult.IsLoaded)] = isLoaded
                    });
                    writer.Flush();
                }
            }
            return Task.CompletedTask;
        }

        [RavenAction("/periodic-backup", "GET", AuthorizationStatus.ValidUser)]
        public Task GetPeriodicBackup()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            if (TryGetAllowedDbs(name, out var _, requireAdmin: false) == false)
                return Task.CompletedTask;

            var taskId = GetLongQueryString("taskId", required: true);
            if (taskId == 0)
                throw new ArgumentException("Task ID cannot be 0");

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var databaseRecord = ServerStore.Cluster.ReadDatabase(context, name, out _);
                var periodicBackup = databaseRecord.PeriodicBackups.FirstOrDefault(x => x.TaskId == taskId);
                if (periodicBackup == null)
                    throw new InvalidOperationException($"Periodic backup task ID: {taskId} doesn't exist");

                var databaseRecordBlittable = EntityToBlittable.ConvertEntityToBlittable(periodicBackup, DocumentConventions.Default, context);
                context.Write(writer, databaseRecordBlittable);
                writer.Flush();
            }

            return Task.CompletedTask;
        }

        

        [RavenAction("/periodic-backup/status", "GET", AuthorizationStatus.ValidUser)]
        public Task GetPeriodicBackupStatus()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            if (TryGetAllowedDbs(name, out var _, requireAdmin: false) == false)
                return Task.CompletedTask;

            var taskId = GetLongQueryString("taskId", required: true);
            if (taskId == 0)
                throw new ArgumentException("Task ID cannot be 0");

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            using (var statusBlittable =
                ServerStore.Cluster.Read(context, PeriodicBackupStatus.GenerateItemName(name, taskId.Value)))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName(nameof(GetPeriodicBackupStatusOperationResult.Status));
                writer.WriteObject(statusBlittable);
                writer.WriteEndObject();
                writer.Flush();
            }

            return Task.CompletedTask;
        }

        [RavenAction("/periodic-backup/next-backup-occurrence", "GET", AuthorizationStatus.ValidUser)]
        public Task GetNextBackupOccurrence()
        {
            var dateAsString = GetQueryStringValueAndAssertIfSingleAndNotEmpty("date");
            if (DateTime.TryParse(dateAsString, out DateTime date) == false)
                throw new ArgumentException("Date");

            var backupFrequency = GetQueryStringValueAndAssertIfSingleAndNotEmpty("backupFrequency");

            CrontabSchedule crontabSchedule;
            try
            {
                // will throw if the backup frequency is invalid
                crontabSchedule = CrontabSchedule.Parse(backupFrequency);
            }
            catch (Exception e)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                using (var streamWriter = new StreamWriter(ResponseBodyStream()))
                {
                    streamWriter.Write(e.Message);
                    streamWriter.Flush();
                }
                return Task.CompletedTask;
            }

            var nextOccurrence = crontabSchedule.GetNextOccurrence(date);

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName(nameof(NextBackupOccurrence.DateTime));
                writer.WriteDateTime(nextOccurrence, false);
                writer.WriteEndObject();
                writer.Flush();
            }

            return Task.CompletedTask;
        }

        private string GetUrl(string tag, ClusterTopology clusterTopology)
        {
            string url = null;

            if (Server.ServerStore.NodeTag == tag)
                url = ServerStore.NodeHttpServerUrl;

            if (url == null)
                url = clusterTopology.GetUrlFromTag(tag);

            return url;
        }

        private Task DbInfo(string dbName)
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                context.OpenReadTransaction();
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var dbId = Constants.Documents.Prefix + dbName;
                    using (var dbRecord = ServerStore.Cluster.Read(context, dbId, out long _))
                    {
                        if (dbRecord == null)
                        {
                            HttpContext.Response.Headers.Remove("Content-Type");
                            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                            HttpContext.Response.Headers["Database-Missing"] = dbName;
                            return Task.CompletedTask;
                        }
                        WriteDatabaseInfo(dbName, dbRecord, context, writer);
                    }
                    return Task.CompletedTask;
                }
            }
        }

        private void WriteDatabaseInfo(string databaseName, BlittableJsonReaderObject dbRecordBlittable,
            TransactionOperationContext context, BlittableJsonTextWriter writer)
        {
            var online = ServerStore.DatabasesLandlord.DatabasesCache.TryGetValue(databaseName, out Task<DocumentDatabase> dbTask) &&
                         dbTask != null &&
                         dbTask.IsCompleted;

            // Check for exceptions
            if (dbTask != null && dbTask.IsFaulted)
            {
                WriteFaultedDatabaseInfo(context, writer, dbTask, databaseName);
                return;
            }

            var dbRecord = JsonDeserializationCluster.DatabaseRecord(dbRecordBlittable);
            var db = online ? dbTask.Result : null;

            var indexingStatus = db?.IndexStore.Status ?? IndexRunningStatus.Running;
            // Looking for disabled indexing flag inside the database settings for offline database status
            if (dbRecord.Settings.TryGetValue(RavenConfiguration.GetKey(x => x.Indexing.Disabled), out var val) && bool.TryParse(val, out var indexingDisabled) && indexingDisabled)
                indexingStatus = IndexRunningStatus.Disabled;

            var disabled = dbRecord.Disabled;
            var topology = dbRecord.Topology;
            var clusterTopology = ServerStore.GetClusterTopology(context);

            var nodesTopology = new NodesTopology();

            if (topology != null)
            {
                foreach (var member in topology.Members)
                {
                    var url = clusterTopology.GetUrlFromTag(member);
                    var node = new InternalReplication
                    {
                        Database = databaseName,
                        NodeTag = member,
                        Url = url
                    };
                    nodesTopology.Members.Add(GetNodeId(node));
                    SetNodeStatus(topology, member, nodesTopology);
                }

                foreach (var promotable in topology.Promotables)
                {
                    var node = GetNode(databaseName, clusterTopology, promotable, out var promotableTask);
                    var mentor = topology.WhoseTaskIsIt(promotableTask, ServerStore.IsPassive());
                    nodesTopology.Promotables.Add(GetNodeId(node, mentor));
                    SetNodeStatus(topology, promotable, nodesTopology);
                }

                foreach (var rehab in topology.Rehabs)
                {
                    var node = GetNode(databaseName, clusterTopology, rehab, out var promotableTask);
                    var mentor = topology.WhoseTaskIsIt(promotableTask, ServerStore.IsPassive());
                    nodesTopology.Rehabs.Add(GetNodeId(node, mentor));
                    SetNodeStatus(topology, rehab, nodesTopology);
                }
            }

            if (online == false)
            {
                // If state of database is found in the cache we can continue
                if (ServerStore.DatabaseInfoCache.TryWriteOfflineDatabaseStatusToRequest(
                    context, writer, databaseName, disabled, indexingStatus, nodesTopology))
                {
                    return;
                }
                // We won't find it if it is a new database or after a dirty shutdown, so just report empty values then
            }

            var size = new Size(GetTotalSize(db));

            var databaseInfo = new DatabaseInfo
            {
                Name = databaseName,
                Disabled = disabled,
                TotalSize = size,

                IsAdmin = true, //TODO: implement me!
                IsEncrypted = dbRecord.Encrypted,
                UpTime = online ? (TimeSpan?)GetUptime(db) : null,
                BackupInfo = GetBackupInfo(db),

                Alerts = db?.NotificationCenter.GetAlertCount() ?? 0,
                RejectClients = false, //TODO: implement me!
                LoadError = null,
                IndexingErrors = db?.IndexStore.GetIndexes().Sum(index => index.GetErrorCount()) ?? 0,

                DocumentsCount = db?.DocumentsStorage.GetNumberOfDocuments() ?? 0,
                HasRevisionsConfiguration = db?.DocumentsStorage.RevisionsStorage.Configuration != null,
                HasExpirationConfiguration = db?.ExpiredDocumentsCleaner != null,
                IndexesCount = db?.IndexStore.GetIndexes().Count() ?? 0,
                IndexingStatus = indexingStatus,

                NodesTopology = nodesTopology,
                ReplicationFactor = topology?.ReplicationFactor ?? -1,
                DynamicNodesDistribution = topology?.DynamicNodesDistribution ?? false,
                DeletionInProgress = dbRecord.DeletionInProgress
            };

            var doc = databaseInfo.ToJson();
            context.Write(writer, doc);
        }

        private static void SetNodeStatus(DatabaseTopology topology, string nodeTag, NodesTopology nodesTopology)
        {
            var nodeStatus = new DbGroupNodeStatus
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
            nodesTopology.Status[nodeTag] = nodeStatus;
        }

        private static InternalReplication GetNode(string databaseName, ClusterTopology clusterTopology, string rehab, out PromotableTask promotableTask)
        {
            var url = clusterTopology.GetUrlFromTag(rehab);
            var node = new InternalReplication
            {
                Database = databaseName,
                NodeTag = rehab,
                Url = url
            };
            promotableTask = new PromotableTask(rehab, url, databaseName); // internal replication doesn't have a predefined mentor
            return node;
        }

        private void WriteFaultedDatabaseInfo(JsonOperationContext context, BlittableJsonTextWriter writer, Task<DocumentDatabase> dbTask, string databaseName)
        {
            var exception = dbTask.Exception;

            var doc = new DynamicJsonValue
            {
                [nameof(DatabaseInfo.Name)] = databaseName,
                [nameof(DatabaseInfo.LoadError)] = exception.ExtractSingleInnerException().Message
            };

            context.Write(writer, doc);
        }

        private static BackupInfo GetBackupInfo(DocumentDatabase db)
        {
            var periodicBackupRunner = db?.PeriodicBackupRunner;
            return periodicBackupRunner?.GetBackupInfo();
        }

        private static TimeSpan GetUptime(DocumentDatabase db)
        {
            return SystemTime.UtcNow - db.StartTime;
        }

        private static long GetTotalSize(DocumentDatabase db)
        {
            if (db == null)
                return 0;

            return
                db.GetAllStoragesEnvironment().Sum(env => env.Environment.Stats().AllocatedDataFileSizeInBytes);
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

    public class NextBackupOccurrence
    {
        public DateTime DateTime { get; set; }
    }
}
