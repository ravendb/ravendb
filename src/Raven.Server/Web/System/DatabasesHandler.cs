using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Client.Server;
using Raven.Client.Server.Operations;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Extensions;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.System
{
    public class DatabasesHandler : RequestHandler
    {

        private bool TryGetAllowedDbs(string dbName, out HashSet<string> dbs)
        {
            dbs = null;
            var feature = HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;
            var status = feature?.Status;
            switch (status)
            {
                case null:
                case RavenServer.AuthenticationStatus.None:
                case RavenServer.AuthenticationStatus.NoCertificateProvided:
                case RavenServer.AuthenticationStatus.UnfamiliarCertificate:
                case RavenServer.AuthenticationStatus.Expired:
                    if (Server.Configuration.Security.AuthenticationEnabled == false)
                        return true;

                    Server.Router.UnlikelyFailAuthorization(HttpContext, dbName, null);
                    return false;
                case RavenServer.AuthenticationStatus.ServerAdmin:
                    return true;
                case RavenServer.AuthenticationStatus.Allowed:
                    if (dbName != null && feature.CanAccess(dbName) == false)
                    {
                        Server.Router.UnlikelyFailAuthorization(HttpContext, dbName, null);
                        return false;
                    }

                    dbs = feature.AuthorizedDatabases;
                    return true;
                default:
                    ThrowInvalidAuthStatus(status);
                    return false;
            }
        }

        private static void ThrowInvalidAuthStatus(RavenServer.AuthenticationStatus? status)
        {
            throw new ArgumentOutOfRangeException("Unknown authentication status: " + status);
        }

        [RavenAction("/databases", "GET", RequiredAuthorization = AuthorizationStatus.ValidUser)]
        public Task Databases()
        {
            var namesOnly = GetBoolValueQueryString("namesOnly", required: false) ?? false;


            //TODO: fill all required information (see: RavenDB-5438) - return Raven.Client.Data.DatabasesInfo
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                context.OpenReadTransaction();
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    var items = ServerStore.Cluster.ItemsStartingWith(context, Constants.Documents.Prefix, GetStart(), GetPageSize());

                    if (TryGetAllowedDbs(null, out var allowedDbs) == false)
                        return Task.CompletedTask;

                    if (allowedDbs != null)
                    {
                        items = items.Where(item => allowedDbs.Contains(item.Item1));
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

        [RavenAction("/topology", "GET", "/topology?name={databaseName:string}", RequiredAuthorization = AuthorizationStatus.ValidUser)]
        public Task GetTopology()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var dbId = Constants.Documents.Prefix + name;
                using (context.OpenReadTransaction())
                using (var dbBlit = ServerStore.Cluster.Read(context, dbId, out long _))
                {

                    if (TryGetAllowedDbs(name, out var _) == false)
                        return Task.CompletedTask;


                    if (dbBlit == null)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.ServiceUnavailable;
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
                                    [nameof(ServerNode.Database)] = dbRecord.DatabaseName,
                                })
                            ),
                            [nameof(Topology.Etag)] = dbRecord.Topology.Stamp.Index,
                        });
                    }
                }
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
            if (dbRecord.Settings.TryGetValue(RavenConfiguration.GetKey(x => x.Indexing.Disabled), out var val) && val == "true")
            {
                indexingStatus = IndexRunningStatus.Disabled;
            }
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
                    nodesTopology.Status[member] = new DbGroupNodeStatus { LastStatus = "Ok" };
                }
                foreach (var promotable in topology.Promotables)
                {
                    var url = clusterTopology.GetUrlFromTag(promotable);
                    var node = new InternalReplication
                    {
                        Database = databaseName,
                        NodeTag = promotable,
                        Url = url
                    };
                    var promotableTask = new PromotableTask(promotable, url, databaseName);
                    nodesTopology.Promotables.Add(GetNodeId(node, topology.WhoseTaskIsIt(promotableTask, ServerStore.IsPassive())));

                    var nodeStatus = new DbGroupNodeStatus();
                    if (topology.PromotablesStatus.TryGetValue(promotable, out var status))
                    {
                        nodeStatus.LastStatus = status;
                    }
                    if (topology.DemotionReasons.TryGetValue(promotable, out var reason))
                    {
                        nodeStatus.LastError = reason;
                    }
                    nodesTopology.Status[promotable] = nodeStatus;
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

                NodesTopology = nodesTopology
            };

            var doc = databaseInfo.ToJson();
            context.Write(writer, doc);
        }

        private void WriteFaultedDatabaseInfo(TransactionOperationContext context, BlittableJsonTextWriter writer, Task<DocumentDatabase> dbTask, string databaseName)
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

        private TimeSpan GetUptime(DocumentDatabase db)
        {
            return SystemTime.UtcNow - db.StartTime;
        }

        private long GetTotalSize(DocumentDatabase db)
        {
            if (db == null)
                return 0;

            return
                db.GetAllStoragesEnvironment().Sum(env => env.Environment.Stats().AllocatedDataFileSizeInBytes);
        }

        private NodeId GetNodeId(InternalReplication node, string responsible = null)
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