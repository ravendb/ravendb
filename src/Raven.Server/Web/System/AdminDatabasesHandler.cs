// -----------------------------------------------------------------------
//  <copyright file="AdminDatabasesHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.Json.Converters;
using Raven.Client.Server;
using Raven.Client.Server.Commands;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication;
using Raven.Server.Extensions;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications.Server;
using Raven.Server.Rachis;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.System
{
    public class AdminDatabasesHandler : RequestHandler
    {

        [RavenAction("/admin/databases/is-loaded", "GET", "/admin/databases/is-loaded?name={databaseName:string}")]
        public Task IsDatabaseLoaded()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
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

        [RavenAction("/admin/databases", "GET", "/admin/databases?name={databaseName:string}")]
        public Task Get()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                var dbId = Constants.Documents.Prefix + name;
                long etag;
                using (context.OpenReadTransaction())
                using (var dbDoc = ServerStore.Cluster.Read(context, dbId, out etag))
                {
                    if (dbDoc == null)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
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

                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        writer.WriteDocument(context, new Document
                        {
                            Etag = etag,
                            Data = dbDoc,
                        });
                    }
                }
            }

            return Task.CompletedTask;
        }

        // add database to already existing database group
        [RavenAction("/admin/databases/add-node", "POST", "/admin/databases/add-node?name={databaseName:string}&node={nodeName:string|optional}")]
        public async Task AddDatabaseNode()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            var node = GetStringQueryString("node", false);

            string errorMessage;
            if (ResourceNameValidator.IsValidResourceName(name, ServerStore.Configuration.Core.DataDirectory.FullPath, out errorMessage) == false)
                throw new BadRequestException(errorMessage);

            ServerStore.EnsureNotPassive();
            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                long etag;
                var databaseRecord = ServerStore.Cluster.ReadDatabase(context, name, out etag);
                var clusterTopology = ServerStore.GetClusterTopology(context);
                //The case where an explicit node was requested 
                if (string.IsNullOrEmpty(node) == false)
                {
                    if (databaseRecord.Topology.RelevantFor(node))
                        throw new InvalidOperationException($"Can't add node {node} to {name} topology because it is already part of it");

                    databaseRecord.Topology.Promotables.Add(new DatabaseTopologyNode
                    {
                        Database = name,
                        NodeTag = node,
                        Url = clusterTopology.GetUrlFromTag(node)
                    });
                }

                //The case were we don't care where the database will be added to
                else
                {                                        
                    var allNodes = clusterTopology.Members.Keys
                        .Concat(clusterTopology.Promotables.Keys)
                        .Concat(clusterTopology.Watchers.Keys)
                        .ToList();
                    allNodes.RemoveAll(n => databaseRecord.Topology.AllNodes.Contains(n));
                    var rand = new Random().Next();
                    var newNode = allNodes[rand % allNodes.Count];

                    databaseRecord.Topology.Promotables.Add(new DatabaseTopologyNode
                    {
                        Database = name,
                        NodeTag = newNode,
                        Url = clusterTopology.GetUrlFromTag(newNode)
                    });                   
                }

                var topologyJson = EntityToBlittable.ConvertEntityToBlittable(databaseRecord, DocumentConventions.Default, context);

                var (index, _) = await ServerStore.WriteDbAsync(name, topologyJson, etag).ThrowOnTimeout();
                await ServerStore.Cluster.WaitForIndexNotification(index).ThrowOnTimeout();

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(DatabasePutResult.ETag)] = index,
                        [nameof(DatabasePutResult.Key)] = name,
                        [nameof(DatabasePutResult.Topology)] = databaseRecord.Topology.ToJson()
                    });
                    writer.Flush();
                }
            }
        }
        
        [RavenAction("/admin/databases", "PUT", "/admin/databases/{databaseName:string}")]
        public async Task Put()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            var nodesAddedTo = new List<string>();

            string errorMessage;
            if (ResourceNameValidator.IsValidResourceName(name, ServerStore.Configuration.Core.DataDirectory.FullPath, out errorMessage) == false)
                throw new BadRequestException(errorMessage);

            ServerStore.EnsureNotPassive();
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                context.OpenReadTransaction();

                var etag = GetLongFromHeaders("ETag");

                var json = context.ReadForDisk(RequestBodyStream(), name);
                var document = JsonDeserializationCluster.DatabaseRecord(json);

                try
                {
                    DatabaseHelper.Validate(name, document);
                }
                catch (Exception e)
                {
                    throw new BadRequestException("Database document validation failed.", e);
                }

                DatabaseTopology topology;
                if (document.Topology?.Members?.Count > 0)
                {
                    topology = document.Topology;
                    ValidateClusterMembers(context, topology);
                }
                else
                {
                    var factor = Math.Max(1, GetIntValueQueryString("replication-factor", required: false) ?? 0);
                    topology = AssignNodesToDatabase(context, factor, name, json, out nodesAddedTo);
                }


                var (index, _) = await ServerStore.WriteDbAsync(name, json, etag).ThrowOnTimeout();
                await ServerStore.Cluster.WaitForIndexNotification(index).ThrowOnTimeout();

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(DatabasePutResult.ETag)] = index,
                        [nameof(DatabasePutResult.Key)] = name,
                        [nameof(DatabasePutResult.Topology)] = topology.ToJson(),
                        [nameof(DatabasePutResult.NodesAddedTo)] = nodesAddedTo
                    });
                    writer.Flush();
                }
            }
        }

        private DatabaseTopology AssignNodesToDatabase(TransactionOperationContext context, int factor, string name, BlittableJsonReaderObject json, out List<string> nodesAddedTo)
        {
            var topology = new DatabaseTopology();

            var clusterTopology = ServerStore.GetClusterTopology(context);

            var allNodes = clusterTopology.Members.Keys
                .Concat(clusterTopology.Promotables.Keys)
                .Concat(clusterTopology.Watchers.Keys)
                .ToArray();

            var offset = new Random().Next();
            nodesAddedTo = new List<string>();

            for (int i = 0; i < Math.Min(allNodes.Length, factor); i++)
            {
                var selectedNode = allNodes[(i + offset) % allNodes.Length];
                var url = clusterTopology.GetUrlFromTag(selectedNode);
                topology.Members.Add(new DatabaseTopologyNode
                {
                    Database = name,
                    NodeTag = selectedNode,
                    Url = url
                });
                nodesAddedTo.Add(url);
            }

            var topologyJson = EntityToBlittable.ConvertEntityToBlittable(topology, DocumentConventions.Default, context);

            json.Modifications = new DynamicJsonValue(json)
            {
                [nameof(DatabaseRecord.DatabaseName)] = name,
                [nameof(DatabaseRecord.Topology)] = topologyJson,
            };

            return topology;
        }

        private void ValidateClusterMembers(TransactionOperationContext context, DatabaseTopology topology)
        {
            var clusterTopology = ServerStore.GetClusterTopology(context);

            foreach (var node in topology.AllReplicationNodes())
            {
                var result = clusterTopology.TryGetNodeTagByUrl(node.Url);
                if(result.hasUrl == false || result.nodeTag != node.NodeTag)
                    throw new InvalidOperationException($"The Url {node.Url} for node {node.NodeTag} is not a part of the cluster, the incoming topology is wrong!");
            }
        }

        [RavenAction("/admin/expiration/config", "POST", "/admin/config-expiration?name={databaseName:string}")]
        public async Task ConfigExpirationBundle()
        {
            await DatabaseConfigurations(ServerStore.ModifyDatabaseExpiration, "read-expiration-config").ThrowOnTimeout(); 
        }

        [RavenAction("/admin/versioning/config", "POST", "/admin/config-versioning?name={databaseName:string}")]
        public async Task ConfigVersioning()
        {
            await DatabaseConfigurations(ServerStore.ModifyDatabaseVersioning, "read-versioning-config").ThrowOnTimeout();
        }

        [RavenAction("/admin/periodic-backup/config", "POST", "/admin/config-periodic-backup?name={databaseName:string}")]
        public async Task ConfigPeriodicBackup()
        {
            await DatabaseConfigurations(ServerStore.ModifyDatabasePeriodicBackup, "read-periodic-backup-config").ThrowOnTimeout();
        }

        private async Task DatabaseConfigurations(Func<TransactionOperationContext, string, BlittableJsonReaderObject, Task<(long, BlittableJsonReaderObject)>> setupConfigurationFunc, string debug)
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            string errorMessage;
            if (ResourceNameValidator.IsValidResourceName(name, ServerStore.Configuration.Core.DataDirectory.FullPath, out errorMessage) == false)
                throw new BadRequestException(errorMessage);

            ServerStore.EnsureNotPassive();
            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {                
                var configurationJson = await context.ReadForMemoryAsync(RequestBodyStream(), debug);
                var (index, _) = await setupConfigurationFunc(context, name, configurationJson);
                DatabaseRecord dbRecord;
                using (context.OpenReadTransaction())                    
                {
                    //TODO: maybe have a timeout here for long loading operations
                    dbRecord = ServerStore.Cluster.ReadDatabase(context, name);
                }
                if (dbRecord.Topology.RelevantFor(ServerStore.NodeTag))
                {
                    var db = await ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(name).ThrowOnTimeout();
                    await db.WaitForIndexNotification(index).ThrowOnTimeout();
                }
                else
                {
                    await ServerStore.Cluster.WaitForIndexNotification(index).ThrowOnTimeout();
                }        
                HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        ["ETag"] = index
                    });
                    writer.Flush();
                }
            }
        }
        
        [RavenAction("/admin/modify-watchers", "POST", "/admin/modify-watchers?name={databaseName:string}")]
        public async Task ModifyWathcers()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");  

            string errorMessage;
            if (ResourceNameValidator.IsValidResourceName(name, ServerStore.Configuration.Core.DataDirectory.FullPath, out errorMessage) == false)
                throw new BadRequestException(errorMessage);

            ServerStore.EnsureNotPassive();
            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                var updateJson = await context.ReadForMemoryAsync(RequestBodyStream(), "read-modify-watchers").ThrowOnTimeout();
                if (updateJson.TryGet(nameof(DatabaseTopology.Watchers), out BlittableJsonReaderArray watchersBlittable) == false)
                {                    
                    throw new InvalidDataException("NewWatchers property was not found.");
                }
                using (context.OpenReadTransaction())
                {              
                    long etag;
                    var databaseRecord = ServerStore.Cluster.ReadDatabase(context, name, out etag);
                    var watchers = new List<DatabaseWatcher>(watchersBlittable.Length);
                    foreach (BlittableJsonReaderObject watcher in watchersBlittable)
                    {
                        watchers.Add(JsonDeserializationClient.DatabaseWatcher(watcher));
                    }
                    var (index, _) = await ServerStore.ModifyDatabaseWatchers(name, watchers).ThrowOnTimeout();
                    await ServerStore.Cluster.WaitForIndexNotification(index).ThrowOnTimeout();

                    HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(writer, new DynamicJsonValue
                        {
                            [nameof(DatabasePutResult.ETag)] = index,
                            [nameof(DatabasePutResult.Key)] = name,
                            [nameof(DatabasePutResult.Topology)] = databaseRecord.Topology.ToJson()
                        });
                        writer.Flush();
                    }
                }
            }
        }

        [RavenAction("/admin/update-resolver", "POST", "/admin/update-resolver?name={databaseName:string}")]
        public async Task ChangeConflictResolver()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            string errorMessage;
            if (ResourceNameValidator.IsValidResourceName(name, ServerStore.Configuration.Core.DataDirectory.FullPath, out errorMessage) == false)
                throw new BadRequestException(errorMessage);

            ServerStore.EnsureNotPassive();
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), "read-conflict-resolver").ThrowOnTimeout();
                var conflictResolver = (ConflictSolver)EntityToBlittable.ConvertToEntity(typeof(ConflictSolver), "convert-conflict-resolver", json, DocumentConventions.Default);

                using (context.OpenReadTransaction())
                {
                    long etag;
                    var databaseRecord = ServerStore.Cluster.ReadDatabase(context, name, out etag);

                    var (index,_) = await ServerStore.ModifyConflictSolverAsync(name, conflictResolver).ThrowOnTimeout();
                    await ServerStore.Cluster.WaitForIndexNotification(index).ThrowOnTimeout();
                    
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(writer, new DynamicJsonValue
                        {
                            ["ETag"] = index,
                            ["Key"] = name,
                            [nameof(DatabaseRecord.ConflictSolverConfig)] = databaseRecord.ConflictSolverConfig.ToJson()
                        });
                        writer.Flush();
                    }
                }
            }
        }

        [RavenAction("/admin/databases", "DELETE", "/admin/databases?name={databaseName:string|multiple}&hard-delete={isHardDelete:bool|optional(false)}&from-node={nodeToDelete:string|optional(null)}")]
        public async Task DeleteQueryString()
        {
            var names = GetStringValuesQueryString("name");
            var fromNode = GetStringValuesQueryString("from-node", required: false).FirstOrDefault();
            var isHardDelete = GetBoolValueQueryString("hard-delete", required: false) ?? false;
            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                if (string.IsNullOrEmpty(fromNode) == false)
                {
                    using (context.OpenReadTransaction())
                    {
                        foreach (var databaseName in names)
                        {
                            if (ServerStore.Cluster.ReadDatabase(context, databaseName)?.Topology.RelevantFor(fromNode) == false)
                            {
                                throw new InvalidOperationException($"Database={databaseName} doesn't reside in node={fromNode} so it can't be deleted from it");
                            }
                        }
                    }
                }

                long etag = -1;
                foreach (var name in names)
                {
                    var (newEtag, _) = await ServerStore.DeleteDatabaseAsync(name, isHardDelete, fromNode).ThrowOnTimeout();
                    etag = newEtag;
                }
                await ServerStore.Cluster.WaitForIndexNotification(etag).ThrowOnTimeout();
                HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        ["ETag"] = etag
                    });
                    writer.Flush();
                }
            }
        }
        
        [RavenAction("/admin/databases/disable", "POST", "/admin/databases/disable?name={resourceName:string|multiple}")]
        public async Task DisableDatabases()
        {
            await ToggleDisableDatabases(disableRequested: true).ThrowOnTimeout();
        }

        [RavenAction("/admin/databases/enable", "POST", "/admin/databases/enable?name={resourceName:string|multiple}")]
        public async Task EnableDatabases()
        {
            await ToggleDisableDatabases(disableRequested: false).ThrowOnTimeout();
        }

        private async Task ToggleDisableDatabases(bool disableRequested)
        {
            var names = GetStringValuesQueryString("name");

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Status");

                writer.WriteStartArray();
                var first = true;
                foreach (var name in names)
                {
                    if (first == false)
                        writer.WriteComma();
                    first = false;

                    DatabaseRecord dbDoc;
                    using (context.OpenReadTransaction())
                        dbDoc = ServerStore.Cluster.ReadDatabase(context, name);

                    if (dbDoc == null)
                    {
                        context.Write(writer, new DynamicJsonValue
                        {
                            ["Name"] = name,
                            ["Success"] = false,
                            ["Reason"] = "database not found",
                        });
                        continue;
                    }

                    if (dbDoc.Disabled == disableRequested)
                    {
                        var state = disableRequested ? "disabled" : "enabled";
                        context.Write(writer, new DynamicJsonValue
                        {
                            ["Name"] = name,
                            ["Success"] = true, //even if we have nothing to do, no reason to return failure status
                            ["Disabled"] = disableRequested,
                            ["Reason"] = $"Database already {state}",
                        });
                        continue;
                    }

                    dbDoc.Disabled = disableRequested;

                    var json = EntityToBlittable.ConvertEntityToBlittable(dbDoc, DocumentConventions.Default, context);

                    var (index, result) = await ServerStore.WriteDbAsync(name, json, null).ThrowOnTimeout();
                    await ServerStore.Cluster.WaitForIndexNotification(index).ThrowOnTimeout();

                    context.Write(writer, new DynamicJsonValue
                    {
                        ["Name"] = name,
                        ["Success"] = true,
                        ["Disabled"] = disableRequested,
                        ["Reason"] = $"Database state={dbDoc.Disabled} was propagated on the cluster"
                    });
                }

                writer.WriteEndArray();

                writer.WriteEndObject();
            }
        }
    }

    public class DatabasePutResult
    {
        public long ETag { get; set; }
        public string Key { get; set; }
        public DatabaseTopology Topology { get; set; }
        public List<string> NodesAddedTo { get; set; }
    }

}