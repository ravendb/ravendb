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
using Microsoft.Extensions.Primitives;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.Server;
using Raven.Client.Server.Commands;
using Raven.Server.Config;
using Raven.Server.Documents;
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

        [RavenAction("/admin/add-database", "POST", "/admin/add-database?name={databaseName:string}&node={nodeName:string|optional}")]
        public async Task Add()
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
                //The case where an explicit node was requested 
                if (string.IsNullOrEmpty(node) == false)
                {
                    if (databaseRecord.Topology.RelevantFor(node))
                        throw new InvalidOperationException($"Can't add node {node} to {name} topology because it is already part of it");
                    
                    //TODO:add as promotable 

                    databaseRecord.Topology.AddMember(node,name);
                }
                //The case were we don't care where the database will be added to
                else
                {
                    var clusterTopology = ServerStore.GetClusterTopology(context);
                    var allNodes = clusterTopology.Members.Keys
                        .Concat(clusterTopology.Promotables.Keys)
                        .Concat(clusterTopology.Watchers.Keys)
                        .ToList();
                    allNodes.RemoveAll(n => databaseRecord.Topology.AllNodes.Contains(n));
                    var rand = new Random().Next();
                    var newNode = allNodes[rand % allNodes.Count];
                    //TODO:add as promotable 
                    databaseRecord.Topology.AddMember(newNode, name);
                }

                var topologyJson = EntityToBlittable.ConvertEntityToBlittable(databaseRecord, DocumentConventions.Default, context);

                var index = await ServerStore.WriteDbAsync(context, name, topologyJson, etag);
                await ServerStore.Cluster.WaitForIndexNotification(index);
                ServerStore.NotificationCenter.Add(DatabaseChanged.Create(name, DatabaseChangeType.Put));

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        ["ETag"] = index,
                        ["Key"] = name,
                        [nameof(DatabaseRecord.Topology)] = databaseRecord.Topology.ToJson()
                    });
                    writer.Flush();
                }
            }
        }

        [RavenAction("/admin/databases", "PUT", "/admin/databases/{databaseName:string}")]
        public async Task Put()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

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
                var factor = Math.Max(1, GetIntValueQueryString("replication-factor", required: false) ?? 0);
                var topology = new DatabaseTopology();
                
                var clusterTopology = ServerStore.GetClusterTopology(context);

                var allNodes = clusterTopology.Members.Keys
                    .Concat(clusterTopology.Promotables.Keys)
                    .Concat(clusterTopology.Watchers.Keys)
                    .ToArray();

                var offset = new Random().Next();

                foreach (var node in allNodes)
                {
                    topology.NameToUrlMap[node] = clusterTopology.GetUrlFromTag(node);
                }

                for (int i = 0; i < Math.Min(allNodes.Length, factor); i++)
                {
                    var selectedNode = allNodes[(i + offset) % allNodes.Length];
                    topology.AddMember(selectedNode, name);
                }

                var topologyJson = EntityToBlittable.ConvertEntityToBlittable(topology, DocumentConventions.Default, context);

                json.Modifications = new DynamicJsonValue(json)
                {
                    [nameof(DatabaseRecord.DatabaseName)] = name,
                    [nameof(DatabaseRecord.Topology)] = topologyJson,
                };

                var index = await ServerStore.WriteDbAsync(context, name, json, etag);
                await ServerStore.Cluster.WaitForIndexNotification(index);

                ServerStore.NotificationCenter.Add(DatabaseChanged.Create(name, DatabaseChangeType.Put));

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        ["ETag"] = index,
                        ["Key"] = name,
                        [nameof(DatabaseRecord.Topology)] = topology.ToJson(),
                    });
                    writer.Flush();
                }
            }
        }

        [RavenAction("/admin/config-expiration-bundle", "POST", "/admin/config-expiration-bundle?name={databaseName:string}")]
        public async Task ConfigExpirationBundle()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            string errorMessage;
            if (ResourceNameValidator.IsValidResourceName(name, ServerStore.Configuration.Core.DataDirectory.FullPath, out errorMessage) == false)
                throw new BadRequestException(errorMessage);

            ServerStore.EnsureNotPassive();
            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                var configurationJson = await context.ReadForMemoryAsync(RequestBodyStream(), "read-expiration-config");
                var index = await ServerStore.ModifyDatabaseExpirationBundle(context, name, configurationJson);
                await ServerStore.Cluster.WaitForIndexNotification(index);
                ServerStore.NotificationCenter.Add(DatabaseChanged.Create(name, DatabaseChangeType.Update));
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

        [RavenAction("/admin/config-c-bundle", "POST", "/admin/config-versioning-bundle?name={databaseName:string}")]
        public async Task ConfigVersioningBundle()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            string errorMessage;
            if (ResourceNameValidator.IsValidResourceName(name, ServerStore.Configuration.Core.DataDirectory.FullPath, out errorMessage) == false)
                throw new BadRequestException(errorMessage);

            ServerStore.EnsureNotPassive();
            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                var configurationJson = await context.ReadForMemoryAsync(RequestBodyStream(), "read-versioning-config");
                var index = await ServerStore.ModifyDatabaseVersioningBundle(context, name, configurationJson);
                await ServerStore.Cluster.WaitForIndexNotification(index);
                ServerStore.NotificationCenter.Add(DatabaseChanged.Create(name, DatabaseChangeType.Update));
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

        [RavenAction("/admin/config-periodic-export-bundle", "POST", "/admin/config-periodic-export-bundle?name={databaseName:string}")]
        public async Task ConfigPeriodicBackupBundle()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            string errorMessage;
            if (ResourceNameValidator.IsValidResourceName(name, ServerStore.Configuration.Core.DataDirectory.FullPath, out errorMessage) == false)
                throw new BadRequestException(errorMessage);

            ServerStore.EnsureNotPassive();
            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                var configurationJson = await context.ReadForMemoryAsync(RequestBodyStream(), "read-periodic-export-config");
                var index = await ServerStore.ModifyDatabasePeriodicExportBundle(context, name, configurationJson);
                await ServerStore.Cluster.WaitForIndexNotification(index);
                ServerStore.NotificationCenter.Add(DatabaseChanged.Create(name, DatabaseChangeType.Update));
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
                var updateJson = await context.ReadForMemoryAsync(RequestBodyStream(), "read-modify-watchers");
                if (updateJson.TryGet(nameof(DatabaseTopology.Watchers), out BlittableJsonReaderArray watchers) == false)
                {
                    throw new InvalidDataException("NewWatchers property was not found.");
                }
                using (context.OpenReadTransaction())
                {              
                    long etag;
                    var databaseRecord = ServerStore.Cluster.ReadDatabase(context, name, out etag);
                    
                    var index = await ServerStore.ModifyDatabaseWatchers(context, name, watchers);
                    await ServerStore.Cluster.WaitForIndexNotification(index);

                    ServerStore.NotificationCenter.Add(DatabaseChanged.Create(name, DatabaseChangeType.Update));

                    HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(writer, new DynamicJsonValue
                        {
                            ["ETag"] = index,
                            ["Key"] = name,
                            [nameof(DatabaseRecord.Topology)] = databaseRecord.Topology.ToJson()
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
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), "read-conflict-resolver");
                var conflictResolver = (ConflictSolver)EntityToBlittable.ConvertToEntity(typeof(ConflictSolver), "convert-conflict-resolver", json, DocumentConventions.Default);
                var conflictResolverJson = EntityToBlittable.ConvertEntityToBlittable(conflictResolver, DocumentConventions.Default, context);
                using (context.OpenReadTransaction())
                {
                    long etag;
                    var databaseRecord = ServerStore.Cluster.ReadDatabase(context, name, out etag);

                    var index = await ServerStore.ModifyConflictSolverAsync(context, name, conflictResolverJson);
                    await ServerStore.Cluster.WaitForIndexNotification(index);
                    
                    ServerStore.NotificationCenter.Add(DatabaseChanged.Create(name, DatabaseChangeType.Update));

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
                long newEtag = -1;
                foreach (var name in names)
                {
                    newEtag = await ServerStore.DeleteDatabaseAsync(context, name, isHardDelete, fromNode);
                    if (string.IsNullOrEmpty(fromNode))
                    {
                        ServerStore.NotificationCenter.Add(DatabaseChanged.Create(name, DatabaseChangeType.Delete));
                    }
                }
                await ServerStore.Cluster.WaitForIndexNotification(newEtag);
                HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        ["ETag"] = newEtag
                    });
                    writer.Flush();
                }
            }
        }
        
        [RavenAction("/admin/databases/disable", "POST", "/admin/databases/disable?name={resourceName:string|multiple}")]
        public async Task DisableDatabases()
        {
            await ToggleDisableDatabases(disableRequested: true);
        }

        [RavenAction("/admin/databases/enable", "POST", "/admin/databases/enable?name={resourceName:string|multiple}")]
        public async Task EnableDatabases()
        {
            await ToggleDisableDatabases(disableRequested: false);
        }

        private async Task ToggleDisableDatabases(bool disableRequested)
        {
            var names = GetStringValuesQueryString("name");

            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
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

                    var index = await ServerStore.WriteDbAsync(context, name, json, null);
                    await ServerStore.Cluster.WaitForIndexNotification(index);

                    ServerStore.NotificationCenter.Add(DatabaseChanged.Create(name, DatabaseChangeType.Put));

                    context.Write(writer, new DynamicJsonValue
                    {
                        ["Name"] = name,
                        ["Success"] = true,
                        ["Disabled"] = disableRequested,
                        ["Reason"] = $"Database state={dbDoc.Disabled} was propagated on the cluster"
                    });
                }

                writer.WriteEndArray();
            }
        }
    }

    public class DatabaseDeleteResult
    {
        public string Name { get; set; }

        public bool Deleted { get; set; }

        public string Reason { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(Deleted)] = Deleted,
                [nameof(Reason)] = Reason
            };
        }
    }
}