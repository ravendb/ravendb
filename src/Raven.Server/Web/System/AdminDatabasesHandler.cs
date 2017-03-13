// -----------------------------------------------------------------------
//  <copyright file="AdminDatabasesHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
using Raven.Client;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications.Server;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron.Platform.Win32;

namespace Raven.Server.Web.System
{
    public class AdminDatabasesHandler : RequestHandler
    {
        [RavenAction("/admin/databases", "GET", "/admin/databases/{databaseName:string}")]
        public Task Get()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();

                var dbId = Constants.Documents.Prefix + name;
                long etag;
                using (context.OpenReadTransaction())
                using (var dbDoc = ServerStore.Cluster.Read(context, dbId, out etag))
                {
                    if (dbDoc == null)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;

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

                    UnprotectSecuredSettingsOfDatabaseDocument(dbDoc);

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

        private void UnprotectSecuredSettingsOfDatabaseDocument(BlittableJsonReaderObject obj)
        {
            //TODO: implement this
            object securedSettings;
            if (obj.TryGetMember("SecuredSettings", out securedSettings) == false)
            {

            }
        }

        [RavenAction("/admin/databases", "PUT", "/admin/databases/{databaseName:string}")]
        public async Task Put()
        {
            // TODO: Redirect to leader


            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            TransactionOperationContext context;
            string errorMessage;
            if (
                ResourceNameValidator.IsValidResourceName(name, ServerStore.Configuration.Core.DataDirectory.FullPath,
                    out errorMessage) == false)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                using (ServerStore.ContextPool.AllocateOperationContext(out context))
                using (var writer = new BlittableJsonTextWriter(context, HttpContext.Response.Body))
                {
                    context.Write(writer,
                        new DynamicJsonValue
                        {
                            ["Type"] = "Error",
                            ["Message"] = errorMessage
                        });
                }
                return;
            }

            ServerStore.EnsureNotPassive();

            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                var dbId = Constants.Documents.Prefix + name;

                var etag = GetLongFromHeaders("ETag");

                var dbDoc = context.ReadForDisk(RequestBodyStream(), dbId);

                var databaseRecord = JsonDeserializationCluster.DatabaseRecord(dbDoc);

                var factor = Math.Max(1, GetIntValueQueryString("replication-factor") ?? 0);

                if ((databaseRecord.Topology?.Members?.Count ?? 0) == 0)
                {
                    databaseRecord.Topology = new DatabaseTopology();

                    var clusterTopology = ServerStore.GetClusterTopology(context);

                    var allNodes = clusterTopology.Members.Keys
                        .Concat(clusterTopology.Promotables.Keys)
                        .Concat(clusterTopology.Watchers.Keys)
                        .ToArray();

                    var offset = new Random().Next();

                    for (int i = 0; i < Math.Min(allNodes.Length, factor); i++)
                    {
                        var selectedNode = allNodes[(i + offset) % allNodes.Length];
                        databaseRecord.Topology.Members.Add(selectedNode);
                    }
                    var entityToBlittable = new EntityToBlittable(null);
                    dbDoc = entityToBlittable.ConvertEntityToBlittable(databaseRecord, DocumentConventions.Default, context);
                }

                var newEtag = await ServerStore.TEMP_WriteDbAsync(context, dbId, dbDoc, etag);

                ServerStore.NotificationCenter.Add(DatabaseChanged.Create(name, DatabaseChangeType.Put));

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        ["ETag"] = newEtag,
                        ["Key"] = dbId
                    });
                    writer.Flush();
                }
            }
        }

        [RavenAction("/admin/databases", "DELETE", "/admin/databases?name={databaseName:string|multiple}")]
        public async Task DeleteQueryString()
        {
            var names = GetStringValuesQueryString("name");

            var tasks = new List<Task>();

            JsonOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                foreach (var name in names)
                {
                    tasks.Add(ServerStore.DeleteDatabaseAsync(context, name));
                }

                await Task.WhenAll(tasks);
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

        private Task ToggleDisableDatabases(bool disableRequested)
        {
            throw new NotSupportedException();
            /*
             var names = GetStringValuesQueryString("name");

            var databasesToUnload = new List<string>();

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

                    var dbId = Constants.Documents.Prefix + name;
                    var dbDoc = ServerStore.Cluster.Read(context, dbId);

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

                    object disabledValue;
                    if (dbDoc.TryGetMember("Disabled", out disabledValue))
                    {
                        if ((bool)disabledValue == disableRequested)
                        {
                            var state = disableRequested ? "disabled" : "enabled";
                            context.Write(writer, new DynamicJsonValue
                            {
                                ["Name"] = name,
                                ["Success"] = false,
                                ["Disabled"] = disableRequested,
                                ["Reason"] = $"Database already {state}",
                            });
                            continue;
                        }
                    }

                    dbDoc.Modifications = new DynamicJsonValue(dbDoc)
                    {
                        ["Disabled"] = disableRequested
                    };

                    var newDoc2 = context.ReadObject(dbDoc, dbId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);

                    await ServerStore.TEMP_WriteDbAsync(context, dbId, newDoc2);
                    ServerStore.NotificationCenter.Add(DatabaseChanged.Create(name, DatabaseChangeType.Put));

                    databasesToUnload.Add(name);

                    context.Write(writer, new DynamicJsonValue
                    {
                        ["Name"] = name,
                        ["Success"] = true,
                        ["Disabled"] = disableRequested,
                    });
                }

                writer.WriteEndArray();
            }

            foreach (var name in databasesToUnload)
            {
              
            ServerStore.DatabasesLandlord.UnloadAndLock(name, () =>
            {
                // empty by design
            });
        }*/
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