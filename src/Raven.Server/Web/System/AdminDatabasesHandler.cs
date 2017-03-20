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
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.Http;
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

        [RavenAction("/admin/databases/topology", "GET", "/admin/databases/topology?&name={databaseName:string}")]
        public Task GetTopology()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                var dbId = Constants.Documents.Prefix + name;
                long etag;
                using (context.OpenReadTransaction())
                using (var dbBlit = ServerStore.Cluster.Read(context, dbId, out etag))
                {
                    if (dbBlit == null)
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

                    UnprotectSecuredSettingsOfDatabaseDocument(dbBlit);
                    var clusterTopology = ServerStore.GetClusterTopology(context);
                    var dbRecord = JsonDeserializationCluster.DatabaseRecord(dbBlit);
                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        GenerateTopology(context, writer, dbRecord, clusterTopology, etag);
                    }
                }
            }

            return Task.CompletedTask;
        }

        private void GenerateTopology(JsonOperationContext context, BlittableJsonTextWriter writer, DatabaseRecord dbRecord, ClusterTopology clusterTopology, long etag = 0)
        {
            context.Write(writer, new DynamicJsonValue
            {
                [nameof(Topology.LeaderNode)] = new DynamicJsonValue
                {
                    //TODO:this should return the senator but for now so it will work I'm returning the "primary" node
                    [nameof(ServerNode.Url)] = Server.Configuration.Core.ServerUrl,
                    [nameof(ServerNode.Database)] = dbRecord.DatabaseName,
                },
                [nameof(Topology.Nodes)] = new DynamicJsonArray(dbRecord.Topology.AllNodes.Select(x => new DynamicJsonValue
                {
                    [nameof(ServerNode.Url)] = clusterTopology.GetUrlFormTag(x),
                    [nameof(ServerNode.Database)] = dbRecord.DatabaseName,
                })),
                [nameof(Topology.ReadBehavior)] =
                ReadBehavior.LeaderWithFailoverWhenRequestTimeSlaThresholdIsReached.ToString(),
                [nameof(Topology.WriteBehavior)] = WriteBehavior.LeaderOnly.ToString(),
                [nameof(Topology.SLA)] = new DynamicJsonValue
                {
                    [nameof(TopologySla.RequestTimeThresholdInMilliseconds)] = 100,
                },
                [nameof(Topology.Etag)] = etag,
            });
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
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            TransactionOperationContext context;
            string errorMessage;
            if (ResourceNameValidator.IsValidResourceName(name, ServerStore.Configuration.Core.DataDirectory.FullPath, out errorMessage) == false)
                throw new BadRequestException(errorMessage);

            ServerStore.EnsureNotPassive();

            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();

                var dbId = Constants.Documents.Prefix + name;

                var etag = GetLongFromHeaders("ETag");

                var json = context.ReadForDisk(RequestBodyStream(), dbId);
                var document = JsonDeserializationServer.DatabaseDocument(json);

                try
                {
                    DatabaseHelper.Validate(name, document);
                }
                catch (Exception e)
                {
                    throw new BadRequestException("Database document validation failed.", e);
                }
                var factor = Math.Max(1, GetIntValueQueryString("replication-factor") ?? 0);
                var topology = new DatabaseTopology();

                var clusterTopology = ServerStore.GetClusterTopology(context);

                var allNodes = clusterTopology.Members.Keys
                    .Concat(clusterTopology.Promotables.Keys)
                    .Concat(clusterTopology.Watchers.Keys)
                    .ToArray();

                var offset = new Random().Next();

                for (int i = 0; i < Math.Min(allNodes.Length, factor); i++)
                {
                    var selectedNode = allNodes[(i + offset) % allNodes.Length];
                    topology.Members.Add(selectedNode);
                }
                var entityToBlittable = new EntityToBlittable(null);
                var topologyJson = entityToBlittable.ConvertEntityToBlittable(topology, DocumentConventions.Default, context);

                json.Modifications = new DynamicJsonValue(json)
                {
                    [nameof(DatabaseRecord.Topology)] = topologyJson
                };

                var newEtag = await ServerStore.TEMP_WriteDbAsync(context, dbId, json, etag);

                ServerStore.NotificationCenter.Add(DatabaseChanged.Create(name, DatabaseChangeType.Put));

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        ["ETag"] = newEtag,
                        ["Key"] = dbId,
                        [nameof(DatabaseRecord.Topology)] = topology.ToJson()
                    });
                    writer.Flush();
                }
            }
        }

        [RavenAction("/admin/databases", "DELETE", "/admin/databases?name={databaseName:string|multiple}&hard-delete={isHardDelete:bool|optional(false)}&from-node={nodeToDelete:string|optional(null)}")]
        public async Task DeleteQueryString()
        {
            var names = GetStringValuesQueryString("name");
            var fromNode = GetStringValuesQueryString("from-node", required: false).Single();
            var isHardDelete = GetBoolValueQueryString("hard-delete", required: false) ?? false;
            if (string.IsNullOrEmpty(fromNode) == false)
            {
                TransactionOperationContext context;
                using (ServerStore.ContextPool.AllocateOperationContext(out context))
                {
                    context.OpenReadTransaction();
                    var databaseName = names.Single();
                    if (ServerStore.Cluster.ReadDatabase(context, $"db/{databaseName}")?.Topology.RelevantFor(fromNode) == false)
                    {
                        throw new InvalidOperationException($"Database={databaseName} doesn't reside in node={fromNode} so it can't be deleted from it");
                    }
                    var newEtag = await ServerStore.DeleteDatabaseAsync(context, names.Single(), isHardDelete, fromNode);
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
            //return DeleteDatabases(names);
        }

        private Task DeleteDatabases(StringValues names)
        {
          /*  var isHardDelete = GetBoolValueQueryString("hard-delete", required: false) ?? false;

            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                var results = new List<DynamicJsonValue>();
                foreach (var name in names)
                {
                    var configuration = ServerStore.DatabasesLandlord.CreateDatabaseConfiguration(name, ignoreDisabledDatabase: true);
                    if (configuration == null)
                    {
                        results.Add(new DatabaseDeleteResult
                        {
                            Name = name,
                            Deleted = false,
                            Reason = "database not found"
                        }.ToJson());

                        continue;
                    }

                    try
                    {
                        DeleteDatabase(name, context, isHardDelete, configuration);

                        results.Add(new DatabaseDeleteResult
                        {
                            Name = name,
                            Deleted = true
                        }.ToJson());
                    }
                    catch (Exception ex)
                    {
                        results.Add(new DatabaseDeleteResult
                        {
                            Name = name,
                            Deleted = false,
                            Reason = ex.Message
                        }.ToJson());
                    }
                }

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteArray(context, results, (w, c, result) =>
                    {
                        c.Write(w, result);
                    });
                }
            }
*/
            return Task.CompletedTask;
        }

        private void DeleteDatabase(string name, TransactionOperationContext context, bool isHardDelete, RavenConfiguration configuration)
        {
            //ServerStore.DatabasesLandlord.UnloadAndLock(name, () =>
            //{
            //    var dbId = Constants.Documents.Prefix + name;
            //    using (var tx = context.OpenWriteTransaction())
            //    {
            //        //ServerStore.Delete(context, dbId);
            //        ServerStore.NotificationCenter.AddAfterTransactionCommit(
            //            DatabaseChanged.Create(name, DatabaseChangeType.Delete), tx);

            //        tx.Commit();
            //    }

            //    if (isHardDelete)
            //        DatabaseHelper.DeleteDatabaseFiles(configuration);
            //});

            //ServerStore.DatabaseInfoCache.Delete(name);
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