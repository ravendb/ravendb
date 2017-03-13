// -----------------------------------------------------------------------
//  <copyright file="AdminDatabasesHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
using Raven.Client;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications.Server;
using Raven.Server.Routing;
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
                using (var dbDoc = ServerStore.Read(context, dbId, out etag))
                {
                    if (dbDoc == null)
                    {
                        HttpContext.Response.StatusCode = (int) HttpStatusCode.NotFound;

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
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            Task<DocumentDatabase> dbTask;
            var online =
                ServerStore.DatabasesLandlord.ResourcesStoresCache.TryGetValue(name, out dbTask) &&
                dbTask != null && dbTask.IsCompleted;

            TransactionOperationContext context;
            string errorMessage;
            if (
                ResourceNameValidator.IsValidResourceName(name, ServerStore.Configuration.Core.DataDirectory.FullPath,
                    out errorMessage) == false)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                using(ServerStore.ContextPool.AllocateOperationContext(out context))
                using (var writer = new BlittableJsonTextWriter(context, HttpContext.Response.Body))
                {
                    context.Write(writer,
                        new DynamicJsonValue
                        {
                            ["Type"] = "Error",
                            ["Message"] = errorMessage
                        });
                }
                return ;
            }

            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                var dbId = Constants.Documents.Prefix + name;

                var etagAsString = HttpContext.Request.Headers["ETag"];
                long etag;
                var hasEtagInRequest = long.TryParse(etagAsString, out etag);

                using (context.OpenReadTransaction())
                {
                    var existingDatabase = ServerStore.Read(context, dbId);
                    if (
                        DatabaseHelper.CheckExistingDatabaseName(existingDatabase, name, dbId, etagAsString,
                            out errorMessage) == false)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;

                        using (var writer = new BlittableJsonTextWriter(context, HttpContext.Response.Body))
                        {
                            context.Write(writer,
                                new DynamicJsonValue
                                {
                                    ["Type"] = "Error",
                                    ["Message"] = errorMessage
                                });
                        }
                        return ;
                    }
                }

                var dbDoc = context.ReadForDisk(RequestBodyStream(), dbId);

                //TODO: Fix this
                //int size;
                //var buffer = context.GetNativeTempBuffer(dbDoc.SizeInBytes, out size);
                //dbDoc.CopyTo(buffer);

                //var reader = new BlittableJsonReaderObject(buffer, dbDoc.SizeInBytes, context);
                //object result;
                //if (reader.TryGetMember("SecureSettings", out result))
                //{
                //    var secureSettings = (BlittableJsonReaderObject) result;
                //    secureSettings.Unloading = new DynamicJsonValue(secureSettings);
                //    foreach (var propertyName in secureSettings.GetPropertyNames())
                //    {
                //        secureSettings.TryGetMember(propertyName, out result);
                //        // protect
                //        secureSettings.Unloading[propertyName] = "fooo";
                //    }
                //}

                long? newEtag = null;

                //TODO: Etag
                await ServerStore.TEMP_WriteDbAsync(context, dbId, dbDoc);

                ServerStore.DatabasesLandlord.UnloadAndLock(name, () =>
                {
                    //newEtag = hasEtagInRequest ? ServerStore.Write(context, dbId, dbDoc, etag) :
                    //                             ServerStore.Write(context, dbId, dbDoc);

                    ServerStore.NotificationCenter.Add(DatabaseChanged.Create(name, DatabaseChangeType.Put));
                });
                
                object disabled;
                if (online && (dbDoc.TryGetMember("Disabled", out disabled) == false || (bool)disabled == false))
                {
                    var task = ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(name);
                    GC.KeepAlive(task);
                }
              
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
            return ;
        }

        [RavenAction("/admin/databases", "DELETE", "/admin/databases?name={databaseName:string|multiple}&hard-delete={isHardDelete:bool|optional(false)}")]
        public Task DeleteQueryString()
        {
            var names = GetStringValuesQueryString("name");

            return DeleteDatabases(names);
        }

        private Task DeleteDatabases(StringValues names)
        {
            var isHardDelete = GetBoolValueQueryString("hard-delete", required: false) ?? false;

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

            return Task.CompletedTask;
        }

        private void DeleteDatabase(string name, TransactionOperationContext context, bool isHardDelete, RavenConfiguration configuration)
        {
            ServerStore.DatabasesLandlord.UnloadAndLock(name, () =>
            {
                var dbId = Constants.Documents.Prefix + name;
                using (var tx = context.OpenWriteTransaction())
                {
                    ServerStore.Delete(context, dbId);
                    ServerStore.NotificationCenter.AddAfterTransactionCommit(
                        DatabaseChanged.Create(name, DatabaseChangeType.Delete), tx);

                    tx.Commit();
                }

                if (isHardDelete)
                    DatabaseHelper.DeleteDatabaseFiles(configuration);
            });

            ServerStore.DatabaseInfoCache.Delete(name);
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
                    var dbDoc = ServerStore.Read(context, dbId);

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
                /* Right now only database resource is supported */
                ServerStore.DatabasesLandlord.UnloadAndLock(name, () =>
                {
                    // empty by design
                });
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