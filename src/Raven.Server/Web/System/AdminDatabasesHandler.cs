// -----------------------------------------------------------------------
//  <copyright file="AdminDatabasesHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;
using Raven.Abstractions.Data;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Json;
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

                var dbId = Constants.Database.Prefix + name;
                long etag;
                var dbDoc = ServerStore.Read(context, dbId, out etag);

                if (dbDoc == null)
                {
                    HttpContext.Response.StatusCode = 404;
                    return HttpContext.Response.WriteAsync("Database " + name + " wasn't found");
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
        public Task Put()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            string errorMessage;
            if (
                ResourceNameValidator.IsValidResourceName(name, ServerStore.Configuration.Core.DataDirectory,
                    out errorMessage) == false)
            {
                HttpContext.Response.StatusCode = 400;
                return HttpContext.Response.WriteAsync(errorMessage);
            }

            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                var dbId = Constants.Database.Prefix + name;

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
                        HttpContext.Response.StatusCode = 400;
                        return HttpContext.Response.WriteAsync(errorMessage);
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

                ServerStore.DatabasesLandlord.UnloadAndLock(name, () =>
                {
                    using (var tx = context.OpenWriteTransaction())
                    {
                        newEtag = hasEtagInRequest ? ServerStore.Write(context, dbId, dbDoc, etag) :
                                                     ServerStore.Write(context, dbId, dbDoc);
                        tx.Commit();
                    }
                });

                HttpContext.Response.StatusCode = 201;

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
            return Task.CompletedTask;
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
                        results.Add(new DynamicJsonValue
                        {
                            ["Name"] = name,
                            ["Deleted"] = false,
                            ["Reason"] = "database not found",
                        });

                        continue;
                    }

                    DeleteDatabase(name, context, isHardDelete, configuration);

                    results.Add(new DynamicJsonValue
                    {
                        ["Name"] = name,
                        ["Deleted"] = true,
                    });
                }

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WriteResults(context, results, (w, c, result) =>
                    {
                        c.Write(w, result);
                    });

                    writer.WriteEndObject();
                }
            }

            return Task.CompletedTask;
        }

        private void DeleteDatabase(string name, TransactionOperationContext context, bool isHardDelete, RavenConfiguration configuration)
        {
            ServerStore.DatabasesLandlord.UnloadAndLock(name, () =>
            {
                var dbId = Constants.Database.Prefix + name;
                using (var tx = context.OpenWriteTransaction())
                {
                    ServerStore.Delete(context, dbId);
                    tx.Commit();
                }

                if (isHardDelete)
                    DatabaseHelper.DeleteDatabaseFiles(configuration);
            });

            ServerStore.DatabaseInfoCache.Delete(name);
        }
    }
}