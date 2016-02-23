// -----------------------------------------------------------------------
//  <copyright file="AdminDatabasesHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Microsoft.AspNet.Http;
using Raven.Abstractions.Data;
using Raven.Server.Config;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.Web.System
{
    public class AdminDatabasesHandler : RequestHandler
    {
        [RavenAction("/admin/databases/$", "GET", "/admin/databases/{databaseName:string}")]
        public Task Get()
        {
            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();

                var name = RouteMatch.Url.Substring(RouteMatch.MatchLength);
                if (string.IsNullOrWhiteSpace(name))
                    throw new InvalidOperationException("Database name was not provided");
                var dbId = Constants.Database.Prefix + name;
                var dbDoc = ServerStore.Read(context, dbId);
                if (dbDoc == null)
                {
                    HttpContext.Response.StatusCode = 404;
                    return HttpContext.Response.WriteAsync("Database " + name + " wasn't found");
                }

                UnprotectSecuredSettingsOfDatabaseDocument(dbDoc);

                HttpContext.Response.StatusCode = 200;
                // TODO: Implement etags

                context.Write(ResponseBodyStream(), dbDoc);
                return Task.CompletedTask;
            }
        }

        private void UnprotectSecuredSettingsOfDatabaseDocument(BlittableJsonReaderObject obj)
        {
            //TODO: implement this
            object securedSettings;
            if (obj.TryGetMember("SecuredSettings", out securedSettings) == false)
            {

            }
        }

        [RavenAction("/admin/databases/$", "PUT", "/admin/databases/{databaseName:string}")]
        public Task Put()
        {
            var name = RouteMatch.Url.Substring(RouteMatch.MatchLength);

            string errorMessage;
            if (ResourceNameValidator.IsValidResourceName(name, ServerStore.Configuration.Core.DataDirectory, out errorMessage) == false)
            {
                HttpContext.Response.StatusCode = 400;
                return HttpContext.Response.WriteAsync(errorMessage);
            }

            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                var dbId = Constants.Database.Prefix + name;

                var etag = HttpContext.Request.Headers["ETag"];
                using (context.OpenReadTransaction())
                {
                    var existingDatabase = ServerStore.Read(context, dbId);
                    if (DatabaseHelper.CheckExistingDatabaseName(existingDatabase, name, dbId, etag, out errorMessage) == false)
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

                ServerStore.DatabasesLandlord.UnloadAndLock(name, () =>
                {
                    using (var tx = context.OpenWriteTransaction())
                    {
                        ServerStore.Write(context, dbId, dbDoc);
                        tx.Commit();
                    }
                });

                HttpContext.Response.StatusCode = 201;

            }
            return Task.CompletedTask;
        }

        [RavenAction("/admin/databases/$", "DELETE", "/admin/databases/{databaseName:string}?hard-delete={isHardDelete:bool|optional(false)}")]
        public Task Delete()
        {
            var name = RouteMatch.Url.Substring(RouteMatch.MatchLength);
            var isHardDelete = GetBoolValueQueryString("isHardDelete");

            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                var configuration = ServerStore.DatabasesLandlord.CreateDatabaseConfiguration(name);
                if (configuration == null)
                {
                    HttpContext.Response.StatusCode = 404;
                    return HttpContext.Response.WriteAsync("Database wasn't found");
                }

                DeleteDatabase(name, context, isHardDelete, configuration);
                HttpContext.Response.StatusCode = 204; // No Content
                return Task.CompletedTask;
            }
        }

        [RavenAction("/admin/databases", "DELETE", "/admin/databases?name={databaseName:string|multiple}&hard-delete={isHardDelete:bool|optional(false)}")]
        public Task DeleteBatch()
        {
            var names = HttpContext.Request.Query["name"];
            if (names.Count == 0)
                throw new ArgumentException("Query string \'name\' is mandatory, but wasn\'t specified");
            var isHardDelete = GetBoolValueQueryString("isHardDelete");

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

                    var configuration = ServerStore.DatabasesLandlord.CreateDatabaseConfiguration(name);
                    if (configuration == null)
                    {
                        writer.WriteString(context.GetLazyString($"Database {name} wasn't found"));
                        continue;
                    }

                    DeleteDatabase(name, context, isHardDelete, configuration);
                    writer.WriteString(context.GetLazyString($"Database {name} was deleted successfully"));
                }
                writer.WriteEndArray();
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
        }
    }
}