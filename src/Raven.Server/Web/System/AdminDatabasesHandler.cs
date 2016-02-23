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
            object securedSettings;
            if (obj.TryGetMember("SecuredSettings", out securedSettings) == false)
            {

            }
        }

        [RavenAction("/admin/databases/$", "PUT", "/admin/databases/{databaseName:string}")]
        public async Task Put()
        {
            var name = RouteMatch.Url.Substring(RouteMatch.MatchLength);

            string errorMessage;
            if (ResourceNameValidator.IsValidResourceName(name, ServerStore.Configuration.Core.DataDirectory, out errorMessage) == false)
            {
                HttpContext.Response.StatusCode = 400;
                await HttpContext.Response.WriteAsync(errorMessage);
                return;
            }

            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenWriteTransaction();
                var dbId = Constants.Database.Prefix + name;

                var etag = HttpContext.Request.Headers["ETag"];
                var existingDatabase = ServerStore.Read(context, dbId);
                if (DatabaseHelper.CheckExistingDatabaseName(existingDatabase, name, dbId, etag, out errorMessage) == false)
                {
                    HttpContext.Response.StatusCode = 400;
                    await HttpContext.Response.WriteAsync(errorMessage);
                    return;
                }

                var dbDoc = await context.ReadForDiskAsync(RequestBodyStream(), dbId);

                //TODO: Fix this
                //int size;
                //var buffer = context.GetNativeTempBuffer(dbDoc.SizeInBytes, out size);
                //dbDoc.CopyTo(buffer);

                //var reader = new BlittableJsonReaderObject(buffer, dbDoc.SizeInBytes, context);
                //object result;
                //if (reader.TryGetMember("SecureSettings", out result))
                //{
                //    var secureSettings = (BlittableJsonReaderObject) result;
                //    secureSettings.Modifications = new DynamicJsonValue(secureSettings);
                //    foreach (var propertyName in secureSettings.GetPropertyNames())
                //    {
                //        secureSettings.TryGetMember(propertyName, out result);
                //        // protect
                //        secureSettings.Modifications[propertyName] = "fooo";
                //    }
                //}


                ServerStore.Write(context, dbId, dbDoc);
                context.Transaction.Commit();

#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                Task.Run(() => ServerStore.DatabasesLandlord.ModifyResource(name));
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed

                HttpContext.Response.StatusCode = 201;

            }
        }

        [RavenAction("/admin/databases/$", "DELETE", "/admin/databases/{databaseName:string}?hard-delete={isHardDelete:bool|optional(false)}")]
        public async Task Delete()
        {
            var name = RouteMatch.Url.Substring(RouteMatch.MatchLength);
            var isHardDelete = GetBoolValueQueryString("isHardDelete", DefaultValue<bool>.Default);

            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenWriteTransaction();

                var configuration = ServerStore.DatabasesLandlord.CreateDatabaseConfiguration(name);
                if (configuration == null)
                {
                    HttpContext.Response.StatusCode = 404;
                    await HttpContext.Response.WriteAsync("Database wasn't found");
                }

                await DeleteDatabase(name, context, isHardDelete, configuration);
                HttpContext.Response.StatusCode = 204; // No Content
            }
        }

        [RavenAction("/admin/databases", "DELETE", "/admin/databases?name={databaseName:string|multiple}&hard-delete={isHardDelete:bool|optional(false)}")]
        public async Task DeleteBatch()
        {
            var names = HttpContext.Request.Query["name"];
            if (names.Count == 0)
                throw new ArgumentException("Query string \'name\' is mandatory, but wasn\'t specified");
            var isHardDelete = GetBoolValueQueryString("isHardDelete", DefaultValue<bool>.Default);

            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.OpenWriteTransaction();

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

                    await DeleteDatabase(name, context, isHardDelete, configuration);
                    writer.WriteString(context.GetLazyString($"Database {name} was deleted successfully"));
                }
                writer.WriteEndArray();
            }
        }

        private async Task DeleteDatabase(string name, TransactionOperationContext context, bool isHardDelete, RavenConfiguration configuration)
        {
            // ModifyResource should be called before deleteing the DatabaseDocument from the server store,
            // since ModifyResource will fail if the database is loading or modified right now
            await ServerStore.DatabasesLandlord.ModifyResource(name);

            var dbId = Constants.Database.Prefix + name;
            ServerStore.Delete(context, dbId);

            if (isHardDelete)
                DatabaseHelper.DeleteDatabaseFiles(configuration);
        }
    }
}