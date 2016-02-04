// -----------------------------------------------------------------------
//  <copyright file="AdminDatabases.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Microsoft.AspNet.Http;
using Raven.Abstractions.Data;
using Raven.Server.Json;
using Raven.Server.Routing;

namespace Raven.Server.Web.System
{
    public class AdminDatabases : RequestHandler
    {
        [Route("/admin/databases/$", "GET")]
        public Task Get()
        {
            RavenOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                context.Transaction = context.Environment.ReadTransaction();

                var id = RouteMatch.Url.Substring(RouteMatch.MatchLength);
                if (string.IsNullOrWhiteSpace(id))
                    throw new InvalidOperationException("Database id was not provided");
                var dbId = Constants.Database.Prefix + id;
                var dbDoc = ServerStore.Read(context, dbId);
                if (dbDoc == null)
                {
                    HttpContext.Response.StatusCode = 404;
                    return HttpContext.Response.WriteAsync("Database " + id + " wasn't found");
                }

                UnprotectSecuredSettingsOfDatabaseDocument(dbDoc);

                HttpContext.Response.StatusCode = 200;
                // TODO: Implement etags

                dbDoc.WriteTo(ResponseBodyStream());
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

        [Route("/admin/databases/$", "PUT")]
     
        public async Task Put()
        {
            var id = RouteMatch.Url.Substring(RouteMatch.MatchLength);

            string errorMessage;
            if (ResourceNameValidator.IsValidResourceName(id, ServerStore.Configuration.Core.DataDirectory, out errorMessage) == false)
            {
                HttpContext.Response.StatusCode = 400;
                await HttpContext.Response.WriteAsync(errorMessage);
                return;
            }

            RavenOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                context.Transaction = context.Environment.WriteTransaction();
                var dbId = Constants.Database.Prefix + id;

                var etag = HttpContext.Request.Headers["ETag"];
                if (CheckExistingDatabaseName(context, id, dbId, etag, out errorMessage) == false)
                {
                    HttpContext.Response.StatusCode = 400;
                    await HttpContext.Response.WriteAsync(errorMessage);
                    return;
                }

                var dbDoc = await context.Read(RequestBodyStream(), dbId);
                
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

                HttpContext.Response.StatusCode = 201;
                
            }
        }

        private bool CheckExistingDatabaseName(RavenOperationContext context, string id, string dbId, string etag, out string errorMessage)
        {
            var database = ServerStore.Read(context, dbId);
            var isExistingDatabase = database != null;

            if (isExistingDatabase && etag == null)
            {
                errorMessage = $"Database with the name '{id}' already exists";
                return false;
            }
            if (!isExistingDatabase && etag != null)
            {
                errorMessage = $"Database with the name '{id}' doesn't exist";
                return false;
            }

            errorMessage = null;
            return true;
        }
    }
}