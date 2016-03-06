//// -----------------------------------------------------------------------
////  <copyright file="AdminDatabases.cs" company="Hibernating Rhinos LTD">
////      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
////  </copyright>
//// -----------------------------------------------------------------------
//
//using System;
//using System.Threading.Tasks;
//using Microsoft.AspNet.Http;
//using Raven.Abstractions.Data;
//using Raven.Server.Json;
//using Raven.Server.Routing;
//using Raven.Server.ServerWide;
//using Raven.Server.ServerWide.Context;
//
//namespace Raven.Server.Web.System
//{
//    public class AdminDatabases : RequestHandler
//    {
//        [RavenAction("/admin/databases/$", "GET")]
//        public Task Get()
//        {
//            TransactionOperationContext context;
//            using (ServerStore.ContextPool.AllocateOperationContext(out context))
//            {
//                context.OpenReadTransaction();
//
//                var id = RouteMatch.Url.Substring(RouteMatch.MatchLength);
//                if (string.IsNullOrWhiteSpace(id))
//                    throw new InvalidOperationException("Database id was not provided");
//                var dbId = Constants.Database.Prefix + id;
//                var dbDoc = ServerStore.Read(context, dbId);
//                if (dbDoc == null)
//                {
//                    HttpContext.Response.StatusCode = 404;
//                    return HttpContext.Response.WriteAsync("Database " + id + " wasn't found");
//                }
//
//                UnprotectSecuredSettingsOfDatabaseDocument(dbDoc);
//
//                HttpContext.Response.StatusCode = 200;
//                // TODO: Implement etags
//
//                context.Write(ResponseBodyStream(), dbDoc);
//                return Task.CompletedTask;
//            }
//        }
//
//        private void UnprotectSecuredSettingsOfDatabaseDocument(BlittableJsonReaderObject obj)
//        {
//            object securedSettings;
//            if (obj.TryGetMember("SecuredSettings", out securedSettings) == false)
//            {
//
//            }
//        }
//
//        [RavenAction("/admin/databases/$", "PUT")]
//        public async Task Put()
//        {
//            var id = RouteMatch.Url.Substring(RouteMatch.MatchLength);
//
//            string errorMessage;
//            if (ResourceNameValidator.IsValidResourceName(id, ServerStore.Configuration.Core.DataDirectory, out errorMessage) == false)
//            {
//                HttpContext.Response.StatusCode = 400;
//                await HttpContext.Response.WriteAsync(errorMessage);
//                return;
//            }
//
//            TransactionOperationContext context;
//            using (ServerStore.ContextPool.AllocateOperationContext(out context))
//            {
//                context.OpenWriteTransaction();
//                var dbId = Constants.Database.Prefix + id;
//
//                var etag = HttpContext.Request.Headers["ETag"];
//                if (CheckExistingDatabaseName(context, id, dbId, etag, out errorMessage) == false)
//                {
//                    HttpContext.Response.StatusCode = 400;
//                    await HttpContext.Response.WriteAsync(errorMessage);
//                    return;
//                }
//
//                var dbDoc = await context.ReadForDiskAsync(RequestBodyStream(), dbId);
//
//                //TODO: Fix this
//                //int size;
//                //var buffer = context.GetNativeTempBuffer(dbDoc.SizeInBytes, out size);
//                //dbDoc.CopyTo(buffer);
//
//                //var reader = new BlittableJsonReaderObject(buffer, dbDoc.SizeInBytes, context);
//                //object result;
//                //if (reader.TryGetMember("SecureSettings", out result))
//                //{
//                //    var secureSettings = (BlittableJsonReaderObject) result;
//                //    secureSettings.Modifications = new DynamicJsonValue(secureSettings);
//                //    foreach (var propertyName in secureSettings.GetPropertyNames())
//                //    {
//                //        secureSettings.TryGetMember(propertyName, out result);
//                //        // protect
//                //        secureSettings.Modifications[propertyName] = "fooo";
//                //    }
//                //}
//
//
//                ServerStore.Write(context, dbId, dbDoc);
//
//                context.Transaction.Commit();
//
//                HttpContext.Response.StatusCode = 201;
//
//            }
//        }
//
//        private bool CheckExistingDatabaseName(TransactionOperationContext context, string id, string dbId, string etag, out string errorMessage)
//        {
//            var database = ServerStore.Read(context, dbId);
//            var isExistingDatabase = database != null;
//
//            if (isExistingDatabase && etag == null)
//            {
//                errorMessage = $"Database with the name '{id}' already exists";
//                return false;
//            }
//            if (!isExistingDatabase && etag != null)
//            {
//                errorMessage = $"Database with the name '{id}' doesn't exist";
//                return false;
//            }
//
//            errorMessage = null;
//            return true;
//        }
//    }
//}