// -----------------------------------------------------------------------
//  <copyright file="AdminDatabases.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Microsoft.AspNet.Http;
using Raven.Server.Json;
using Raven.Server.Routing;

namespace Raven.Server.Web.System
{
    public class AdminDatabases : RequestHandler
    {
        private readonly RequestHandlerContext _requestHandlerContext;

        public AdminDatabases(RequestHandlerContext requestHandlerContext)
        {
            _requestHandlerContext = requestHandlerContext;
        }

        [Route("/admin/databases/$", "GET")]
        public Task Get()
        {
            RavenOperationContext context;
            using (_requestHandlerContext.ServerStore.AllocateRequestContext(out context))
            {
                var id = _requestHandlerContext.RouteMatch.Url.Substring(_requestHandlerContext.RouteMatch.MatchLength);
                var dbId = "db/" + id;
                var obj = _requestHandlerContext.ServerStore.Read(context, dbId);
                if (obj == null)
                {
                    _requestHandlerContext.HttpContext.Response.StatusCode = 404;
                    return _requestHandlerContext.HttpContext.Response.WriteAsync("Database " + id + " wasn't found");
                }

                UnprotectSecuredSettingsOfDatabaseDocument(obj);

                _requestHandlerContext.HttpContext.Response.StatusCode = 200;
                _requestHandlerContext.HttpContext.Response.Headers["ETag"] = "TODO: Please implement this: " + Guid.NewGuid(); // TODO (fitzchak)
                obj.WriteTo(_requestHandlerContext.HttpContext.Response.Body);
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
        public Task Put()
        {
            var id = _requestHandlerContext.RouteMatch.Url.Substring(_requestHandlerContext.RouteMatch.MatchLength);

            string errorMessage;
            if (ResourceNameValidator.IsValidResourceName(id, _requestHandlerContext.ServerStore.DataDirectory, out errorMessage) == false)
            {
                _requestHandlerContext.HttpContext.Response.StatusCode = 400;
                return _requestHandlerContext.HttpContext.Response.WriteAsync(errorMessage);
            }

            RavenOperationContext context;
            using (_requestHandlerContext.ServerStore.AllocateRequestContext(out context))
            {
                var dbId = "db/" + id;

                var writer = context.Read(_requestHandlerContext.HttpContext.Request.Body,  dbId);

                _requestHandlerContext.ServerStore.Write(dbId, writer);

                _requestHandlerContext.HttpContext.Response.StatusCode = 201;

                return Task.CompletedTask;
            }
        }
    }
}