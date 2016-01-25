// -----------------------------------------------------------------------
//  <copyright file="AdminDatabases.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
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
                    return Task.CompletedTask;
                }
                _requestHandlerContext.HttpContext.Response.StatusCode = 200;
                obj.WriteTo(_requestHandlerContext.HttpContext.Response.Body);
                return Task.CompletedTask;
            }
        }

        [Route("/admin/databases/$", "PUT")]
        public Task Put()
        {
            RavenOperationContext context;
            using (_requestHandlerContext.ServerStore.AllocateRequestContext(out context))
            {
                var id = _requestHandlerContext.RouteMatch.Url.Substring(_requestHandlerContext.RouteMatch.MatchLength);
                var dbId = "db/" + id;

                var writer = context.Read(_requestHandlerContext.HttpContext.Request.Body,  dbId);

                _requestHandlerContext.ServerStore.Write(dbId, writer);

                _requestHandlerContext.HttpContext.Response.StatusCode = 201;

                return Task.CompletedTask;
            }
        }
    }
}