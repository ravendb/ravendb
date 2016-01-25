// -----------------------------------------------------------------------
//  <copyright file="AdminDatabases.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using Microsoft.AspNet.Http;
using Raven.Server.Json;
using Raven.Server.Routing;

namespace Raven.Server.Web.System
{
    public class AdminDatabases : RequestHandler
    {
        private readonly CurrentRequestContext _requestContext;

        public AdminDatabases(CurrentRequestContext requestContext)
        {
            _requestContext = requestContext;
        }

        [Route("/admin/databases/$", "GET")]
        public Task Get(HttpContext ctx)
        {
            RavenOperationContext context;
            using (_requestContext.ServerStore.AllocateRequestContext(out context))
            {
                var id = _requestContext.RouteMatch.Url.Substring(_requestContext.RouteMatch.MatchLength);
                var dbId = "db/" + id;
                var obj = _requestContext.ServerStore.Read(context, dbId);
                if (obj == null)
                {
                    ctx.Response.StatusCode = 404;
                    return Task.CompletedTask;
                }
                ctx.Response.StatusCode = 200;
                obj.WriteTo(ctx.Response.Body);
                return Task.CompletedTask;
            }
        }

        [Route("/admin/databases/$", "PUT")]
        public Task Put(HttpContext ctx)
        {
            RavenOperationContext context;
            using (_requestContext.ServerStore.AllocateRequestContext(out context))
            {
                var id = _requestContext.RouteMatch.Url.Substring(_requestContext.RouteMatch.MatchLength);
                var dbId = "db/" + id;

                var writer = context.Read(ctx.Request.Body,  dbId);

                _requestContext.ServerStore.Write(dbId, writer);

                ctx.Response.StatusCode = 201;

                return Task.CompletedTask;
            }
        }
    }
}