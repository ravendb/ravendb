// -----------------------------------------------------------------------
//  <copyright file="Documents.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Json;
using Raven.Server.Routing;

namespace Raven.Server.Web.System
{
    public class Documents : RequestHandler
    {
        private readonly RequestHandlerContext _requestHandlerContext;

        public Documents(RequestHandlerContext requestHandlerContext)
        {
            _requestHandlerContext = requestHandlerContext;
        }

        [Route("/databases/*/docs", "PUT")]
        public Task DocumentPut()
        {
            var databaseName = _requestHandlerContext.RouteMatch.Url.Substring(_requestHandlerContext.RouteMatch.CaptureStart, _requestHandlerContext.RouteMatch.CaptureLength);
            var docId = _requestHandlerContext.HttpContext.Request.Query["docId"];

            RavenOperationContext context;
            using (_requestHandlerContext.ServerStore.AllocateRequestContext(out context))
            {
                var json = context.Read(_requestHandlerContext.HttpContext.Request.Body, docId);

                _requestHandlerContext.HttpContext.Response.StatusCode = 201;
                return Task.CompletedTask;
            }
        }

        [Route("/databases/*/docs", "DELETE")]
        public Task DocumentDelete()
        {
            var databaseName = _requestHandlerContext.RouteMatch.Url.Substring(_requestHandlerContext.RouteMatch.CaptureStart, _requestHandlerContext.RouteMatch.CaptureLength);
            var docId = _requestHandlerContext.RouteMatch.Url.Substring(_requestHandlerContext.RouteMatch.MatchLength);

            RavenOperationContext context;
            using (_requestHandlerContext.ServerStore.AllocateRequestContext(out context))
            {
                _requestHandlerContext.HttpContext.Response.StatusCode = 204;
                return Task.CompletedTask;
            }
        }
    }
}