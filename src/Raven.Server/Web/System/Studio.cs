// -----------------------------------------------------------------------
//  <copyright file="Studio.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using Raven.Server.Routing;

namespace Raven.Server.Web.System
{
    public class Studio : RequestHandler
    {
        private readonly RequestHandlerContext _requestHandlerContext;

        public Studio(RequestHandlerContext requestHandlerContext)
        {
            _requestHandlerContext = requestHandlerContext;
        }

        [Route("/", "GET")]
        public Task RavenRoot()
        {
            _requestHandlerContext.HttpContext.Response.StatusCode = 302; // Found
            const string rootPath = "studio/index.html";
            _requestHandlerContext.HttpContext.Response.Headers["Location"] = rootPath;
            return Task.CompletedTask;
        }
    }
}