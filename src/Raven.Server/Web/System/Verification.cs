// -----------------------------------------------------------------------
//  <copyright file="Verification.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using Microsoft.AspNet.Http;
using Raven.Server.Json;
using Raven.Server.Routing;

namespace Raven.Server.Web.System
{
    public class Verification : RequestHandler
    {
        private readonly RequestHandlerContext _requestHandlerContext;

        public Verification(RequestHandlerContext requestHandlerContext)
        {
            _requestHandlerContext = requestHandlerContext;
        }

        [Route("/loaderio-9efdb3fbfd7839963e77e4443564c2f4.txt", "GET")]
        public Task IndexesGet()
        {
            RavenOperationContext context;
            using (_requestHandlerContext.ServerStore.AllocateRequestContext(out context))
            {
                return _requestHandlerContext.HttpContext.Response.WriteAsync("loaderio-9efdb3fbfd7839963e77e4443564c2f4");
            }
        }
    }
}