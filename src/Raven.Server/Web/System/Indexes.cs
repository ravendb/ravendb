// -----------------------------------------------------------------------
//  <copyright file="Indexes.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using Raven.Server.Json;
using Raven.Server.Routing;

namespace Raven.Server.Web.System
{
    public class Indexes : RequestHandler
    {
        private readonly RequestHandlerContext _requestHandlerContext;

        public Indexes(RequestHandlerContext requestHandlerContext)
        {
            _requestHandlerContext = requestHandlerContext;
        }

        [Route("/databases/*/indexes", "GET")]
        public Task IndexesGet()
        {
            RavenOperationContext context;
            using (_requestHandlerContext.ServerStore.AllocateRequestContext(out context))
            {
                return Task.CompletedTask;
            }
        }
    }
}