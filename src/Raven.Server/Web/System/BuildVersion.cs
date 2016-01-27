// -----------------------------------------------------------------------
//  <copyright file="BuildVersion.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Microsoft.AspNet.Http;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Raven.Server.Routing;
using Raven.Server.ServerWide;

namespace Raven.Server.Web.System
{
    public unsafe class BuildVersion : RequestHandler
    {
        private readonly RequestHandlerContext _requestHandlerContext;

        public BuildVersion(RequestHandlerContext requestHandlerContext)
        {
            _requestHandlerContext = requestHandlerContext;
        }

        [Route("/build/version", "GET")]
        public Task Get()
        {
            RavenOperationContext context;
            using (_requestHandlerContext.ServerStore.AllocateRequestContext(out context))
            {
                var result = new DynamicJsonValue
                {
                    ["BuildVersion"] = ServerVersion.Build,
                    ["ProductVersion"] = ServerVersion.Version,
                    ["CommitHash"] = ServerVersion.CommitHash
                };
                using (var doc = context.ReadObject(result, "build/version"))
                {
                    var response = _requestHandlerContext.HttpContext.Response;
                    response.StatusCode = 200;
                    doc.WriteTo(response.Body);
                    return Task.CompletedTask;
                }
            }
        }
    }


}