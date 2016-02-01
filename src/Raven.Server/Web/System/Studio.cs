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
        [Route("/", "GET")]
        public Task RavenRoot()
        {
            HttpContext.Response.StatusCode = 302; // Found
            const string rootPath = "studio/index.html";
            HttpContext.Response.Headers["Location"] = rootPath;
            return Task.CompletedTask;
        }
    }
}