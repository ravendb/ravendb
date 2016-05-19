// -----------------------------------------------------------------------
//  <copyright file="AdminDatabasesHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using Raven.Server.Routing;

namespace Raven.Server.Web.System
{
    public class EmptyMessageHandler : RequestHandler
    {
        [RavenAction("/admin/emptymessage", "GET")]
        public Task Get()
        {
           HttpContext.Response.StatusCode = 200;
           return Task.CompletedTask;
        }
    }
}