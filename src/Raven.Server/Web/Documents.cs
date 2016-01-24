// -----------------------------------------------------------------------
//  <copyright file="AdminDatabases.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Microsoft.AspNet.Http;
using Raven.Server.Routing;
using Raven.Server.ServerWide;

namespace Raven.Server.Web
{
    public class Documents : RequestHandler
    {
        private readonly ServerStore _serverStore;

        public Documents(ServerStore serverStore)
        {
            _serverStore = serverStore;
        }

        [Route("/databases", "GET")]
        public Task Get(HttpContext ctx)
        {
            throw new NotImplementedException();
        }

        [Route("/databases", "PUT")]
        public Task Put(HttpContext ctx)
        {
            throw new NotImplementedException();
        }

        [Route("/databases", "DELETE")]
        public Task Delete(HttpContext ctx)
        {
            throw new NotImplementedException();
        }
    }
}