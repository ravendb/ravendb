// -----------------------------------------------------------------------
//  <copyright file="RequestRouter.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNet.Builder;
using Microsoft.AspNet.Http;
using Raven.Server.ServerWide;
using Raven.Server.Web.System;

namespace Raven.Server.Web
{
    public class RequestRouter
    {
        private readonly ServerStore _serverStore;
        private Dictionary<string, Func<HttpContext, RequestHandler>> _routes = new Dictionary<string, Func<HttpContext, RequestHandler>>(StringComparer.OrdinalIgnoreCase);


        public RequestRouter(ServerStore serverStore)
        {
            _serverStore = serverStore;
            _routes["admin/databases"] = context => new AdminDatabases(serverStore);
            //_routes["databases/?/docs"] = context => new Documents(SelectDatabase(context));
        }
    }

    public abstract class RequestHandler
    {
        public virtual Task Get(HttpContext ctx)
        {
            return Task.CompletedTask;
        }

        public virtual Task Put(HttpContext ctx)
        {
            return Task.CompletedTask;
        }
    }
}