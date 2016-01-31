// -----------------------------------------------------------------------
//  <copyright file="RequestRouter.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNet.Http;
using Microsoft.Extensions.DependencyInjection;
using Raven.Client.Document;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Raven.Server.Web;

namespace Raven.Server.Routing
{
    public class RequestRouter
    {
        private readonly Trie<RouteInformation> _trie;

        public RequestRouter(Dictionary<string, RouteInformation> routes)
        {
            _trie = Trie<RouteInformation>.Build(routes);
        }

        public Task HandlePath(HttpContext context)
        {
            var tryMatch = _trie.TryMatch(context.Request.Path);
            if (tryMatch.Match.Success == false)
            {
                context.Response.StatusCode = 400;
                return context.Response.WriteAsync("There is no handler for path: " + context.Request.Path);
            }

            var handler = tryMatch.Value.CreateHandler(context);
            if (handler == null)
            {
                context.Response.StatusCode = 400;
                return context.Response.WriteAsync("There is no handler for path: " + context.Request.Path + " with method: " + context.Request.Method);
            }

            var serverStore = context.ApplicationServices.GetRequiredService<ServerStore>();
            var reqCtx = new RequestHandlerContext
            {
                HttpContext = context,
                ServerStore = serverStore,
                DocumentStore = context.ApplicationServices.GetRequiredService<DocumentsStorage>(),
                OperationContextPool = serverStore.ContextPool,
                RouteMatch = tryMatch.Match,
            };
            return handler(reqCtx);
        }
    }
}