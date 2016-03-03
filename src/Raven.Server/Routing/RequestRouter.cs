// -----------------------------------------------------------------------
//  <copyright file="RequestRouter.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Metrics.Utils;
using Microsoft.AspNet.Http;
using NetTopologySuite.Noding;
using Raven.Database.Util;
using Raven.Server.Web;

namespace Raven.Server.Routing
{
    public class RequestRouter
    {
        private readonly Trie<RouteInformation> _trie;
        private readonly RavenServer _ravenServer;

        public RequestRouter(Dictionary<string, RouteInformation> routes, RavenServer ravenServer)
        {
            _trie = Trie<RouteInformation>.Build(routes);
            _ravenServer = ravenServer;
        }

        public RouteInformation GetRoute(string method, string path, out RouteMatch match)
        {
            var tryMatch = _trie.TryMatch(method, path);
            match = tryMatch.Match;
            return tryMatch.Value;
        }

        public async Task HandlePath(HttpContext context, string method, string path)
        {
            var tryMatch = _trie.TryMatch(method, path);
            if (tryMatch.Value == null)
            {
                context.Response.StatusCode = 400;
                await context.Response.WriteAsync($"There is no handler for path: {method} {path}{context.Request.QueryString}");
                return;
            }

            var reqCtx = new RequestHandlerContext
            {
                HttpContext = context,
                RavenServer = _ravenServer,
                RouteMatch = tryMatch.Match,
            };

            var handler = await tryMatch.Value.CreateHandler(reqCtx);
            var metricsCountersManager = reqCtx.Database?.Metrics ?? reqCtx.RavenServer.Metrics;
            metricsCountersManager.RequestsMeter.Mark();
            metricsCountersManager.RequestsPerSecondCounter.Mark();
            Interlocked.Increment(ref metricsCountersManager.ConcurrentRequestsCount);
            var sp = Environment.TickCount;
            if (handler == null)
            {
                context.Response.StatusCode = 400;
                await context.Response.WriteAsync("There is no handler for {context.Request.Method} {context.Request.Path}");
                return;
            }
            await handler(reqCtx);
            metricsCountersManager.RequestDuationMetric.Update((Environment.TickCount - sp)/1000);
            Interlocked.Decrement(ref metricsCountersManager.ConcurrentRequestsCount);

        }
    }
}