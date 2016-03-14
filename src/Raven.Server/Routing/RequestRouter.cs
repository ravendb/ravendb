// -----------------------------------------------------------------------
//  <copyright file="RequestRouter.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNet.Http;
using Raven.Abstractions.Data;
using Raven.Server.Config;
using Raven.Server.Config.Attributes;
using Raven.Server.Documents;
using System.Threading;
using Raven.Client.Data;
using Raven.Server.Web;
using static System.String;

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
            if (handler == null)
            {
                context.Response.StatusCode = 400;
                await context.Response.WriteAsync("There is no handler for {context.Request.Method} {context.Request.Path}");
                return;
            }

            if (tryMatch.Value.NoAuthorizationRequired == false)
            {
                var authResult = await TryAuthorize(context, _ravenServer.Configuration, reqCtx.Database);
                if (authResult == false)
                    return;
            }

            await handler(reqCtx);

            Interlocked.Decrement(ref metricsCountersManager.ConcurrentRequestsCount);
        }

        private async Task<bool> TryAuthorize(HttpContext context, RavenConfiguration configuration,
            DocumentDatabase database)
        {
            if (configuration.Server.AnonymousUserAccessMode == AnonymousUserAccessModeValues.Admin)
                return true;

            var authHeaderValues = context.Request.Headers["Authorization"];
            var token = authHeaderValues.Count == 0 ? null : authHeaderValues[0];

            if (token == null)
            {
                var oAuthTokenInCookieValues = context.Request.Cookies["OAuth-Token"];
                token = oAuthTokenInCookieValues.Count == 0 ? null : oAuthTokenInCookieValues[0];
            }

            if (token == null)
            {
                context.Response.StatusCode = 412;
                await context.Response.WriteAsync("The access token is required");
                return false;
            }

            AccessToken accessToken;
            if (_ravenServer.AccessTokensById.TryGetValue(token, out accessToken) == false)
            {
                context.Response.StatusCode = 412;
                await context.Response.WriteAsync("The access token is invalid");
                return false;
            }

            if (accessToken.IsExpired)
            {
                context.Response.StatusCode = 412;
                await context.Response.WriteAsync("The access token is expired");
                return false;
            }

            var resourceName = database?.ResourceName;

            if (resourceName == null)
                return true;

            AccessToken.Mode mode;
            var hasValue = 
                accessToken.AuthorizedDatabases.TryGetValue(resourceName, out mode) ||
                accessToken.AuthorizedDatabases.TryGetValue("*", out mode);

            if (hasValue == false)
                mode = AccessToken.Mode.None;

            string text;
            switch (mode)
            {
                case AccessToken.Mode.None:
                    context.Response.StatusCode = 403;
                    text = $"Api Key {accessToken.Name} does not have access to {resourceName}";
                    await context.Response.WriteAsync(text);
                    return false;
                case AccessToken.Mode.ReadOnly:
                    if (context.Request.Method != "GET")
                    {
                        context.Response.StatusCode = 403;
                        text = $"Api Key {accessToken.Name} does not have write access to {resourceName} but made a {context.Request.Method} request";
                        await context.Response.WriteAsync(text);
                        return false;
                    }
                    return true;
                case AccessToken.Mode.ReadWrite:
                case AccessToken.Mode.Admin:
                    return true;
                default:
                    throw new ArgumentOutOfRangeException("Unknown access mode: " + mode);
            }
        }
    }
}