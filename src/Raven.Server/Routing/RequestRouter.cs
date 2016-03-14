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
using Raven.Server.Authentication;
using Raven.Server.Config;
using Raven.Server.Config.Attributes;
using Raven.Server.Documents;
using System.Threading;
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
            if (handler == null)
            {
                context.Response.StatusCode = 400;
                await context.Response.WriteAsync("There is no handler for {context.Request.Method} {context.Request.Path}");
                return;
            }

            var sp = Stopwatch.StartNew();
            if (!tryMatch.Value.SkipTryAuthorize &&
                 await TryAuthorize(context, _ravenServer.Configuration, reqCtx.Database, tryMatch.Value.IgnoreDbRoute) == false)
                return;
            Console.WriteLine(sp.ElapsedMilliseconds);
            

            await handler(reqCtx);

            Interlocked.Decrement(ref metricsCountersManager.ConcurrentRequestsCount);
        }

        private async Task<bool> TryAuthorize(HttpContext context, RavenConfiguration configuration, DocumentDatabase database, bool ignoreDb)
        {
            if (configuration.Server.AnonymousUserAccessMode == AnonymousUserAccessModeValues.Admin)
                return true;

            var authHeaderValues = context.Request.Headers["Authorization"];
            var authHeader = authHeaderValues.Count == 0 ? null : authHeaderValues[0];

            string oAuthTokenInCookie = null;
            if (authHeader == null)
            {
                var oAuthTokenInCookieValues = context.Request.Cookies["OAuth-Token"];
                oAuthTokenInCookie = oAuthTokenInCookieValues.Count == 0 ? null : oAuthTokenInCookieValues[0];
            }

            if (string.IsNullOrEmpty(authHeader) == false && authHeader.StartsWith("Bearer ") || 
                oAuthTokenInCookie != null)
            {
                var token = GetToken(authHeader, oAuthTokenInCookie);

                AccessTokenBody tokenBody;
                if (_ravenServer.AccessTokensById.TryGetValue(token, out tokenBody) == false)
                {
                    context.Response.StatusCode = 401;
                    await context.Response.WriteAsync("The access token is invalid");
                    return false;
                }

                if (tokenBody.IsExpired(token, _ravenServer.AccessTokensById))
                {
                    context.Response.StatusCode = 401;
                    await context.Response.WriteAsync("The access token is expired");
                    return false;
                }

                var resourceName = database?.ResourceName;

                var writeAccess = context.Request.Method.Equals("GET");
                if (!tokenBody.IsAuthorized(resourceName, writeAccess))
                {
                    // TODO : Add to these routes with "true" for skipAuthorize(neverSecret) RavenActionAttr: (some obsoulite)
                    //    "/raven/studio.html",
                    //    "/silverlight/Raven.Studio.xap",
                    //    "/favicon.ico",
                    //    "/clientaccesspolicy.xml",
                    //    "/OAuth/Cookie",

                    if (ignoreDb)
                        return true;

                    var msg = writeAccess ?
                        "Not authorized for read/write access for tenant " + resourceName :
                        "Not authorized for tenant " + resourceName;

                    context.Response.StatusCode = 403;
                    await context.Response.WriteAsync(msg);
                    return false;
                }

                // TODO (OAuth): Save user..
                //controller.User = new OAuthPrincipal(tokenBody, controller.ResourceName);
                //CurrentOperationContext.User.Value = controller.User;

                return true;
            }

            context.Response.StatusCode = 412;
            await context.Response.WriteAsync("The access token is required");
            return false;
        }

        static string GetToken(string authValue, string authCookie) // TODO (OAuth): better perform needed + segment instead of substring etc...
        {
            const string bearerPrefix = "Bearer ";

            var auth = authValue;

            if (auth == null)
            {
                auth = authCookie;
                if (auth != null)
                    auth = Uri.UnescapeDataString(auth);
            }
            if (auth == null || auth.Length <= bearerPrefix.Length ||
                !auth.StartsWith(bearerPrefix, StringComparison.OrdinalIgnoreCase))
                return null;

            var token = auth.Substring(bearerPrefix.Length, auth.Length - bearerPrefix.Length);

            return token;
        }
    }
}