// -----------------------------------------------------------------------
//  <copyright file="RequestRouter.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
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
            var sp = Environment.TickCount;
            if (handler == null)
            {
                context.Response.StatusCode = 400;
                await context.Response.WriteAsync("There is no handler for {context.Request.Method} {context.Request.Path}");
                return;
            }

            string tryAuthMsg = null;
            int authResponseCode;
            if (NeverSecret.IsNeverSecretUrl(path) == false && 
                TryAuthorize(context, _ravenServer.Configuration, reqCtx.Database, IgnoreDb.Urls.Contains(path),
                out authResponseCode, out tryAuthMsg) == false)
            {
                context.Response.StatusCode = authResponseCode;
                await context.Response.WriteAsync($"{tryAuthMsg} for path: {method} {path}{context.Request.QueryString}");
                return;
            }

            await handler(reqCtx);
            Interlocked.Decrement(ref metricsCountersManager.ConcurrentRequestsCount);

        }

        private bool TryAuthorize(HttpContext context, RavenConfiguration configuration, DocumentDatabase database, bool ignoreDbAccess,
                                  out int responseCode, out string msg)
        {
            var authHeaderValues = context.Request.Headers["Authorization"];
            var authHeader = authHeaderValues.FirstOrDefault();

            var hasApiKeyHeaderValues = context.Request.Headers["Has-Api-Key"]; // TODO (OAuth): is it redundent ?
            var apiKeyHeader = hasApiKeyHeaderValues.FirstOrDefault();

            
            
            var hasApiKey = false;
            if (apiKeyHeader != null)
                hasApiKey = "True".Equals(apiKeyHeader, StringComparison.CurrentCultureIgnoreCase);

            var oAuthTokenInCookieValues = context.Request.Cookies["OAuth-Token"];
            var oAuthTokenInCookie = oAuthTokenInCookieValues.FirstOrDefault();

            if (hasApiKey || oAuthTokenInCookie != null ||
                string.IsNullOrEmpty(authHeader) == false && authHeader.StartsWith("Bearer "))
            {
                var token = GetToken(authHeader, oAuthTokenInCookie);
                var allowUnauthenticatedUsers = // we need to auth even if we don't have to, for bundles that want the user 
                    configuration.Server.AnonymousUserAccessMode == AnonymousUserAccessModeValues.All ||
                    configuration.Server.AnonymousUserAccessMode == AnonymousUserAccessModeValues.Admin ||
                    configuration.Server.AnonymousUserAccessMode == AnonymousUserAccessModeValues.Get && context.Request.Method.Equals("GET");

                responseCode = 0;
                msg = "";

                if (token == null)
                {
                    if (allowUnauthenticatedUsers)
                        return true;

                    msg = "The access token is required";
                    responseCode = hasApiKey ? 412 : 401;
                    return false;
                }

                AccessTokenBody tokenBody;
                var oauthParamaters = OAuthServerHelper.GetOAuthParameters(configuration);
                if (AccessToken.TryParseBody(oauthParamaters, token, out tokenBody) == false)
                {
                    if (allowUnauthenticatedUsers)
                        return true;

                    msg = "The access token is invalid";
                    responseCode = 401;
                    return false;
                }

                if (tokenBody.IsExpired()) // TODO (OAuth): IMHO this is way too expansive for each request (store utc now each minute, and check against stored value)
                {
                    if (allowUnauthenticatedUsers)
                        return true;

                    msg = "The access token is expired";
                    responseCode = 401;
                    return false;
                }


                var resourceName = database?.ResourceName;

                var writeAccess = context.Request.Method.Equals("GET");
                if (!tokenBody.IsAuthorized(resourceName, writeAccess))
                {
                    if (allowUnauthenticatedUsers || ignoreDbAccess)
                        return true;

                    msg = writeAccess ?
                        "Not authorized for read/write access for tenant " + resourceName :
                        "Not authorized for tenant " + resourceName;
                    responseCode = 403;
                    return false;
                }

                // TODO (OAuth): What to do with these:
                //controller.User = new OAuthPrincipal(tokenBody, controller.ResourceName);
                //CurrentOperationContext.User.Value = controller.User;

                return true;
            }

            msg = "The access token is required";
            responseCode = hasApiKey ? 412 : 401;
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