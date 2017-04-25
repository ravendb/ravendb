// -----------------------------------------------------------------------
//  <copyright file="RequestRouter.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;

using Raven.Server.Config;
using Raven.Server.Config.Attributes;
using Raven.Server.Documents;
using System.Threading;
using Microsoft.Extensions.Primitives;
using Raven.Client.Server.Operations.ApiKeys;
using Raven.Server.Utils;
using Raven.Server.Web;
using Raven.Server.Web.Authentication;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Routing
{
    public class RequestRouter
    {
        private readonly Trie<RouteInformation> _trie;
        private readonly RavenServer _ravenServer;
        private readonly MetricsCountersManager _serverMetrics;

        public RequestRouter(Dictionary<string, RouteInformation> routes, RavenServer ravenServer)
        {
            _trie = Trie<RouteInformation>.Build(routes);
            _ravenServer = ravenServer;
            _serverMetrics = ravenServer.Metrics;

        }

        public RouteInformation GetRoute(string method, string path, out RouteMatch match)
        {
            var tryMatch = _trie.TryMatch(method, path);
            match = tryMatch.Match;
            return tryMatch.Value;
        }

        public async Task<string> HandlePath(HttpContext context, string method, string path)
        {
            var tryMatch = _trie.TryMatch(method, path);
            if (tryMatch.Value == null)
            {
                context.Response.StatusCode = (int)HttpStatusCode.BadRequest;

                using(var ctx = JsonOperationContext.ShortTermSingleUse())
                using (var writer = new BlittableJsonTextWriter(ctx, context.Response.Body))
                {
                    ctx.Write(writer,
                        new DynamicJsonValue
                        {
                            ["Type"] = "Error",
                            ["Message"] = $"There is no handler for path: {method} {path}{context.Request.QueryString}"
                        });
                }
                return null;
            }

            var reqCtx = new RequestHandlerContext
            {
                HttpContext = context,
                RavenServer = _ravenServer,
                RouteMatch = tryMatch.Match,
            };

            var tuple = tryMatch.Value.TryGetHandler(reqCtx);
            var handler = tuple.Item1 ?? await tuple.Item2;

            reqCtx.Database?.Metrics?.RequestsMeter.Mark();
            _serverMetrics.RequestsMeter.Mark();

            Interlocked.Increment(ref _serverMetrics.ConcurrentRequestsCount);
            if (handler == null)
            {
                context.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                using (var ctx = JsonOperationContext.ShortTermSingleUse())
                using (var writer = new BlittableJsonTextWriter(ctx, context.Response.Body))
                {
                    ctx.Write(writer,
                        new DynamicJsonValue
                        {
                            ["Type"] = "Error",
                            ["Message"] = $"There is no handler for {context.Request.Method} {context.Request.Path}"
                        });
                }
                return null;
            }

            if (tryMatch.Value.NoAuthorizationRequired == false)
            {
                var authResult = TryAuthorize(context, _ravenServer.Configuration, reqCtx.Database);
                if (authResult == false)
                    return reqCtx.Database?.Name;
            }

            if (reqCtx.Database != null)
            {
                using (reqCtx.Database.DatabaseInUse(tryMatch.Value.SkipUsagesCount))
                    await handler(reqCtx);
            }
            else
            {
                await handler(reqCtx);
            }

            Interlocked.Decrement(ref _serverMetrics.ConcurrentRequestsCount);

            return reqCtx.Database?.Name;
        }

        private bool TryAuthorize(HttpContext context, RavenConfiguration configuration,
            DocumentDatabase database)
        {
            if (configuration.Server.AnonymousUserAccessMode == AnonymousUserAccessModeValues.Admin)
                return true;

            var authHeaderValues = context.Request.Headers["Raven-Authorization"];
            var token = authHeaderValues.Count == 0 ? null : authHeaderValues[0];

            if (token == null)
            {
                token = context.Request.Cookies["Raven-Authorization"];
            }

            if (token == null)
            {
                context.Response.StatusCode = (int)HttpStatusCode.PreconditionFailed;
                using (var ctx = JsonOperationContext.ShortTermSingleUse())
                using (var writer = new BlittableJsonTextWriter(ctx, context.Response.Body))
                {
                    DrainRequest(ctx, context);

                    ctx.Write(writer,
                        new DynamicJsonValue
                        {
                            ["Type"] = "Error",
                            ["Message"] = "The access token is required"
                        });
                }
                return false;
            }

            AccessToken accessToken;
            if (_ravenServer.AccessTokensById.TryGetValue(token, out accessToken) == false)
            {
                context.Response.StatusCode = (int)HttpStatusCode.PreconditionFailed;
                using (var ctx = JsonOperationContext.ShortTermSingleUse())
                using (var writer = new BlittableJsonTextWriter(ctx, context.Response.Body))
                {
                    DrainRequest(ctx, context);

                    ctx.Write(writer,
                        new DynamicJsonValue
                        {
                            ["Type"] = "Error",
                            ["Message"] = "The access token is invalid"
                        });
                }
                return false;
            }

            if (accessToken.IsExpired)
            {
                context.Response.StatusCode = (int)HttpStatusCode.PreconditionFailed;
                using (var ctx = JsonOperationContext.ShortTermSingleUse())
                using (var writer = new BlittableJsonTextWriter(ctx, context.Response.Body))
                {
                    DrainRequest(ctx, context);

                    ctx.Write(writer,
                        new DynamicJsonValue
                        {
                            ["Type"] = "Error",
                            ["Message"] = "The access token is expired"
                        });
                }
                return false;
            }

            var resourceName = database?.Name;

            if (resourceName == null)
                return true;

            AccessModes mode;
            var hasValue =
                accessToken.AuthorizedDatabases.TryGetValue(resourceName, out mode) ||
                accessToken.AuthorizedDatabases.TryGetValue("*", out mode);

            if (hasValue == false)
                mode = AccessModes.None;

            switch (mode)
            {
                case AccessModes.None:
                    context.Response.StatusCode = (int)HttpStatusCode.Forbidden;
                    using (var ctx = JsonOperationContext.ShortTermSingleUse())
                    using (var writer = new BlittableJsonTextWriter(ctx, context.Response.Body))
                    {
                        DrainRequest(ctx, context);

                        ctx.Write(writer,
                            new DynamicJsonValue
                            {
                                ["Type"] = "Error",
                                ["Message"] = $"Api Key {accessToken.Name} does not have access to {resourceName}"
                            });
                    }
                    return false;
                case AccessModes.ReadOnly:
                    if (context.Request.Method != "GET")
                    {
                        context.Response.StatusCode = (int)HttpStatusCode.Forbidden;
                        using (var ctx = JsonOperationContext.ShortTermSingleUse())
                        using (var writer = new BlittableJsonTextWriter(ctx, context.Response.Body))
                        {
                            DrainRequest(ctx, context);

                            ctx.Write(writer,
                                new DynamicJsonValue
                                {
                                    ["Type"] = "Error",
                                    ["Message"] = $"Api Key {accessToken.Name} does not have write access to {resourceName} but made a {context.Request.Method} request"
                                });
                        }
                        return false;
                    }
                    return true;
                case AccessModes.ReadWrite:
                case AccessModes.Admin:
                    return true;
                default:
                    throw new ArgumentOutOfRangeException("Unknown access mode: " + mode);
            }
        }

        private void DrainRequest(JsonOperationContext ctx, HttpContext context)
        {
            StringValues value;
            if (context.Response.Headers.TryGetValue("Connection", out value) && value == "close")
                return; // don't need to drain it, the connection will close 

            JsonOperationContext.ManagedPinnedBuffer buffer;
            using (ctx.GetManagedBuffer(out buffer))
            {
                var requestBody = context.Request.Body;
                while (true)
                {
                    var read = requestBody.Read(buffer.Buffer.Array, buffer.Buffer.Offset, buffer.Buffer.Count);
                    if (read == 0)
                        break;
                }
            }
        }
    }
}