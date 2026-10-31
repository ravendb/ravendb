using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Web;
using Raven.Server.Web.System;
using Sparrow.Platform;

namespace Raven.Server.Routing
{
    /// <summary>
    /// Registers the routes of the server from the route table generated at compile time
    /// </summary>
    public sealed class RouteScanner
    {
        public static readonly List<RouteInformation> DebugRoutes = Scan(attr =>
            {
                var isDebugEndpoint = attr.IsDebugInformationEndpoint && attr.Path.Contains("info-package") == false;

                if (isDebugEndpoint && attr.IsPosixSpecificEndpoint && PlatformDetails.RunningOnPosix == false)
                    return false;

                return isDebugEndpoint;
        })
            .Values
            .Where(x => x.Method != "OPTIONS")
            .ToList();

        public static readonly Dictionary<string, RouteInformation> AllRoutes = Scan();

        public static Dictionary<string, RouteInformation> Scan(Func<RavenActionAttribute, bool> predicate = null)
        {
            var routes = new Dictionary<string, RouteInformation>(StringComparer.OrdinalIgnoreCase);

            foreach (var action in RouteTable.Actions)
            {
                var route = action.Attribute;
                if (predicate != null && predicate(route) == false)
                    continue;

                if (route.CorsMode != CorsMode.None)
                {
                    // register endpoint for preflight request
                    var optionsRouteKey = "OPTIONS" + route.Path;

                    // we don't check for duplicates here, as single endpoint like: /admin/cluster/node might have 2 verbs (PUT, DELETE),
                    // but we need single OPTIONS handler

                    if (routes.TryGetValue(optionsRouteKey, out RouteInformation optionsRouteInfo) == false)
                    {
                        routes[optionsRouteKey] = optionsRouteInfo = new RouteInformation(
                            "OPTIONS",
                            route.Path,
                            route.RequiredAuthorization,
                            route.EndpointType,
                            route.SkipUsagesCount,
                            route.SkipLastRequestTimeUpdate,
                            route.CorsMode,
                            route.IsDebugInformationEndpoint,
                            route.DisableOnCpuCreditsExhaustion,
                            route.CheckForChanges);

                        optionsRouteInfo.Build(HandlePreflightRequest, isDatabaseHandler: false);
                    }
                }

                var routeKey = route.Method + route.Path;
                if (routes.TryGetValue(routeKey, out RouteInformation routeInfo) == false)
                {
                    routes[routeKey] = routeInfo = new RouteInformation(
                        route.Method,
                        route.Path,
                        route.RequiredAuthorization,
                        route.EndpointType,
                        route.SkipUsagesCount,
                        route.SkipLastRequestTimeUpdate,
                        route.CorsMode,
                        route.IsDebugInformationEndpoint,
                        route.DisableOnCpuCreditsExhaustion,
                        route.CheckForChanges);
                }
                else
                {
                    throw new InvalidOperationException($"A duplicate route found: {routeKey} on {action.Name}");
                }
                routeInfo.Build(action.Handler, action.IsDatabaseHandler);
            }

            if (predicate == null)
            {
                foreach (var shardedAction in RouteTable.ShardedActions)
                {
                    var route = shardedAction.Attribute;
                    var routeKey = route.Method + route.Path;
                    if (routes.TryGetValue(routeKey, out RouteInformation routeInfo) == false)
                    {
                        throw new InvalidOperationException(
                            $"Sharded action {shardedAction.Name} was specified, but there is no matching normal action");
                    }

                    routeInfo.BuildSharded(shardedAction.Handler);
                }
            }
            return routes;
        }

        private static Task HandlePreflightRequest(RequestHandlerContext context)
        {
            var handler = new CorsPreflightHandler();
            handler.Init(context);
            return handler.HandlePreflightRequest();
        }
    }
}