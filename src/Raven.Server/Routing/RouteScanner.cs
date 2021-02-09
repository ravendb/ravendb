// -----------------------------------------------------------------------
//  <copyright file="RouteScanner.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Microsoft.AspNetCore.Http;
using Raven.Server.ServerWide;
using Raven.Server.Web.System;
using Sparrow.Platform;

namespace Raven.Server.Routing
{
    /// <summary>
    /// Scans all the classes in this assembly for potential routes
    /// and register them
    /// </summary>
    public class RouteScanner
    {
        public readonly static List<RouteInformation> DebugRoutes = Scan(attr =>
            {
                var isDebugEndpoint = attr.IsDebugInformationEndpoint && attr.Path.Contains("info-package") == false;

                if (isDebugEndpoint && attr.IsPosixSpecificEndpoint && PlatformDetails.RunningOnPosix == false)
                    return false;

                return isDebugEndpoint;
            }).Values.ToList();


        public readonly static Dictionary<string, RouteInformation> AllRoutes = Scan();


        public static Dictionary<string, RouteInformation> Scan(Func<RavenActionAttribute, bool> predicate = null)
        {
            var routes = new Dictionary<string, RouteInformation>(StringComparer.OrdinalIgnoreCase);

            var corsHandler = typeof(CorsPreflightHandler).GetMethod(nameof(CorsPreflightHandler.HandlePreflightRequest));
            
            var actions = typeof(RouteScanner).Assembly.GetTypes()
                .SelectMany(type => type.GetMethods(BindingFlags.Instance | BindingFlags.Public | BindingFlags.DeclaredOnly))
                .Where(type => type.IsDefined(typeof(RavenActionAttribute)))
                .ToList();

            foreach (var memberInfo in actions)
            {
                foreach (var route in memberInfo.GetCustomAttributes<RavenActionAttribute>())
                {
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
                                route.SkipUsagesCount,
                                route.SkipLastRequestTimeUpdate,
                                route.CorsMode,
                                route.IsDebugInformationEndpoint,
                                route.DisableOnCpuCreditsExhaustion);
                            
                            optionsRouteInfo.Build(corsHandler);
                        }
                    }
                    
                    var routeKey = route.Method + route.Path;
                    if (routes.TryGetValue(routeKey, out RouteInformation routeInfo) == false)
                    {
                        routes[routeKey] = routeInfo = new RouteInformation(
                            route.Method, 
                            route.Path, 
                            route.RequiredAuthorization, 
                            route.SkipUsagesCount,
                            route.SkipLastRequestTimeUpdate,
                            route.CorsMode,
                            route.IsDebugInformationEndpoint,
                            route.DisableOnCpuCreditsExhaustion);
                    }
                    else
                    {
                        throw new InvalidOperationException($"A duplicate route found: {routeKey} on {memberInfo.DeclaringType}.{memberInfo.Name}");
                    }
                    routeInfo.Build(memberInfo);
                }
            }
            
            if (predicate == null)
            {
                var shardedActions = typeof(RouteScanner).GetTypeInfo().Assembly.GetTypes()
                    .SelectMany(type => type.GetMethods(BindingFlags.Instance | BindingFlags.Public | BindingFlags.DeclaredOnly))
                    .Where(type => type.IsDefined(typeof(RavenShardedActionAttribute)))
                    .ToList();

                foreach (var shardedAction in shardedActions)
                {
                    foreach (var route in shardedAction.GetCustomAttributes<RavenShardedActionAttribute>())
                    {

                        var routeKey = route.Method + route.Path;
                        if (routes.TryGetValue(routeKey, out RouteInformation routeInfo) == false)
                        {
                            throw new InvalidOperationException(
                                $"Sharded action {shardedAction.Name} on {shardedAction.DeclaringType} was specified, but there is no matching normal action");
                        }

                        routeInfo.BuildSharded(shardedAction);
                    }
                }
            }
            return routes;
        }
        
    }
    
}
