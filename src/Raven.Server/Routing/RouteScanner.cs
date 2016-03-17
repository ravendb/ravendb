// -----------------------------------------------------------------------
//  <copyright file="RouteScanner.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Raven.Server.ServerWide;
using Raven.Server.Web.System;

namespace Raven.Server.Routing
{
    /// <summary>
    /// Scans all the classes in this assembly for potential routes
    /// and register them
    /// </summary>
    public class RouteScanner
    {
        public static Dictionary<string, RouteInformation> Scan()
        {
            var routes = new Dictionary<string, RouteInformation>(StringComparer.OrdinalIgnoreCase);

            var actions = typeof(RouteScanner).GetTypeInfo().Assembly.GetTypes()
                .SelectMany(type => type.GetMethods(BindingFlags.Instance | BindingFlags.Public | BindingFlags.DeclaredOnly))
                .Where(type => type.IsDefined(typeof(RavenActionAttribute)))
                .ToList();

            foreach (var memberInfo in actions)
            {
                foreach (var route in memberInfo.GetCustomAttributes<RavenActionAttribute>())
                {
                    RouteInformation routeInfo;
                    var routeKey = route.Method + route.Path;
                    if (routes.TryGetValue(routeKey, out routeInfo) == false)
                    {
                        routes[routeKey] = routeInfo = new RouteInformation(route.Method, route.Path, route.NoAuthorizationRequired);
                    }
                    else
                    {
                        throw new InvalidOperationException($"A duplicate route found: {routeKey} on {memberInfo.DeclaringType}.{memberInfo.Name}");
                    }
                    routeInfo.Build(memberInfo);
                }
            }

            return routes;
        }
    }
}