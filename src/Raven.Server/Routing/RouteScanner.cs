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
        public Dictionary<string, RouteInformation> Scan()
        {
            var routes = new Dictionary<string, RouteInformation>(StringComparer.OrdinalIgnoreCase);

            var actions = GetType().GetTypeInfo().Assembly.GetTypes()
                .SelectMany(type => type.GetMembers())
                .Where(type => type.IsDefined(typeof (RouteAttribute)))
                .ToList();

            foreach (var memberInfo in actions)
            {
                var route = memberInfo.GetCustomAttributes<RouteAttribute>().Single();

                RouteInformation routeInfo;
                if (routes.TryGetValue(route.Path, out routeInfo) == false)
                {
                    routes[route.Path] = routeInfo = new RouteInformation(route.Path);
                }
                routeInfo.Build(memberInfo, route.Method);
            }

            return routes;
        }
    }
}