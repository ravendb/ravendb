// -----------------------------------------------------------------------
//  <copyright file="RouteScanner.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Net.Http;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.AspNet.Builder;
using Microsoft.AspNet.Http;
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
        private readonly ServerStore _serverStore;
        private Trie<RouteInformation> trie;

        public RouteScanner(ServerStore serverStore)
        {
            _serverStore = serverStore;
        }

        public void Scan()
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
                    routes[route.Path] = routeInfo = new RouteInformation(route.Path, _serverStore);
                }
                routeInfo.Build(memberInfo, route.Method);
            }

            trie = Trie<RouteInformation>.Build(routes);
        }

        public Task HandlePath(HttpContext context)
        {
            var tryMatch = trie.TryMatch(context.Request.Path);
            if (tryMatch.Success == false)
                throw new NotImplementedException();

            var handler = tryMatch.Value.CreateHandler(context);
            return handler(context);
        }
    }

    public class RouteInformation
    {
        public readonly string Path;
        private readonly ServerStore _serverStore;

        public RequestDelegate Get;
        public RequestDelegate Put;

        public RouteInformation(string path, ServerStore serverStore)
        {
            Path = path;
            _serverStore = serverStore;
        }

        public void Build(MemberInfo memberInfo, string method)
        {

            //HttpContext ctx
            var ctx = Expression.Parameter(typeof(HttpContext), "ctx");
            //new Handler()
            var newExpression = Expression.New(memberInfo.DeclaringType.GetConstructors().Single());
            //.Init(serverStore);
            var initExpr = Expression.Call(newExpression, "Init", new[] { typeof(ServerStore) }, Expression.Constant(_serverStore));
            //.Handle(ctx);
            var handleExpr = Expression.Call(initExpr, "Handle", new[] { typeof(HttpContext) }, ctx);
            var requestDelegate = Expression.Lambda<RequestDelegate>(handleExpr).Compile();

            switch (method)
            {
                case "GET":
                    Get = requestDelegate;
                    break;
                case "PUT":
                    Put = requestDelegate;
                    break;
                default:
                    throw new NotSupportedException("There is no handler for " + method);
            }
        }

        public RequestDelegate CreateHandler(HttpContext context)
        {
            switch (context.Request.Method)
            {
                case "GET":
                    return Get;
                case "PUT":
                    return Put;
                default:
                    throw new NotSupportedException("There is no handler for " + context.Request.Method);
            }
        }
    }
}