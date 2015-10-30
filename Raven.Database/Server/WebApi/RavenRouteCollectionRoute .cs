// -----------------------------------------------------------------------
//  <copyright file="RavenRouteCollectionRoute .cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Net.Http;
using System.Web.Http.Routing;

namespace Raven.Database.Server.WebApi
{
    internal class RavenRouteCollectionRoute : IHttpRoute, IReadOnlyCollection<IHttpRoute>
    {
        public const string SubRouteDataKey = "MS_SubRoutes";

        private static readonly IDictionary<string, object> Empty = new ReadOnlyDictionary<string, object>(new Dictionary<string, object>());

        private readonly IReadOnlyCollection<IHttpRoute> subRoutes;

        public RavenRouteCollectionRoute(IReadOnlyCollection<IHttpRoute> subRoutes)
        {
            this.subRoutes = subRoutes;
        }

        /// <summary>
        /// Determine whether this route is a match for the incoming request by looking up the &lt;see cref="!:IRouteData" /&gt; for the route. 
        /// </summary>
        /// <returns>
        /// The &lt;see cref="!:RouteData" /&gt; for a route if matches; otherwise null.
        /// </returns>
        /// <param name="virtualPathRoot">The virtual path root.</param><param name="request">The request.</param>
        public IHttpRouteData GetRouteData(string virtualPathRoot, HttpRequestMessage request)
        {
            var route = new SubRoute(request.RequestUri.LocalPath);
            while (!route.AtEnd())
            {
                var key = Tuple.Create(request.Method.Method, route);
                IHttpRoute[] data;
                if (routDataCache.TryGetValue(key, out data))
                {
                    var httpRouteDatas = data.Select(x => x.GetRouteData(virtualPathRoot, request)).Where(x => x != null).ToArray();
                    return new RavenRouteCollectionRouteData(this, httpRouteDatas);
                }
                route.NextSubRoute();
            }

            return LocateRouteData(virtualPathRoot, request);
        }

        private RavenRouteCollectionRouteData LocateRouteData(string virtualPathRoot, HttpRequestMessage request)
        {
            var matches = (
                from route in subRoutes
                let match = route.GetRouteData(virtualPathRoot, request)
                where match != null
                select new { match, route }
                ).ToArray();

            var matchesTemplateRoutes = matches.Select(m => m.match.Route.RouteTemplate).Distinct();
            var result = matches.Length == 0 ? null : new RavenRouteCollectionRouteData(this, matches.Select(x => x.match).ToArray());
            var routes = matches.Select(x => x.route).ToArray();
            foreach (var matchRoute in matchesTemplateRoutes)
            {
                var subRoute = new SubRoute(request.RequestUri.LocalPath);
                subRoute.ReduceRouteMatchTemplate(matchRoute);
                routDataCache.TryAdd(Tuple.Create(request.Method.Method, subRoute), routes);
            }
            return result;
        }

        private SubRoute GetSubRout(string localPath)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets a virtual path data based on the route and the values provided.
        /// </summary>
        /// <returns>
        /// The virtual path data.
        /// </returns>
        /// <param name="request">The request message.</param><param name="values">The values.</param>
        public IHttpVirtualPathData GetVirtualPath(HttpRequestMessage request, IDictionary<string, object> values)
        {
            return null;
        }

        /// <summary>
        /// Gets the route template describing the URI pattern to match against.  
        /// </summary>
        /// <returns>
        /// The route template.
        /// </returns>
        public string RouteTemplate
        {
            get
            {
                return string.Empty;
            }
        }

        /// <summary>
        /// Gets the default values for route parameters if not provided by the incoming <see cref="T:System.Net.Http.HttpRequestMessage"/>. 
        /// </summary>
        /// <returns>
        /// The default values for route parameters.
        /// </returns>
        public IDictionary<string, object> Defaults
        {
            get
            {
                return Empty;
            }
        }

        /// <summary>
        /// Gets the constraints for the route parameters. 
        /// </summary>
        /// <returns>
        /// The constraints for the route parameters.
        /// </returns>
        public IDictionary<string, object> Constraints
        {
            get
            {
                return Empty;
            }
        }

        /// <summary>
        /// Gets any additional data tokens not used directly to determine whether a route matches an incoming <see cref="T:System.Net.Http.HttpRequestMessage"/>. 
        /// </summary>
        /// <returns>
        /// The additional data tokens.
        /// </returns>
        public IDictionary<string, object> DataTokens
        {
            get
            {
                return null;
            }
        }

        /// <summary>
        /// Gets the message handler that will be the recipient of the request.
        /// </summary>
        /// <returns>
        /// The message handler.
        /// </returns>
        public HttpMessageHandler Handler
        {
            get
            {
                return null;
            }
        }

        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.Collections.Generic.IEnumerator`1"/> that can be used to iterate through the collection.
        /// </returns>
        /// <filterpriority>1</filterpriority>
        public IEnumerator<IHttpRoute> GetEnumerator()
        {
            return subRoutes.GetEnumerator();
        }

        /// <summary>
        /// Returns an enumerator that iterates through a collection.
        /// </summary>
        /// <returns>
        /// An <see cref="T:System.Collections.IEnumerator"/> object that can be used to iterate through the collection.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return subRoutes.GetEnumerator();
        }

        /// <summary>
        /// Gets the number of elements in the collection.
        /// </summary>
        /// <returns>
        /// The number of elements in the collection. 
        /// </returns>
        public int Count
        {
            get
            {
                return subRoutes.Count;
            }
        }

        private readonly ConcurrentDictionary<Tuple<string, SubRoute>, IHttpRoute[]> routDataCache = new ConcurrentDictionary<Tuple<string, SubRoute>, IHttpRoute[]>();
        private static readonly char[] RouteSpliter = new[] { '/' };

        private class SubRoute
        {
            private readonly string fullRoute;
            private bool isOriginalRoute;
            private int subRouteLength;
            public SubRoute(String fullRoute)
            {
                this.fullRoute = fullRoute;
                subRouteLength = fullRoute.Length;
                if (fullRoute.EndsWith("/", StringComparison.OrdinalIgnoreCase))
                    subRouteLength--;
                isOriginalRoute = true;
            }

            public override string ToString()
            {
                return fullRoute.Substring(0, subRouteLength);
            }

            public void NextSubRoute()
            {
                isOriginalRoute = false;
                if (subRouteLength <= 0)
                {
                    subRouteLength = -1;
                    return;
                }
                subRouteLength = fullRoute.LastIndexOf('/', subRouteLength - 1, subRouteLength - 1);
            }

            public bool AtEnd()
            {
                return subRouteLength == -1;
            }
            public override bool Equals(object obj)
            {
                var subRoute = obj as SubRoute;
                if (subRoute == null) return false;
                if (subRoute.subRouteLength != subRouteLength) return false;
                if (isOriginalRoute != subRoute.isOriginalRoute) return false;
                return string.Compare(fullRoute, 0, subRoute.fullRoute, 0, subRouteLength, StringComparison.OrdinalIgnoreCase) == 0;
            }



            public override int GetHashCode()
            {
                int code = 0;
                for (int index = 0; index < subRouteLength; index++)
                {
                    code = code * 397 ^ fullRoute[index];
                }
                if (isOriginalRoute)
                    code = code * 397 ^ 7;
                return code;
            }

            public void ReduceRouteMatchTemplate(string templateRoute)
            {
                var lastIndexOfStar = templateRoute.LastIndexOf("/{*", StringComparison.InvariantCulture);
                if (lastIndexOfStar == -1) return;
                var lastIndexOfDash = Math.Max(templateRoute.LastIndexOf('/', lastIndexOfStar - 1, lastIndexOfStar - 1), 0);
                while (AtEnd() == false)
                {
                    var length = lastIndexOfStar - lastIndexOfDash;
                    var fullRouteIndex = subRouteLength - length;
                    if (fullRouteIndex > 0)
                    {
                        var match = string.Compare(fullRoute, fullRouteIndex, templateRoute, lastIndexOfDash, length, StringComparison.OrdinalIgnoreCase) == 0;
                        if (match && (isOriginalRoute || fullRoute[subRouteLength] == '/'))
                        {
                            break;
                        }
                    }
                    NextSubRoute();
                }
                if (AtEnd()) throw new InvalidOperationException(String.Format("Could not match route template: {0}, with route: {1}, this should not happen!", templateRoute, fullRoute));
            }
        }

        private class RavenRouteCollectionRouteData : IHttpRouteData
        {
            public RavenRouteCollectionRouteData(IHttpRoute parent, IHttpRouteData[] subRouteDatas)
            {
                Route = parent;
                Values = new HttpRouteValueDictionary { { SubRouteDataKey, subRouteDatas } };
            }

            public IHttpRoute Route { get; private set; }

            public IDictionary<string, object> Values { get; private set; }
        }
    }
}
