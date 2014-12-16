// -----------------------------------------------------------------------
//  <copyright file="RavenRouteCacher.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Web.Http;
using System.Web.Http.Controllers;
using System.Web.Http.Routing;

namespace Raven.Database.Server.WebApi
{
	internal static class RouteCacher
	{
		private const string ActionsDataTokenKey = "actions";

		private const string InfoDataTokenKey = "info";

		private static readonly Dictionary<string, HttpRouteInformation> RouteCache = new Dictionary<string, HttpRouteInformation>();

		public static void CacheRoutesIfNecessary(HttpConfiguration cfg)
		{
			if (RouteCache.Count > 0)
				return;

			for (var i = 0; i < cfg.Routes.Count; i++)
			{
				var j = 0;
				var route = cfg.Routes[i];

				foreach (var routeInformation in CacheRoutes(route))
				{
					RouteCache.Add(string.Format("RavenDB_Route_{0}_{1}", i, j), routeInformation);
					j++;
				}
			}
		}

		public static bool TryAddRoutesFromCache(HttpConfiguration cfg)
		{
			if (RouteCache.Count == 0)
				return false;

			var previousInitializer = cfg.Initializer;
			cfg.Initializer = config =>
			{
				previousInitializer(config);

				var controllerDescriptors = new Dictionary<Type, HttpControllerDescriptor>();
				foreach (var pair in RouteCache)
				{
					var name = pair.Key;
					var route = pair.Value;
					cfg.Routes.Add(name, RebuildRoutes(route, cfg, controllerDescriptors));
				}
			};

			return true;
		}

		private static IHttpRoute RebuildRoutes(HttpRouteInformation routeInformation, HttpConfiguration cfg, Dictionary<Type, HttpControllerDescriptor> controllerDescriptors)
		{
			var routeCollectionInformation = routeInformation as HttpRouteCollectionInformation;
			if (routeCollectionInformation != null)
			{
				var routes = routeCollectionInformation
					.Routes
					.Select(innerRoute => RebuildRouteInternal(innerRoute, cfg, controllerDescriptors))
					.ToList();

				return new RavenRouteCollectionRoute(routes);
			}

			return RebuildRouteInternal(routeInformation, cfg, controllerDescriptors);
		}

		private static IHttpRoute RebuildRouteInternal(HttpRouteInformation routeInformation, HttpConfiguration cfg, Dictionary<Type, HttpControllerDescriptor> controllerDescriptors)
		{
			var route = new HttpRoute(routeInformation.RouteTemplate, new HttpRouteValueDictionary(routeInformation.Defaults), new HttpRouteValueDictionary(routeInformation.Constraints), new HttpRouteValueDictionary(routeInformation.DataTokens), null);

			object value;
			if (route.DataTokens.TryGetValue(InfoDataTokenKey, out value))
			{
				var descriptorInformations = (HttpActionDescriptorInformation[])value;
				var descriptors = new HttpActionDescriptor[descriptorInformations.Length];
				for (var index = 0; index < descriptorInformations.Length; index++)
				{
					var descriptorInformation = descriptorInformations[index];

					HttpControllerDescriptor controllerDescriptor;
					if (controllerDescriptors.TryGetValue(descriptorInformation.ControllerType, out controllerDescriptor) == false)
						controllerDescriptors[descriptorInformation.ControllerType] = controllerDescriptor = new HttpControllerDescriptor(cfg, descriptorInformation.ControllerName, descriptorInformation.ControllerType);

					var descriptor = new ReflectedHttpActionDescriptor(controllerDescriptor, descriptorInformation.MethodInfo);

					foreach (var pair in descriptorInformation.Properties)
						descriptor.Properties.AddOrUpdate(pair.Key, pair.Value, (_, __) => pair.Value);

					descriptors[index] = descriptor;
				}

				route.DataTokens.Remove(InfoDataTokenKey);
				route.DataTokens[ActionsDataTokenKey] = descriptors;
			}

			return route;
		}

		private static IEnumerable<HttpRouteInformation> CacheRoutes(IHttpRoute route)
		{
			var collection = route as IReadOnlyCollection<IHttpRoute>;
			if (collection != null)
			{
				var routes = collection
					.Select(CacheRouteInternal)
					.ToList();

				yield return new HttpRouteCollectionInformation(routes);

				yield break;
			}

			yield return CacheRouteInternal(route);
		}

		private static HttpRouteInformation CacheRouteInternal(IHttpRoute route)
		{
			var httpRoute = route as HttpRoute;
			if (httpRoute == null)
				throw new InvalidOperationException("Invalid route");

			if (httpRoute.Handler != null)
				throw new InvalidOperationException("Cannot copy route with handler: " + route.RouteTemplate);

			var routeInformation = new HttpRouteInformation
			{
				Constraints = httpRoute.Constraints.ToDictionary(x => x.Key, x => x.Value),
				DataTokens = httpRoute.DataTokens.ToDictionary(x => x.Key, x => x.Value),
				Defaults = httpRoute.Defaults.ToDictionary(x => x.Key, x => x.Value),
				RouteTemplate = httpRoute.RouteTemplate
			};

			object value;
			if (routeInformation.DataTokens.TryGetValue(ActionsDataTokenKey, out value))
			{
				var descriptors = (HttpActionDescriptor[])value;
				var descriptorInformations = new HttpActionDescriptorInformation[descriptors.Length];
				for (var index = 0; index < descriptors.Length; index++)
				{
					var descriptor = (ReflectedHttpActionDescriptor)descriptors[index];
					var methodInfo = descriptor.MethodInfo;
					var controllerName = descriptor.ControllerDescriptor.ControllerName;
					var controllerType = descriptor.ControllerDescriptor.ControllerType;
					var properties = descriptor.Properties.ToDictionary(x => x.Key, x => x.Value);

					descriptorInformations[index] = new HttpActionDescriptorInformation
					{
						MethodInfo = methodInfo,
						ControllerName = controllerName,
						ControllerType = controllerType,
						Properties = properties
					};
				}

				routeInformation.DataTokens.Remove(ActionsDataTokenKey);
				routeInformation.DataTokens[InfoDataTokenKey] = descriptorInformations;
			}

			return routeInformation;
		}

		private class HttpRouteCollectionInformation : HttpRouteInformation
		{
			public IReadOnlyCollection<HttpRouteInformation> Routes { get; private set; }

			public HttpRouteCollectionInformation(IReadOnlyCollection<HttpRouteInformation> routes)
			{
				Routes = routes;
			}
		}

		private class HttpRouteInformation
		{
			public string RouteTemplate { get; set; }

			public IDictionary<string, object> Defaults { get; set; }

			public IDictionary<string, object> Constraints { get; set; }

			public IDictionary<string, object> DataTokens { get; set; }
		}

		private class HttpActionDescriptorInformation
		{
			public string ControllerName { get; set; }

			public Type ControllerType { get; set; }

			public MethodInfo MethodInfo { get; set; }

			public Dictionary<object, object> Properties { get; set; }
		}
	}
}