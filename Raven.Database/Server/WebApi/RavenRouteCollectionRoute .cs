// -----------------------------------------------------------------------
//  <copyright file="RavenRouteCollectionRoute .cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections;
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

		public IHttpRouteData GetRouteData(string virtualPathRoot, HttpRequestMessage request)
		{
			var matches = subRoutes
				.Select(route => route.GetRouteData(virtualPathRoot, request))
				.Where(match => match != null)
				.ToArray();

			return matches.Length == 0 ? null : new RavenRouteCollectionRouteData(this, matches);
		}

		public IHttpVirtualPathData GetVirtualPath(HttpRequestMessage request, IDictionary<string, object> values)
		{
			return null;
		}

		public string RouteTemplate
		{
			get
			{
				return string.Empty;
			}
		}

		public IDictionary<string, object> Defaults
		{
			get
			{
				return Empty;
			}
		}

		public IDictionary<string, object> Constraints
		{
			get
			{
				return Empty;
			}
		}

		public IDictionary<string, object> DataTokens
		{
			get
			{
				return null;
			}
		}

		public HttpMessageHandler Handler
		{
			get
			{
				return null;
			}
		}

		public IEnumerator<IHttpRoute> GetEnumerator()
		{
			return subRoutes.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return subRoutes.GetEnumerator();
		}

		public int Count
		{
			get
			{
				return subRoutes.Count;
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