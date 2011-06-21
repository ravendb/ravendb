using System;
using System.Collections.Generic;
using System.Web;
using System.Web.Mvc;
using System.Web.Routing;
using System.Linq;
using Raven.Client.Connection.Profiling;

namespace Raven.Client.MvcIntegration
{
	public class RavenProfiler
	{

		public static void InitializeFor(IDocumentStore store, params string[] fieldsToFilter)
		{
			var existing = RouteTable.Routes.Select(x =>
			{
				var route = x as Route;
				if (route == null)
				{
					return null;
				}
				return route.RouteHandler;
			})
				.OfType<RavenProfilingHandler>()
				.FirstOrDefault();
			if(existing != null)
			{
				existing.AddStore(store);
				return;
			}
			var ravenProfilingHandler = new RavenProfilingHandler(new HashSet<string>(fieldsToFilter ?? Enumerable.Empty<string>()));
			ravenProfilingHandler.AddStore(store);
			RouteTable.Routes.Add("RavenDB Profiling", new Route("ravendb/profiling", ravenProfilingHandler));
		}

		public static HtmlString CurrentRequestSessions()
		{
			const string template =
				@"<script type=""text/javascript"" src=""/ravendb-profiler-scripts.js""></script>
<script type=""text/javascript""> jQuery(function() {{ RavenDBProfiler.initalize({{ id:[{0}]}}); }} ); </script>";

			var ids = string.Join(",", ContextualSessionList.Select(guid => "'" + guid + "'"));

			return new HtmlString(string.Format(template, ids));
		}

		internal static List<Guid> ContextualSessionList
		{
			get
			{
				const string key = "Raven.Client.MvcIntegration.RavenProfiler.SessionList";

				if (HttpContext.Current == null)
					return new List<Guid>();

				var result = HttpContext.Current.Items[key] as List<Guid>;
				if (result == null)
				{
					HttpContext.Current.Items[key] = result = new List<Guid>();
				}
				return result;
			}
		}
	}
}