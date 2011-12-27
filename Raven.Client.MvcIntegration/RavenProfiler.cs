using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using System.Web.Routing;
using Raven.Client.Connection.Profiling;

namespace Raven.Client.MvcIntegration
{
	public class RavenProfiler
	{
		/// <summary>
		/// Initializes the RavenProfiler for MVC
		/// IMPORTANT! This method may only be called from the Application_Start method, otherwise
		/// it might lead to problems, since it modify the Routes table.
		/// </summary>
		public static void InitializeFor(IDocumentStore store, params string[] fieldsToFilter)
		{
			var existing = RouteTable.Routes
				.Select(x =>
				        	{
				        		var route = x as Route;
				        		if (route == null)
				        			return null;
				        		return route.RouteHandler;
				        	})
				.OfType<RavenProfilingHandler>()
				.FirstOrDefault();

			if (existing != null)
			{
				existing.AddStore(store);
				return;
			}
			
			ProfilingInformation.OnContextCreated += ProfilingInformationOnOnContextCreated;

			var ravenProfilingHandler = new RavenProfilingHandler(new HashSet<string>(fieldsToFilter ?? Enumerable.Empty<string>()));
			ravenProfilingHandler.AddStore(store);

			RouteTable.Routes.Insert(0, new Route("ravendb/profiling", new RouteValueDictionary(new { controller = "RavenProfilingHandler", action = "ProcessRequest" }), ravenProfilingHandler));
			GlobalFilters.Filters.Add(new RecordCurrentControllerContextFilter(), -100);
		}

		private static void ProfilingInformationOnOnContextCreated(ProfilingInformation profilingInformation)
		{
			if (RecordCurrentControllerContextFilter.CurrentControllerContext == null)
				return;

			var routeData = RecordCurrentControllerContextFilter.CurrentControllerContext.RequestContext.RouteData;
			if (routeData == null)
				return;
			foreach (var value in routeData.Values)
			{
				profilingInformation.Context[value.Key] = (value.Value ?? "null").ToString();
			}
		}

		public static HtmlString CurrentRequestSessions()
		{
			var httpContextWrapper = new HttpContextWrapper(HttpContext.Current);
			var root = UrlHelper.GenerateContentUrl("~/ravendb/profiling", httpContextWrapper);
			var script = UrlHelper.GenerateContentUrl("~/ravendb/profiling?path=ravendb-profiler-scripts.js", httpContextWrapper);
			var yepnope = UrlHelper.GenerateContentUrl("~/ravendb/profiling?path=yepnope.js", httpContextWrapper);
			var jquery = UrlHelper.GenerateContentUrl("~/ravendb/profiling?path=jquery-1.6.1.min.js", httpContextWrapper);
			var jquerytmpl = UrlHelper.GenerateContentUrl("~/ravendb/profiling?path=jquery.tmpl.min.js", httpContextWrapper);
			const string template =
				@"
<script type=""text/javascript"" src=""{3}""></script>
<script type=""text/javascript""> yepnope([{{ test: window.jQuery, nope: '{4}' }}, {{ test: window.jQuery && window.jQuery.fn.tmpl, nope: '{5}' }}, {{load: '{1}', complete: function() {{ jQuery(function() {{ RavenDBProfiler.initalize({{ id:[{0}], url: '{2}' }}); }} ); }}  }}]); </script>";

			var ids = string.Join(",", ContextualSessionList.Select(guid => "'" + guid + "'"));

			return new HtmlString(string.Format(template, ids, script, root, yepnope, jquery, jquerytmpl));
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