using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using System.Web.Routing;
using Raven.Client.Connection.Profiling;
using Raven.Client.Document;

namespace Raven.Client.MvcIntegration
{
	public class RavenProfiler
	{
		/// <summary>
		/// Initializes the RavenProfiler for MVC.
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
			store.Conventions.DisableProfiling = false;

			((DocumentStore)store).InitializeProfiling();
			
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
			var rootUrl = UrlHelper.GenerateContentUrl(@"~/ravendb/profiling", new HttpContextWrapper(HttpContext.Current));
			var script = GetScript(rootUrl, ContextualSessionList);
			return new HtmlString(script);
		}

		private static string GetScript(string rootUrl, IEnumerable<Guid> sessionList)
		{
			using (var stream = typeof(RavenProfiler).Assembly.GetManifestResourceStream("Raven.Client.MvcIntegration.Content.index.html"))
			{
				return new StreamReader(stream).ReadToEnd()
					.Replace("{|id|}", string.Join(",", sessionList.Select(guid => "'" + guid + "'")))
					.Replace("{|rootUrl|}", rootUrl)
					;
			}
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