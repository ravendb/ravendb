using System.Diagnostics;
using System.Web;
using System.Web.Http;
using System.Web.Mvc;
using System.Web.Routing;
using Raven.Client;
using Raven.Client.Document;

namespace Raven.Sample.MvcIntegration
{
	public class WebApiApplication : HttpApplication
	{
		protected void Application_Start()
		{
			AreaRegistration.RegisterAllAreas();

			WebApiConfig.Register(GlobalConfiguration.Configuration);
			RouteConfig.RegisterRoutes(RouteTable.Routes);

			Store = new DocumentStore
			{
				Url = "http://localhost:8080"
			}.Initialize();

			InitializeRavenProfiler();
		}

		public static IDocumentStore Store { get; set; }

		[Conditional("DEBUG")]
		private void InitializeRavenProfiler()
		{
			Client.MvcIntegration.RavenProfiler.InitializeFor(Store);
		}
	}
}