namespace Raven.Tests.Silverlight.Web
{
	using System.Web;
	using System.Web.Mvc;
	using System.Web.Routing;
	using Client;
	using Client.Client;

	public class MvcApplication : HttpApplication
	{
		const string RavenSessionKey = "Raven.Session";
		static EmbeddableDocumentStore _documentStore;

		public static IDocumentSession CurrentSession
		{
			get { return (IDocumentSession) HttpContext.Current.Items[RavenSessionKey]; }
		}

		public static void RegisterRoutes(RouteCollection routes)
		{
			routes.IgnoreRoute("{resource}.axd/{*pathInfo}");

			routes.MapRoute(
				"Default", // Route name
				"{controller}/{action}/{id}", // URL with parameters
				new {controller = "Home", action = "Index", id = UrlParameter.Optional} // Parameter defaults
				);
		}

		protected void Application_Start()
		{
			//_documentStore = new EmbeddableDocumentStore {Url = "http://localhost:8080/",UseEmbeddedHttpServer = true};
			//_documentStore.Configuration.DataDirectory ="MyData";
			//_documentStore.Initialize();

			AreaRegistration.RegisterAllAreas();

			RegisterRoutes(RouteTable.Routes);
		}
	}
}