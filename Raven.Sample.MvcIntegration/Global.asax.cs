using System.Web;
using System.Web.Http;
using System.Web.Mvc;
using System.Web.Routing;

namespace Raven.Sample.MvcIntegration
{
	public class WebApiApplication : HttpApplication
	{
		protected void Application_Start()
		{
			AreaRegistration.RegisterAllAreas();

			WebApiConfig.Register(GlobalConfiguration.Configuration);
			RouteConfig.RegisterRoutes(RouteTable.Routes);
		}
	}
}