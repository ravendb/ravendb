using System;
using System.Web;
using System.Web.Routing;

namespace Raven.Server
{
	public class RavenApplication : HttpApplication
	{
		public void Application_Start()
		{
			RouteTable.Routes.Add(new AllRoutes());
		}
 
	}

	public class AllRoutes : RouteBase
	{
		public override RouteData GetRouteData(HttpContextBase httpContext)
		{
			return new RouteData(this, new SimpleHandler());
		}

		public override VirtualPathData GetVirtualPath(RequestContext requestContext, RouteValueDictionary values)
		{
			return new VirtualPathData(this, requestContext.HttpContext.Request.Url.PathAndQuery);
		}
	}

	public class SimpleHandler : IRouteHandler, IHttpHandler
	{
		public IHttpHandler GetHttpHandler(RequestContext requestContext)
		{
			return this;
		}

		public void ProcessRequest(HttpContext context)
		{
			context.Response.Write("Hi");
		}

		public bool IsReusable
		{
			get { return false; }
		}
	}
}