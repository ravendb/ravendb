// -----------------------------------------------------------------------
//  <copyright file="Startup.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Web.Http;

using Owin;

namespace Raven.Tests.Server.Runner
{
	public class Startup
	{
		public void Configuration(IAppBuilder appBuilder)
		{
			var config = new HttpConfiguration();
			config.Routes.MapHttpRoute(
				name: "DefaultApi",
				routeTemplate: "{controller}/{id}",
				defaults: new { id = RouteParameter.Optional }
			);

			appBuilder.UseWebApi(config);
		}
	}
}