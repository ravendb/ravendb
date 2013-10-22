using System;
using System.Net;
using Microsoft.Owin.Hosting;
using Raven.Database.Config;

namespace Raven.Database.Server
{
	public sealed class OwinHttpServer : IDisposable
	{
		private readonly IDisposable server;
		private readonly Startup startup;

		public OwinHttpServer(InMemoryRavenConfiguration config)
		{
			//TODO DH: configuration.ServerUrl doesn't bind properly
			startup = new Startup(config);
			server = WebApp.Start("http://+:" + config.Port, app =>
			{
				var listener = (HttpListener) app.Properties["System.Net.HttpListener"];
				listener.AuthenticationSchemes = AuthenticationSchemes.IntegratedWindowsAuthentication |
				                                 AuthenticationSchemes.Anonymous;
				startup.Configuration(app);
			});
		}

		// Would prefer not to expose this.
		public RavenDBOptions Options
		{
			get { return startup.Options; }
		}
	
		public void Dispose()
		{
			server.Dispose();
		}
	}
}