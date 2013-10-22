using System;
using System.ComponentModel.Composition;
using System.Net;
using Microsoft.Owin.Hosting;
using Raven.Abstractions.MEF;
using Raven.Database.Config;
using Raven.Database.Server.Security.Windows;

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
				/*foreach (var configureHttpListener in ConfigureHttpListeners)
				{
					configureHttpListener.Value.Configure(listener, config);
				}*/
				new WindowsAuthConfigureHttpListener().Configure(listener, config);
				startup.Configuration(app);
			});
		}

		[ImportMany]
		public OrderedPartCollection<IConfigureHttpListener> ConfigureHttpListeners { get; set; }

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