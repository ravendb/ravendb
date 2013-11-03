using System;
using System.ComponentModel.Composition;
using System.Net;
using System.Text;
using Microsoft.Owin.Hosting;
using Owin;
using Raven.Abstractions.MEF;
using Raven.Database.Config;
using Raven.Database.Server.Security.Windows;

namespace Raven.Database.Server
{
	public sealed class OwinHttpServer : IDisposable
	{
		private readonly IDisposable server;
		private readonly Startup startup;
		private static readonly byte[] NotFoundBody = Encoding.UTF8.GetBytes("Route invalid");

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
				app.Use(async (context, _) =>
				{
					context.Response.StatusCode = 404;
					context.Response.ReasonPhrase = "Not Found";
					await context.Response.Body.WriteAsync(NotFoundBody, 0, NotFoundBody.Length);
				});
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