using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Owin.Hosting;
using Owin;
using Raven.Database.Config;
using Raven.Database.Embedded;
using Raven.Database.Impl;
using Raven.Database.Server.Security.Windows;

namespace Raven.Database.Server
{
    public sealed class OwinHttpServer : IDisposable
    {
        private IDisposable server;
        private readonly Startup startup;
        private static readonly byte[] NotFoundBody = Encoding.UTF8.GetBytes("Route invalid");
        private readonly OwinEmbeddedHost owinEmbeddedHost;

        public OwinHttpServer(InMemoryRavenConfiguration config, DocumentDatabase db = null, bool useHttpServer = true, Action<RavenDBOptions> configure = null)
        {
            startup = new Startup(config, db);
            if (configure != null)
                configure(startup.Options);
            owinEmbeddedHost = OwinEmbeddedHost.Create(app => startup.Configuration(app));

            if (!useHttpServer)
            {
                return;
            }

            EnableHttpServer(config);
        }

        public void EnableHttpServer(InMemoryRavenConfiguration config)
        {
            if(server != null)
                throw new InvalidOperationException("Http server is already running");

            var schema = config.Encryption.UseSsl ? "https" : "http";
            var hostName = config.HostName ?? "+";
            var url = $"{schema}://{hostName}:{config.Port}";
            server = WebApp.Start(url, app => //TODO DH: configuration.ServerUrl doesn't bind properly
            {
                var listener = (HttpListener) app.Properties["System.Net.HttpListener"];
                if (listener != null)
                {
                    new WindowsAuthConfigureHttpListener().Configure(listener, config);
                }
                startup.Configuration(app);
                app.Use(async (context, _) =>
                {
                    context.Response.StatusCode = 404;
                    context.Response.ReasonPhrase = "Not Found";
                    await context.Response.Body.WriteAsync(NotFoundBody, 0, NotFoundBody.Length).ConfigureAwait(false);
                });
            });
        }

        public void DisableHttpServer()
        {
            if(server == null)
                return;

            using (Options.PreventDispose())
            {
                server.Dispose();

                server = null;
            }
        }

        public Task Invoke(IDictionary<string, object> environment)
        {
            return owinEmbeddedHost.Invoke(environment);
        }

        // Would prefer not to expose this.
        public RavenDBOptions Options
        {
            get { return startup.Options; }
        }
    
        public void Dispose()
        {
            var ea = new ExceptionAggregator("Cannot dispose server");
            ea.Execute(owinEmbeddedHost.Dispose);
            if (server != null)
                ea.Execute(server.Dispose);

            ea.ThrowIfNeeded();
        }
    }
}
