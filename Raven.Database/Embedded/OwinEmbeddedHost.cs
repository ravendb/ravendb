// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved. See License.txt in the project root for license information.
// Imported from the KatanaProject. Licence Apache 2.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Owin;
using Microsoft.Owin.Hosting;
using Microsoft.Owin.Hosting.Engine;
using Microsoft.Owin.Hosting.ServerFactory;
using Microsoft.Owin.Hosting.Services;
using Owin;

namespace Raven.Database.Embedded
{
    public sealed class OwinEmbeddedHost : IDisposable
    {
        private Func<IDictionary<string, object>, Task> _next;
        private IDisposable _started;
        private bool _disposed;

        private OwinEmbeddedHost()
        {
        }

        public void Dispose()
        {
            _disposed = true;
            _started.Dispose();
        }

        public static OwinEmbeddedHost Create(Action<IAppBuilder> startup)
        {
            var server = new OwinEmbeddedHost();
            server.Configure(startup);
            return server;
        }

        private void Configure(Action<IAppBuilder> startup, StartOptions options = null)
        {
            if (startup == null)
            {
                throw new ArgumentNullException("startup");
            }

            options = options ?? new StartOptions();
            if (string.IsNullOrWhiteSpace(options.AppStartup))
            {
                // Populate AppStartup for use in host.AppName
                options.AppStartup = startup.Method.ReflectedType.FullName;
            }

            var testServerFactory = new OwinEmbeddedServerFactory();
            IServiceProvider services = ServicesFactory.Create();
            var engine = services.GetService<IHostingEngine>();
            var context = new StartContext(options)
            {
                ServerFactory = new ServerFactoryAdapter(testServerFactory),
                Startup = startup
            };
            _started = engine.Start(context);
            _next = testServerFactory.Invoke;
        }

        public async Task Invoke(IDictionary<string, object> environment)
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(GetType().FullName);
            }
            var owinContext = new OwinContext(environment);
            owinContext.Response.Headers.Append("Server", "OwinEmbedded");
            owinContext.Response.Headers.Set("Date", DateTimeOffset.UtcNow.ToString("r"));
            owinContext.Response.Headers.Append("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0");
            owinContext.Response.Headers.Append("Cache-Control", "post-check=0, pre-check=0");
            owinContext.Response.Headers.Set("Pragma", "no-cache");
            await _next.Invoke(environment);
        }

        private class OwinEmbeddedServerFactory
        {
            private Func<IDictionary<string, object>, Task> _app;
            [SuppressMessage("Microsoft.Performance", "CA1823:AvoidUnusedPrivateFields", Justification = "For future use")]
            private IDictionary<string, object> _properties;

            [SuppressMessage("Microsoft.Performance", "CA1811:AvoidUncalledPrivateCode", Justification = "Invoked via reflection.")]
            public IDisposable Create(Func<IDictionary<string, object>, Task> app, IDictionary<string, object> properties)
            {
                _app = app;
                _properties = properties;
                return new Disposable();
            }

            public Task Invoke(IDictionary<string, object> env)
            {
                return _app.Invoke(env);
            }

            private class Disposable : IDisposable
            {
                public void Dispose()
                {
                }
            }
        }
    }
}