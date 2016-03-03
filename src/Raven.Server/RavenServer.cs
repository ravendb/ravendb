using System;
using System.Diagnostics;
using Microsoft.AspNet.Hosting;
using Microsoft.AspNet.Hosting.Internal;
using Microsoft.Extensions.DependencyInjection;
using Raven.Abstractions.Logging;
using Raven.Database.Util;
using Raven.Server.Config;
using Raven.Server.Routing;
using Raven.Server.ServerWide;

namespace Raven.Server
{
    public class RavenServer : IDisposable
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(RavenServer));

        public readonly RavenConfiguration Configuration;

        public readonly ServerStore ServerStore;
        private IApplication _application;

        public RavenServer(RavenConfiguration configuration)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));

            Configuration = configuration;
            if (Configuration.Initialized == false)
                throw new InvalidOperationException("Configuration must be initialized");

            ServerStore = new ServerStore(Configuration);
            Metrics = new MetricsCountersManager();
        }

        public void Initialize()
        {
            var sp = Stopwatch.StartNew();
            try
            {
                ServerStore.Initialize();
            }
            catch (Exception e)
            {
                Log.FatalException("Could not open the server store", e);
                throw;
            }

            if (Log.IsDebugEnabled)
            {
                Log.Debug("Server store started took {0:#,#;;0} ms", sp.ElapsedMilliseconds);
            }
            sp.Restart();

            Router = new RequestRouter(RouteScanner.Scan(), this);

            IHostingEngine hostingEngine;
            try
            {
                hostingEngine = new WebHostBuilder(Configuration.WebHostConfig, true)
                    .UseServer("Microsoft.AspNet.Server.Kestrel")
                    .UseStartup<RavenServerStartup>()
                    .UseServices(services => services.AddInstance(Router))
                    // ReSharper disable once AccessToDisposedClosure
                    .Build();
            }
            catch (Exception e)
            {
                Log.FatalException("Could not configure server", e);
                throw;
            }

            if (Log.IsDebugEnabled)
            {
                Log.Debug("Configuring HTTP server took {0:#,#;;0} ms", sp.ElapsedMilliseconds);
            }

            try
            {
                _application = hostingEngine.Start();
            }
            catch (Exception e)
            {
                Log.FatalException("Could not start server", e);
                throw;
            }
        }

        public RequestRouter Router { get; private set; }
        public MetricsCountersManager Metrics { get; private set; }

        public void Dispose()
        {
            _application?.Dispose();
            ServerStore?.Dispose();
        }
    }
}