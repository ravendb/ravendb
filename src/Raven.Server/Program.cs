using System;
using System.Diagnostics;
using Microsoft.AspNet.Builder;
using Microsoft.AspNet.Hosting;
using Microsoft.AspNet.Hosting.Internal;
using Microsoft.AspNet.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Raven.Abstractions.Logging;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.Utils;

namespace Raven.Server
{
    public class Program
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(Program));

        public void ConfigureServices(IServiceCollection services)
        {
            //services.AddMvc();
        }

        public void Configure(IApplicationBuilder app, ILoggerFactory loggerfactory)
        {
            var scanner = new RouteScanner();
            var routes = scanner.Scan();
            var router = new RequestRouter(routes);
            app.Run(context => router.HandlePath(context));
        }

        public static int Main(string[] args)
        {
            var configBuilder = new ConfigurationBuilder()
               .AddJsonFile("settings.json", optional: true)
               .AddEnvironmentVariables(prefix: "RAVEN_");

         
            if (args != null)
            {
                configBuilder.AddCommandLine(args);
            }

            var config = configBuilder.Build();

            WelcomeMessage.Print();
           
            var sp = Stopwatch.StartNew();

            ServerStore serverStore;
            try
            {
                serverStore = new ServerStore(config);
                serverStore.Initialize();
            }
            catch (Exception e)
            {
                Log.FatalException("Could not open the server store", e);
                Console.WriteLine(e);
                return -1;
            }

            if (Log.IsDebugEnabled)
            {
                Log.Debug("Server store started took {0:#,#;;0} ms", sp.ElapsedMilliseconds);
            }
            sp.Restart();
            IHostingEngine application;
            try
            {
                application = new WebHostBuilder(config)
                    .UseStartup<Program>()
                    .UseServer("Microsoft.AspNet.Server.Kestrel")
                    .UseServices(services => services.AddInstance(serverStore))
                    .Build();
            }
            catch (Exception e)
            {
                Log.FatalException("Could not setup server", e);
                Console.WriteLine(e);
                return -2;
            }


            if (Log.IsDebugEnabled)
            {
                Log.Debug("Configuring HTTP server took {0:#,#;;0} ms", sp.ElapsedMilliseconds);
            }


            IApplication app = null;
            try
            {
                try
                {
                    app = application.Start();
                }
                catch (Exception e)
                {
                    Log.FatalException("Could not start server", e);
                    Console.WriteLine(e);
                    return -3;
                }

                Console.WriteLine("Server started, listening to requests...");

                //TODO: Move the command line options to here
                while (true)
                {
                    if (Console.ReadLine() == "q")
                        break;

                    Console.ForegroundColor++;
                }
                Log.Info("Server is shutting down");
                return 0;
            }
            finally
            {
                app?.Dispose();
            }
        }

    }
}