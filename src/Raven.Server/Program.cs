using System;
using System.Runtime.Loader;
using System.Threading;
using Microsoft.Extensions.Configuration;
using Raven.Abstractions.Logging;
using Raven.Server.Config;
using Raven.Server.Utils;
using Sparrow.Logging;

namespace Raven.Server
{
    public class Program
    {
        private static Logger _logger;

        public static int Main(string[] args)
        {
            LoggingSource.Instance.SetupLogMode(LogMode.Operations, "Logs");
            _logger = LoggingSource.Instance.GetLogger<Program>("Raven/Server");

            WelcomeMessage.Print();

            var configuration = new RavenConfiguration();
            if (args != null)
            {
                configuration.AddCommandLine(args);
            }
            configuration.Initialize();

            try
            {
                using (var server = new RavenServer(configuration))
                {
                    try
                    {
                        server.Initialize();
                        Console.WriteLine($"Listening to: {string.Join(", ", configuration.Core.ServerUrl)}");
                        Console.WriteLine("Server started, listening to requests...");

                        if (configuration.Core.RunAsService)
                        {
                            RunAsService();
                        }
                        else
                        {
                            RunInteractive();
                        }
                        Console.WriteLine("Starting shut down...");
                        if (_logger.IsInfoEnabled)
                            _logger.Info("Server is shutting down");
                    }
                    catch (Exception e)
                    {
                        if (_logger.IsOperationsEnabled)
                            _logger.Operations("Failed to initialize the server", e);
                        Console.WriteLine(e);
                        return -1;
                    }
                }
                Console.WriteLine("Shutdown completed");
                return 0;
            }
            catch (Exception e)
            {
                Console.WriteLine("Error during shutdown");
                Console.WriteLine(e);
                return -2;
            }
        }

        private static void RunAsService()
        {
            ManualResetEvent mre = new ManualResetEvent(false);
            if (_logger.IsInfoEnabled)
                _logger.Info("Server is running as a service");
            Console.WriteLine("Running as Service");
            AssemblyLoadContext.Default.Unloading += (s) =>
            {
                Console.WriteLine("Received graceful exit request...");
                mre.Set();
            };
            mre.WaitOne();
        }

        private static void RunInteractive()
        {
            while (true)
            {
                //TODO: Move the command line options to here
                switch (Console.ReadLine()?.ToLower())
                {
                    case "q":

                        return;
                    case "cls":
                        Console.Clear();
                        break;
                }
            }
        }
    }
}