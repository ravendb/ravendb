using System;
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

            using (var server = new RavenServer(configuration))
            {
                try
                {
                    server.Initialize();
                    Console.WriteLine($"Listening to: {string.Join(", ", configuration.Core.ServerUrl)}");
                    Console.WriteLine("Server started, listening to requests...");

                    //TODO: Move the command line options to here
                    var run = true;
                    while (run)
                    {
                        switch (Console.ReadLine()?.ToLower())
                        {
                            case "q":
                                run = false;
                                break;
                            case "cls":
                                Console.Clear();
                                break;
                        }
                        // Console.ForegroundColor++;
                    }
                    if (_logger.IsInfoEnabled)
                        _logger.Info("Server is shutting down");
                    return 0;
                }
                catch (Exception e)
                {
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations("Failed to initialize the server", e);
                    Console.WriteLine(e);
                    return -1;
                }
                finally
                {
                    Console.WriteLine("Bye, bye...");
                }
            }
        }
    }
}