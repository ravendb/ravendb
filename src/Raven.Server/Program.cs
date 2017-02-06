using System;
using System.IO;
using System.Linq;
using System.Runtime.Loader;
using System.Threading;
using Raven.Server.Config;
using Raven.Server.Documents.Handlers.Debugging;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.LowMemoryNotification;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server
{
    public class Program
    {
        private static Logger _logger;

        public static int Main(string[] args)
        {
            WelcomeMessage.Print();

            var configuration = new RavenConfiguration(null, ResourceType.Server);
            if (args != null)
            {
                configuration.AddCommandLine(args);
            }

            configuration.Initialize();

            LogMode mode;
            if (Enum.TryParse(configuration.Core.LogLevel, out mode) == false)
                mode = LogMode.Operations;

            LoggingSource.Instance.SetupLogMode(mode, Path.Combine(AppContext.BaseDirectory, configuration.Core.LogsDirectory));
            _logger = LoggingSource.Instance.GetLogger<Program>("Raven/Server");

            try
            {
                using (var server = new RavenServer(configuration))
                {
                    try
                    {
                        server.Initialize();
                        Console.WriteLine($"Listening to: {string.Join(", ", server.WebUrls)}");

                        var serverWebUrl = server.WebUrls[0];
                        server.GetTcpServerStatusAsync()
                            .ContinueWith(tcp =>
                            {
                                if (tcp.IsCompleted)
                                {
                                    Console.WriteLine($"Tcp listening on {string.Join(", ", tcp.Result.Listeners.Select(l => l.LocalEndpoint))}");
                                }
                                else
                                {
                                    Console.Error.WriteLine($"Tcp listen failure (see {serverWebUrl}/info/tcp for details) {tcp.Exception.Message}");
                                }
                            });
                        Console.WriteLine("Server started, listening to requests...");

                        if (configuration.Core.RunAsService)
                        {
                            RunAsService();
                        }
                        else
                        {
                            RunInteractive(server);
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

        private static void RunInteractive(RavenServer server)
        {
            var configuration = server.Configuration;

            bool ctrlCPressed = false;
            Console.CancelKeyPress += (sender, args) =>
            {
                ctrlCPressed = true;
            };

            while (true)
            {
                var lower = Console.ReadLine()?.ToLower();
                switch (lower)
                {
                    case null:
                        Thread.Sleep(75);//waiting for Ctrl+C 
                        if (ctrlCPressed)
                            break;
                        Console.WriteLine("End of standard input detected, switching to server mode...");
                        RunAsService();
                        return;

                    case "q":
                        return;

                    case "cls":
                        Console.Clear();
                        break;

                    case "log no http":
                    case "log-no-http":
                        RavenServerStartup.SkipHttpLogging = true;
                        goto case "log";

                    case "log":
                        
                        LoggingSource.Instance.EnableConsoleLogging();
                        LoggingSource.Instance.SetupLogMode(LogMode.Information,
                            Path.Combine(AppContext.BaseDirectory, configuration.Core.LogsDirectory));
                        break;

                    case "nolog":
                        LoggingSource.Instance.DisableConsoleLogging();
                        LoggingSource.Instance.SetupLogMode(LogMode.None,
                            Path.Combine(AppContext.BaseDirectory, configuration.Core.LogsDirectory));
                        break;

                    case "stats":
                        //stop dumping logs
                        LoggingSource.Instance.DisableConsoleLogging();
                        LoggingSource.Instance.SetupLogMode(LogMode.None,
                            Path.Combine(AppContext.BaseDirectory, configuration.Core.LogsDirectory));

                        WriteServerStatsAndWaitForEsc(server);

                        break;

                    case "oom":
                    case "low-mem":
                    case "low-memory":
                        AbstractLowMemoryNotification.Instance.SimulateLowMemoryNotification();
                        break;

                    case "help":
                    case "–help":
                    case "-help":
                    case "--help":
                    case "/?":
                    case "--?":
                        Console.WriteLine("Avaliable Commands :");
                        Console.WriteLine("[cls] : clear screen");
                        Console.WriteLine("[log]: dump logs to console");
                        Console.WriteLine("[log no http]: dump logs to console without outputting the http request logs");
                        Console.WriteLine("[nolog]: stop dumping logs to console");
                        Console.WriteLine("[low-mem] : simulate low memory");
                        Console.WriteLine("[stats]: dump statistical information");
                        Console.WriteLine("[q]: quit");
                        Console.WriteLine();
                        break;

                    default:
                        Console.WriteLine("Unknown command...");
                        goto case "help";
                }
            }
        }

        private static void WriteServerStatsAndWaitForEsc(RavenServer server)
        {
            Console.WriteLine("Showing stats, press ESC to close...");
            Console.WriteLine("    working set     | native mem      | managed mem     | mmap size         | reqs/sec (now, 5s, 1m, 5m) ");
            var i = 0;
            while (Console.KeyAvailable == false || Console.ReadKey(true).Key != ConsoleKey.Escape)
            {
                var json = MemoryStatsHandler.MemoryStatsInternal();
                var humaneProp = (json["Humane"] as DynamicJsonValue);
                var reqCounter = server.Metrics.RequestsMeter;

                Console.Write($"\r {((i++%2) == 0 ? "*" : "+")} ");

                Console.Write($" {humaneProp?["WorkingSet"],-14} ");
                Console.Write($" | {humaneProp?["TotalUnmanagedAllocations"],-14} ");
                Console.Write($" | {humaneProp?["ManagedAllocations"],-14} ");
                Console.Write($" | {humaneProp?["TotalMemoryMapped"],-17} ");

                Console.Write($"| {Math.Round(reqCounter.OneSecondRate, 1):#,#.#;;0}, {Math.Round(reqCounter.FiveSecondRate, 1):#,#.#;;0}, {Math.Round(reqCounter.OneMinuteRate, 1):#,#.#;;0}, {Math.Round(reqCounter.FiveMinuteRate, 1):#,#.#;;0}             ");

                for (int j = 0; j < 5 && Console.KeyAvailable == false; j++)
                {
                    Thread.Sleep(100);
                }
            }
            Console.WriteLine();
            Console.WriteLine("Stats halted");
        }
    }
}
