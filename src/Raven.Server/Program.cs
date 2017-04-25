using System;
using System.IO;
using System.Linq;
using System.Runtime.Loader;
using System.Threading;
using System.Threading.Tasks;
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

            var customConfigPath = ParseCustomConfigPath(args);
            var configuration = new RavenConfiguration(null, ResourceType.Server, customConfigPath);
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

        private static string ParseCustomConfigPath(string[] args)
        {
            string customConfigPath = null;
            if (args != null)
            {
                foreach (var cliOpt in args)
                {
                    if (cliOpt.StartsWith("/Raven/Config="))
                    {
                        customConfigPath = cliOpt.Split('=')[1].Trim();
                        break;
                    }
                }
            }
            return customConfigPath;
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

            //stop dumping logs
            LoggingSource.Instance.DisableConsoleLogging();
            LoggingSource.Instance.SetupLogMode(LogMode.None,
                Path.Combine(AppContext.BaseDirectory, configuration.Core.LogsDirectory));

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

                    case "log -n":
                    case "log no http":
                    case "log-no-http":
                        RavenServerStartup.SkipHttpLogging = true;
                        goto case "log";

                    case "log":
                        
                        LoggingSource.Instance.EnableConsoleLogging();
                        LoggingSource.Instance.SetupLogMode(LogMode.Information,
                            Path.Combine(AppContext.BaseDirectory, configuration.Core.LogsDirectory));
                        break;

                    case "no-log":
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
                        WriteListOfAvailableCommands();
                        break;

                    default:
                        Console.WriteLine("Unknown command...");
                        goto case "help";
                }
            }
        }

        private static void WriteListOfAvailableCommands()
        {
            Console.WriteLine("Available Commands:");

            var description = "clear screen";
            Console.WriteLine($"    cls {description, 23}");

            description = "dump logs to console";
            Console.WriteLine($"    log [-n] {description, 26}");

            description = "-n to dump logs without outputting http request logs";
            Console.WriteLine($"    {description, 67}");

            description = "stop dumping logs to console";
            Console.WriteLine($"    no-log {description, 36}");

            description = "simulate low memory";
            Console.WriteLine($"    low-mem {description, 26}");

            description = "dump statistical information";
            Console.WriteLine($"    stats {description, 37}");

            description = "quit";
            Console.WriteLine($"    q {description, 17}");

            Console.WriteLine();

        }

        private static void WriteServerStatsAndWaitForEsc(RavenServer server)
        {
            Console.WriteLine("Showing stats, press any key to close...");
            Console.WriteLine("    working set     | native mem      | managed mem     | mmap size         | reqs/sec       | docs (all dbs)");
            var i = 0;
            while (Console.KeyAvailable == false)
            {
                var json = MemoryStatsHandler.MemoryStatsInternal();
                var humaneProp = (json["Humane"] as DynamicJsonValue);
                var reqCounter = server.Metrics.RequestsMeter;

                Console.Write($"\r {((i++%2) == 0 ? "*" : "+")} ");

                Console.Write($" {humaneProp?["WorkingSet"],-14} ");
                Console.Write($" | {humaneProp?["TotalUnmanagedAllocations"],-14} ");
                Console.Write($" | {humaneProp?["ManagedAllocations"],-14} ");
                Console.Write($" | {humaneProp?["TotalMemoryMapped"],-17} ");

                Console.Write($"| {Math.Round(reqCounter.OneSecondRate, 1),-14:#,#.#;;0} ");

                long allDocs = 0;
                foreach (var value in server.ServerStore.DatabasesLandlord.DatabasesCache.Values)
                {
                    if(value.Status != TaskStatus.RanToCompletion)
                        continue;

                    try
                    {
                        allDocs += value.Result.DocumentsStorage.GetNumberOfDocuments();
                    }
                    catch (Exception)
                    {
                        // may run out of virtual address space, or just shutdown, etc
                    }
                }

                Console.Write($"| {allDocs,14:#,#.#;;0}      ");

                for (int j = 0; j < 5 && Console.KeyAvailable == false; j++)
                {
                    Thread.Sleep(100);
                }
            }

            Console.ReadKey(true);
            Console.WriteLine();
            Console.WriteLine("Stats halted");

        }
    }
}
