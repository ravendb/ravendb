using System;
using System.IO;
using System.Diagnostics;
using System.Linq;
using System.Runtime.Loader;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.CommandLineUtils;
using Raven.Server.Config;
using Raven.Server.Documents.Handlers.Debugging;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Utils;

namespace Raven.Server
{
    public class Program
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<Program>("Raven/Server");

        public static int Main(string[] args)
        {
            string[] configurationArgs;
            try
            {
                configurationArgs = CommandLineSwitches.Process(args);
            }
            catch (CommandParsingException commandParsingException)
            {
                Console.WriteLine(commandParsingException.Message);
                CommandLineSwitches.ShowHelp();
                return 1;
            }

            if (CommandLineSwitches.ShouldShowHelp)
            {
                CommandLineSwitches.ShowHelp();
                return 0;
            }

            if (CommandLineSwitches.PrintVersionAndExit)
            {
                Console.WriteLine(ServerVersion.FullVersion);
                return 0;
            }

            if (CommandLineSwitches.RegisterService)
            {
                RavenWindowsServiceController.Install(args);
                return 0;
            }

            if (CommandLineSwitches.UnregisterService)
            {
                RavenWindowsServiceController.Uninstall();
                return 0;
            }

            WelcomeMessage.Print();

            var configuration = new RavenConfiguration(null, ResourceType.Server, CommandLineSwitches.CustomConfigPath);

            if (configurationArgs != null)
                configuration.AddCommandLine(configurationArgs);

            configuration.Initialize();

            LogMode mode;
            if (Enum.TryParse(configuration.Core.LogLevel, out mode) == false)
                mode = LogMode.Operations;

            LoggingSource.Instance.SetupLogMode(mode, Path.Combine(AppContext.BaseDirectory, configuration.Core.LogsDirectory));

            if (RavenWindowsServiceController.ShouldRunAsWindowsService(configuration))
            {
                RavenWindowsServiceController.Run(configuration);
                return 0;
            }

            var rerun = false;
            do
            {
                if (rerun)
                {
                    Console.WriteLine("\nRestarting Server...");
                    rerun = false;
                }

                try
                {
                    using (var server = new RavenServer(configuration))
                    {
                        try
                        {
                            server.Initialize();

                            if (CommandLineSwitches.PrintServerId)
                                Console.WriteLine($"Server ID is {server.ServerStore.GetServerId()}.");

                            if (CommandLineSwitches.LaunchBrowser)
                                BrowserHelper.OpenStudioInBrowser(server);

                            Console.WriteLine($"Listening on: {string.Join(", ", server.WebUrls)}");

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

                            if (CommandLineSwitches.RunAsService)
                            {
                                RunAsService();
                            }
                            else
                            {
                                rerun = RunInteractive(server);
                            }
                            Console.WriteLine("Starting shut down...");
                            if (Logger.IsInfoEnabled)
                                Logger.Info("Server is shutting down");
                        }
                        catch (Exception e)
                        {
                            if (Logger.IsOperationsEnabled)
                                Logger.Operations("Failed to initialize the server", e);
                            Console.WriteLine(e);
                            
                            return -1;
                        }
                    }

                    Console.WriteLine("Shutdown completed");
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error during shutdown");
                    Console.WriteLine(e);
                    return -2;
                }
            } while (rerun);

            return 0;
        }

        private static void RunAsService()
        {
            ManualResetEvent mre = new ManualResetEvent(false);

            if (Logger.IsInfoEnabled)
                Logger.Info("Server is running as a service");
            Console.WriteLine("Running as Service");

            AssemblyLoadContext.Default.Unloading += (s) =>
            {
                Console.WriteLine("Received graceful exit request...");
                mre.Set();
            };

            mre.WaitOne();
        }

        private static bool RunInteractive(RavenServer server)
        {
            var configuration = server.Configuration;

            var ctrlCPressed = false;
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
                        return false;

                    case "q":
                        return false;

                    case "cls":
                        Console.Clear();
                        break;

                    case "reset":
                        return true;

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

                    case "info":
                        var meminfo = MemoryInformation.GetMemoryInfo();
                        Console.WriteLine(" Build {0}, Version {1}, SemVer {2}, Commit {3}\r\n PID {4}, {5} bits, {6} Cores\r\n {7} Physical Memory, {8} Available Memory",
                            ServerVersion.Build, ServerVersion.Version, ServerVersion.FullVersion, ServerVersion.CommitHash, Process.GetCurrentProcess().Id,
                            IntPtr.Size * 8, ProcessorInfo.ProcessorCount, meminfo.TotalPhysicalMemory, meminfo.AvailableMemory);
                        break;

                    case "gc2":
                        GC.Collect(GC.MaxGeneration);
                        GC.WaitForPendingFinalizers();
                        break;

                    case "oom":
                    case "low-mem":
                    case "low-memory":
                        LowMemoryNotification.Instance.SimulateLowMemoryNotification();
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
            Console.WriteLine($"    cls {description,23}");

            description = "dump logs to console";
            Console.WriteLine($"    log [-n] {description,26}");

            description = "-n to dump logs without outputting http request logs";
            Console.WriteLine($"    {description,67}");

            description = "stop dumping logs to console";
            Console.WriteLine($"    no-log {description,36}");

            description = "simulate low memory";
            Console.WriteLine($"    low-mem {description,26}");

            description = "dump statistical information";
            Console.WriteLine($"    stats {description,37}");

            description = "print info (core count, memory, pid, etc)";
            Console.WriteLine($"    info {description,51}");

            description = "collect gc max generation";
            Console.WriteLine($"    gc2 {description,36}");

            description = "reset the server";
            Console.WriteLine($"    reset {description,25}");

            description = "quit";
            Console.WriteLine($"    q {description,17}");

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

                Console.Write($"\r {((i++ % 2) == 0 ? "*" : "+")} ");

                Console.Write($" {humaneProp?["WorkingSet"],-14} ");
                Console.Write($" | {humaneProp?["TotalUnmanagedAllocations"],-14} ");
                Console.Write($" | {humaneProp?["ManagedAllocations"],-14} ");
                Console.Write($" | {humaneProp?["TotalMemoryMapped"],-17} ");

                Console.Write($"| {Math.Round(reqCounter.OneSecondRate, 1),-14:#,#.#;;0} ");

                long allDocs = 0;
                foreach (var value in server.ServerStore.DatabasesLandlord.DatabasesCache.Values)
                {
                    if (value.Status != TaskStatus.RanToCompletion)
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
