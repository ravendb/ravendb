using System;
using System.IO;
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

            new WelcomeMessage(Console.Out).Print();

            var configuration = new RavenConfiguration(null, ResourceType.Server, CommandLineSwitches.CustomConfigPath);

            if (configurationArgs != null)
                configuration.AddCommandLine(configurationArgs);

            configuration.Initialize();

            LoggingSource.Instance.SetupLogMode(configuration.Logs.Mode, Path.Combine(AppContext.BaseDirectory, configuration.Logs.Path));

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
                            server.OpenPipe(); // TODO: although server should dispose the pipe - it is not.. after resetserver
                            server.Initialize();
                            
                            if (CommandLineSwitches.PrintServerId)
                                Console.WriteLine($"Server ID is {server.ServerStore.GetServerId()}.");

                            if (CommandLineSwitches.LaunchBrowser)
                                BrowserHelper.OpenStudioInBrowser(server.ServerStore.NodeHttpServerUrl);

                            Console.WriteLine($"Server available on: {server.ServerStore.NodeHttpServerUrl}");

                            server.GetTcpServerStatusAsync()
                                .ContinueWith(tcp =>
                                {
                                    if (tcp.IsCompleted)
                                    {
                                        Console.WriteLine($"Tcp listening on {string.Join(", ", tcp.Result.Listeners.Select(l => l.LocalEndpoint))}");
                                    }
                                    else
                                    {
                                        Console.Error.WriteLine($"Tcp listen failure (see {server.ServerStore.NodeHttpServerUrl}/info/tcp for details) {tcp.Exception.Message}");
                                    }
                                });
                            Console.WriteLine("Server started, listening to requests...");

                            IsRunningAsService = false;
                            rerun = CommandLineSwitches.Daemon ? RunAsService() : RunInteractive(server);

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

        public static ManualResetEvent QuitServerMre = new ManualResetEvent(false);
        public static ManualResetEvent ResetServerMre = new ManualResetEvent(false);

        public static bool IsRunningAsService;

        public static bool RunAsService()
        {
            IsRunningAsService = true;

            if (Logger.IsInfoEnabled)
                Logger.Info("Server is running as a service");
            Console.WriteLine("Running as Service");

            AssemblyLoadContext.Default.Unloading += (s) =>
            {
                Console.WriteLine("Received graceful exit request...");
                QuitServerMre.Set();
            };

            QuitServerMre.WaitOne();
            if (ResetServerMre.WaitOne(0))
            {
                ResetServerMre.Reset();
                QuitServerMre.Reset();
                return true;
            }
            return false;
        }

        private static bool RunInteractive(RavenServer server)
        {
            var configuration = server.Configuration;

            //stop dumping logs
            LoggingSource.Instance.DisableConsoleLogging();
            LoggingSource.Instance.SetupLogMode(LogMode.None,
                Path.Combine(AppContext.BaseDirectory, configuration.Logs.Path));


            return new RavenCli().Start(server, Console.Out, Console.In, true);
        }

        public static void WriteServerStatsAndWaitForEsc(RavenServer server)
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
