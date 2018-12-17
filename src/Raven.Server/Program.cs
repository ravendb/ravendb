using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Runtime.Loader;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Connections;
using Microsoft.Extensions.CommandLineUtils;
using Raven.Server.Commercial;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Raven.Server.Utils.Cli;
using Sparrow.Logging;
using Sparrow.Platform;

namespace Raven.Server
{
    public class Program
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<Program>("Server");

        public static int Main(string[] args)
        {
            UseOnlyInvariantCultureInRavenDB();

            SetCurrentDirectoryToServerPath();

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

            new WelcomeMessage(Console.Out).Print();

            var targetSettingsFile = new PathSetting(string.IsNullOrEmpty(CommandLineSwitches.CustomConfigPath)
                ? "settings.json"
                : CommandLineSwitches.CustomConfigPath);

            var destinationSettingsFile = new PathSetting("settings.default.json");

            if (File.Exists(targetSettingsFile.FullPath) == false &&
                File.Exists(destinationSettingsFile.FullPath)) //just in case
            {
                File.Copy(destinationSettingsFile.FullPath, targetSettingsFile.FullPath);
            }

            var configuration = RavenConfiguration.CreateForServer(null, CommandLineSwitches.CustomConfigPath);

            if (configurationArgs != null)
                configuration.AddCommandLine(configurationArgs);

            configuration.Initialize();

            LoggingSource.Instance.SetupLogMode(configuration.Logs.Mode, configuration.Logs.Path.FullPath);
            LoggingSource.UseUtcTime = configuration.Logs.UseUtcTime;

            if (Logger.IsInfoEnabled)
                Logger.Info($"Logging to {configuration.Logs.Path} set to {configuration.Logs.Mode} level.");

            if (Logger.IsOperationsEnabled)
                Logger.Operations(RavenCli.GetInfoText());

            if (WindowsServiceRunner.ShouldRunAsWindowsService())
            {
                try
                {
                    WindowsServiceRunner.Run(CommandLineSwitches.ServiceName, configuration, configurationArgs);
                }
                catch (Exception e)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info("Error running Windows Service", e);

                    return 1;
                }

                return 0;
            }

            RestartServer = () =>
            {
                ResetServerMre.Set();
                ShutdownServerMre.Set();
            };

            var rerun = false;
            RavenConfiguration configBeforeRestart = configuration;
            do
            {
                if (rerun)
                {
                    Console.WriteLine("\nRestarting Server...");
                    rerun = false;

                    configuration = RavenConfiguration.CreateForServer(null, CommandLineSwitches.CustomConfigPath);

                    if (configurationArgs != null)
                    {
                        var argsAfterRestart = PostSetupCliArgumentsUpdater.Process(
                            configurationArgs, configBeforeRestart, configuration);

                        configuration.AddCommandLine(argsAfterRestart);
                        configBeforeRestart = configuration;
                    }

                    configuration.Initialize();
                }

                try
                {
                    using (var server = new RavenServer(configuration))
                    {
                        try
                        {
                            try
                            {
                                server.OpenPipes();
                            }
                            catch (Exception e)
                            {
                                if (Logger.IsInfoEnabled)
                                    Logger.Info("Unable to OpenPipe. Admin Channel will not be available to the user", e);
                                Console.WriteLine("Warning: Admin Channel is not available:" + e);
                            }

                            server.Initialize();

                            if (CommandLineSwitches.PrintServerId)
                                Console.WriteLine($"Server ID is {server.ServerStore.GetServerId()}.");

                            new RuntimeSettings(Console.Out).Print();

                            if (CommandLineSwitches.LaunchBrowser)
                                BrowserHelper.OpenStudioInBrowser(server.ServerStore.GetNodeHttpServerUrl());

                            new ClusterMessage(Console.Out, server.ServerStore).Print();

                            var prevColor = Console.ForegroundColor;
                            Console.Write("Server available on: ");
                            Console.ForegroundColor = ConsoleColor.Green;
                            Console.WriteLine($"{server.ServerStore.GetNodeHttpServerUrl()}");
                            Console.ForegroundColor = prevColor;

                            var tcpServerStatus = server.GetTcpServerStatus();
                            prevColor = Console.ForegroundColor;
                            Console.Write("Tcp listening on ");
                            Console.ForegroundColor = ConsoleColor.Green;
                            Console.WriteLine($"{string.Join(", ", tcpServerStatus.Listeners.Select(l => l.LocalEndpoint))}");
                            Console.ForegroundColor = prevColor;

                            Console.WriteLine("Server started, listening to requests...");

                            prevColor = Console.ForegroundColor;
                            Console.ForegroundColor = ConsoleColor.DarkGray;
                            Console.WriteLine("TIP: type 'help' to list the available commands.");
                            Console.ForegroundColor = prevColor;

                            IsRunningNonInteractive = false;
                            rerun = CommandLineSwitches.NonInteractive ||
                                    configuration.Core.SetupMode == SetupMode.Initial
                                ? RunAsNonInteractive()
                                : RunInteractive(server);

                            Console.WriteLine("Starting shut down...");
                            if (Logger.IsInfoEnabled)
                                Logger.Info("Server is shutting down");
                        }
                        catch (Exception e)
                        {
                            string message = null;
                            if (e.InnerException is AddressInUseException)
                            {
                                message =
                                    $"{Environment.NewLine}Port might be already in use.{Environment.NewLine}Try running with an unused port.{Environment.NewLine}" +
                                    $"You can change the port using one of the following options:{Environment.NewLine}" +
                                    $"1) Change the ServerUrl property in setting.json file.{Environment.NewLine}" +
                                    $"2) Run the server from the command line with --ServerUrl option.{Environment.NewLine}" +
                                    $"3) Add RAVEN_ServerUrl to the Environment Variables.{Environment.NewLine}" +
                                    "For more information go to https://ravendb.net/l/EJS81M/4.1";
                            }
                            else if (e is SocketException && PlatformDetails.RunningOnPosix)
                            {
                                var ravenPath = typeof(RavenServer).Assembly.Location;
                                if (ravenPath.EndsWith(".dll", StringComparison.OrdinalIgnoreCase))
                                    ravenPath = ravenPath.Substring(ravenPath.Length - 4);

                                message =
                                    $"{Environment.NewLine}In Linux low-level port (below 1024) will need a special permission, " +
                                    $"if this is your case please run{Environment.NewLine}" +
                                    $"sudo setcap CAP_NET_BIND_SERVICE=+eip {ravenPath}";
                            }

                            if (Logger.IsOperationsEnabled)
                            {
                                Logger.Operations("Failed to initialize the server", e);
                                Logger.Operations(message);
                            }

                            Console.WriteLine(message);

                            Console.WriteLine();

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
                finally
                {
                    if (Logger.IsOperationsEnabled)
                        Logger.OperationsAsync("Server has shut down").Wait(TimeSpan.FromSeconds(15));
                }
            } while (rerun);

            return 0;
        }

        private static void UseOnlyInvariantCultureInRavenDB()
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.InvariantCulture;
            Thread.CurrentThread.CurrentUICulture = CultureInfo.InvariantCulture;
            CultureInfo.DefaultThreadCurrentCulture = CultureInfo.InvariantCulture;
            CultureInfo.DefaultThreadCurrentUICulture = CultureInfo.InvariantCulture;
        }

        private static void SetCurrentDirectoryToServerPath()
        {
            try
            {
                Directory.SetCurrentDirectory(AppContext.BaseDirectory);
            }
            catch (Exception exception)
            {
                var msg = $"Error setting current directory: {AppContext.BaseDirectory}.";
                Logger.Operations(msg, exception);
                Console.WriteLine($"{msg} Exception: {exception}");
            }
        }

        public static ManualResetEvent ShutdownServerMre = new ManualResetEvent(false);
        public static ManualResetEvent ResetServerMre = new ManualResetEvent(false);
        public static Action RestartServer;

        public static bool IsRunningNonInteractive;

        public static bool RunAsNonInteractive()
        {
            IsRunningNonInteractive = true;

            if (Logger.IsInfoEnabled)
                Logger.Info("Server is running as non-interactive.");

            Console.WriteLine("Running non-interactive.");

            if (CommandLineSwitches.LogToConsole)
                LoggingSource.Instance.EnableConsoleLogging();

            AssemblyLoadContext.Default.Unloading += s =>
            {
                LoggingSource.Instance.DisableConsoleLogging();
                if (ShutdownServerMre.WaitOne(0))
                    return; // already done
                Console.WriteLine("Received graceful exit request...");
                ShutdownServerMre.Set();
            };

            ShutdownServerMre.WaitOne();
            if (ResetServerMre.WaitOne(0))
            {
                ResetServerMre.Reset();
                ShutdownServerMre.Reset();
                return true;
            }
            return false;
        }

        private static bool RunInteractive(RavenServer server)
        {
            //stop dumping logs
            LoggingSource.Instance.DisableConsoleLogging();

            bool consoleColoring = true;
            if (server.Configuration.Embedded.ParentProcessId.HasValue)
                //When opening an embedded server we must disable console coloring to avoid exceptions,
                //due to the fact, we redirect standard input from the console.
                consoleColoring = false;

            return new RavenCli().Start(server, Console.Out, Console.In, consoleColoring);
        }

        public static void WriteServerStatsAndWaitForEsc(RavenServer server)
        {
            var workingSetText = PlatformDetails.RunningOnPosix == false ? "working set" : "    RSS    ";
            Console.WriteLine("Showing stats, press any key to close...");
            Console.WriteLine($"    {workingSetText}     | native mem      | managed mem     | mmap size         | reqs/sec       | docs (all dbs)");
            var i = 0;
            while (Console.KeyAvailable == false)
            {
                var stats = RavenCli.MemoryStatsWithMemoryMappedInfo();
                var reqCounter = server.Metrics.Requests.RequestsPerSec;

                Console.Write($"\r {((i++ % 2) == 0 ? "*" : "+")} ");

                Console.Write($" {stats.WorkingSet,-14} ");
                Console.Write($" | {stats.TotalUnmanagedAllocations,-14} ");
                Console.Write($" | {stats.ManagedMemory,-14} ");
                Console.Write($" | {stats.TotalMemoryMapped,-17} ");

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
            Console.WriteLine($"Stats halted.");
        }

        public static void WriteThreadsInfoAndWaitForEsc(RavenServer server, int maxTopThreads, int updateIntervalInMs, double cpuUsageThreshold)
        {
            Console.WriteLine("Showing threads info, press any key to close...");
            var i = 0L;
            var threadsUsage = new ThreadsUsage();
            Thread.Sleep(100);

            var cursorLeft = Console.CursorLeft;
            var cursorTop = Console.CursorTop;

            var waitIntervals = updateIntervalInMs / 100;
            var maxNameLength = 0;
            while (Console.KeyAvailable == false)
            {
                Console.SetCursorPosition(cursorLeft, cursorTop);

                var threadsInfo = threadsUsage.Calculate();
                Console.Write($"{(i++ % 2 == 0 ? "*" : "+")} ");
                Console.WriteLine($"CPU usage: {threadsInfo.CpuUsage:0.00}% (total threads: {threadsInfo.List.Count:#,#0}, active cores: {threadsInfo.ActiveCores})   ");
                var printedLines = 1;

                var count = 0;
                var isFirst = true;
                foreach (var threadInfo in threadsInfo.List
                    .Where(x => x.CpuUsage >= cpuUsageThreshold)
                    .OrderByDescending(x => x.CpuUsage))
                {
                    if (isFirst)
                    {
                        printedLines++;
                        Console.WriteLine("  thread id  |  cpu usage  |   priority    |     thread name       ");
                        isFirst = false;
                    }

                    if (++count > maxTopThreads)
                        break;

                    var nameLength = threadInfo.Name.Length;
                    maxNameLength = Math.Max(maxNameLength, nameLength);
                    var numberOfEmptySpaces = maxNameLength - nameLength + 1;
                    var emptySpaces = numberOfEmptySpaces > 0 ? new string(' ', numberOfEmptySpaces) : string.Empty;

                    Console.Write($"    {threadInfo.Id,-7} ");
                    Console.Write($" |    {$"{threadInfo.CpuUsage:0.00}%",-8}");
                    Console.Write($" | {threadInfo.Priority,-12} ");
                    Console.Write($" | {threadInfo.Name}{emptySpaces}");

                    Console.WriteLine();
                    printedLines++;
                }

                for (var j = 0; j < waitIntervals && Console.KeyAvailable == false; j++)
                {
                    Thread.Sleep(100);
                }

                if (PlatformDetails.RunningOnPosix)
                {
                    var newTop = Console.BufferHeight - cursorTop - printedLines - 1;
                    if (newTop < 0)
                    {
                        cursorTop = Math.Max(0, cursorTop + newTop);
                    }
                }
            }

            Console.ReadKey(true);
            Console.WriteLine();
            Console.WriteLine("Threads info halted.");
        }
    }
}
