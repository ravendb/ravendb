using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Runtime.Loader;
using System.Threading;
using System.Threading.Tasks;
using McMaster.Extensions.CommandLineUtils;
using Microsoft.AspNetCore.Connections;
using Raven.Client.Http;
using Raven.Server.Commercial;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Indexes.Static.NuGet;
using Raven.Server.EventListener;
using Raven.Server.Logging;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.BackgroundTasks;
using Raven.Server.TrafficWatch;
using Raven.Server.Utils;
using Raven.Server.Utils.Cli;
using Sparrow;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Server.Logging;
using Sparrow.Server.Platform;
using Sparrow.Utils;
using Voron;
using Voron.Exceptions;
using Voron.Impl;
using NativeMemory = Sparrow.Utils.NativeMemory;

namespace Raven.Server
{
    public sealed class Program
    {
        private static readonly RavenLogger Logger;

        static Program()
        {
            RavenLogManager.Set(RavenNLogLogManager.Instance);
            Logger = RavenLogManager.Instance.GetLoggerForServer<Program>();
        }

        public static unsafe int Main(string[] args)
        {
            bool useLegacyHttpClientFactory = false;
            var useLegacyHttpClientFactoryAsString = Environment.GetEnvironmentVariable("RAVEN_HTTP_USELEGACYHTTPCLIENTFACTORY");
            if (useLegacyHttpClientFactoryAsString != null && bool.TryParse(useLegacyHttpClientFactoryAsString, out useLegacyHttpClientFactory) == false)
                throw new InvalidOperationException($"Could not parse 'RAVEN_HTTP_USELEGACYHTTPCLIENTFACTORY' env variable with value '{useLegacyHttpClientFactoryAsString}'.");

            RequestExecutor.HttpClientFactory = useLegacyHttpClientFactory
                ? DefaultRavenHttpClientFactory.Instance
                : RavenServerHttpClientFactory.Instance;

            NativeMemory.GetCurrentUnmanagedThreadId = () => (ulong)Pal.rvn_get_current_thread_id();
            ZstdLib.CreateDictionaryException = message => new VoronErrorException(message);

            Lucene.Net.Util.UnmanagedStringArray.Segment.AllocateMemory = NativeMemory.AllocateMemoryByLucene;
            Lucene.Net.Util.UnmanagedStringArray.Segment.FreeMemory = NativeMemory.FreeMemoryByLucene;

            UseOnlyInvariantCultureInRavenDB();

            SetCurrentDirectoryToServerPath();

            LowMemoryNotification.Instance.SupportsCompactionOfLargeObjectHeap = true;

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

            if (CommandLineSwitches.PrintInfoAndExit)
            {
                Console.WriteLine($"{nameof(ServerVersion.Version)}: {ServerVersion.Version}");
                Console.WriteLine($"{nameof(ServerVersion.FullVersion)}: {ServerVersion.FullVersion}");
                Console.WriteLine($"{nameof(ServerVersion.AssemblyVersion)}: {ServerVersion.AssemblyVersion}");
                Console.WriteLine($"{nameof(ServerVersion.Build)}: {ServerVersion.Build}");
                Console.WriteLine($"{nameof(ServerVersion.CommitHash)}: {ServerVersion.CommitHash}");
                Console.WriteLine($"{nameof(ServerVersion.ReleaseDate)}: {ServerVersion.ReleaseDate}");
                Console.WriteLine($"Framework: {RuntimeInformation.FrameworkDescription}");
                Console.WriteLine($"Runtime: {RuntimeInformation.RuntimeIdentifier}");
                Console.WriteLine($"Architecture: {RuntimeInformation.ProcessArchitecture}");
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

            GlobalFlushingBehavior.NumberOfConcurrentSyncs = configuration.Storage.NumberOfConcurrentSyncsPerPhysicalDrive;

            EncryptionBuffersPool.Instance.Disabled = configuration.Storage.DisableEncryptionBuffersPooling;

            MultiSourceNuGetFetcher.ForIndexes.Initialize(configuration.Indexing.NuGetPackagesPath, configuration.Indexing.NuGetPackageSourceUrl, configuration.Indexing.NuGetAllowPreReleasePackages);
            MultiSourceNuGetFetcher.ForLogging.Initialize(configuration.Logs.NuGetPackagesPath, configuration.Logs.NuGetPackageSourceUrl, configuration.Logs.NuGetAllowPreReleasePackages);

            RavenLogManager.Instance.ConfigureLogging(configuration);

            TrafficWatchToLog.Instance.UpdateConfiguration(configuration.TrafficWatch);
            EventListenerToLog.Instance.UpdateConfiguration(new EventListenerToLog.EventListenerConfiguration
            {
                EventListenerMode = configuration.DebugConfiguration.EventListenerMode,
                EventTypes = configuration.DebugConfiguration.EventTypes,
                MinimumDurationInMs = configuration.DebugConfiguration.MinimumDuration.GetValue(TimeUnit.Milliseconds),
                AllocationsLoggingIntervalInMs = configuration.DebugConfiguration.AllocationsLoggingInterval.GetValue(TimeUnit.Milliseconds),
                AllocationsLoggingCount = configuration.DebugConfiguration.AllocationsLoggingCount
            });

            InitializeThreadPoolThreads(configuration);

            LatestVersionCheck.Instance.Initialize(configuration.Updates);

            if (Logger.IsInfoEnabled)
                Logger.Info(RavenCli.GetInfoText());

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
                RestartServerMre.Set();
                ShutdownServerMre.Set();
            };

            var rerun = false;
            RavenConfiguration configBeforeRestart = configuration;
            do
            {
                if (rerun)
                {
                    Console.WriteLine("\nRestarting Server...");

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

                            configuration.UpdateLicenseType(server.ServerStore.LicenseManager.LicenseStatus.Type);

                            if (CommandLineSwitches.PrintServerId)
                                Console.WriteLine($"Server ID is {server.ServerStore.GetServerId()}.");

                            new RuntimeSettings(Console.Out).Print();

                            if (rerun == false && CommandLineSwitches.LaunchBrowser)
                                BrowserHelper.OpenStudioInBrowser(server.ServerStore.GetNodeHttpServerUrl());

                            new ClusterMessage(Console.Out, server.ServerStore.Engine.Tag, server.ServerStore.Engine.ClusterId).Print();

                            var prevColor = Console.ForegroundColor;
                            Console.Write("Server available on: ");
                            Console.ForegroundColor = ConsoleColor.Green;
                            Console.WriteLine($"{server.ServerStore.GetNodeHttpServerUrl()}");
                            Console.ForegroundColor = prevColor;

                            var tcpServerStatus = server.GetTcpServerStatus();
                            if (tcpServerStatus.Listeners.Count > 0)
                            {
                                prevColor = Console.ForegroundColor;
                                Console.Write("Tcp listening on ");
                                Console.ForegroundColor = ConsoleColor.Green;
                                Console.WriteLine($"{string.Join(", ", tcpServerStatus.Listeners.Select(l => l.LocalEndpoint))}");
                                Console.ForegroundColor = prevColor;
                            }

                            Console.WriteLine("Server started, listening to requests...");

                            prevColor = Console.ForegroundColor;
                            Console.ForegroundColor = ConsoleColor.DarkGray;
                            Console.WriteLine("TIP: type 'help' to list the available commands.");
                            Console.ForegroundColor = prevColor;

                            if (configuration.Storage.IgnoreInvalidJournalErrors == true)
                            {
                                var message =
                                    $"Server is running in dangerous mode because {RavenConfiguration.GetKey(x => x.Storage.IgnoreInvalidJournalErrors)} was set. " +
                                    "It means that storages of databases, indexes and system one will be loaded regardless missing or corrupted journal files which " +
                                    "are mandatory to properly load the storage. " +
                                    "This switch is meant to be use only for recovery purposes. Please make sure that you won't use it on regular basis. ";

                                if (Logger.IsWarnEnabled)
                                    Logger.Warn(message);

                                prevColor = Console.ForegroundColor;
                                Console.ForegroundColor = ConsoleColor.Red;
                                Console.WriteLine(message);
                                Console.ForegroundColor = prevColor;
                            }

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
                                    "For more information go to https://ravendb.net/l/EJS81M/7.0";
                            }
                            else if (e is SocketException && PlatformDetails.RunningOnPosix)
                            {
                                string urls;
                                try
                                {
                                    var web = server.Configuration.Core?.ServerUrls ?? Array.Empty<string>();
                                    var tcp = server.Configuration.Core.TcpServerUrls ?? Array.Empty<string>();
                                    urls = string.Join(", ", web.Concat(tcp));
                                }
                                catch (Exception eUrl)
                                {
                                    urls = "Unable to figure our which URL is used because: " + eUrl; // should never happen
                                }
                                message =
                                    $"{Environment.NewLine}In Linux low-level port (below 1024) will need a special permission, " +
                                    $"if this is your case please run{Environment.NewLine}" +
                                    $"sudo setcap CAP_NET_BIND_SERVICE=+eip {Path.Combine(AppContext.BaseDirectory, "Raven.Server")}{Environment.NewLine}" + 
                                    $"Urls: [{urls}]";
                            }
                            else if (e.InnerException is LicenseExpiredException)
                            {
                                message = e.InnerException.Message;
                            }

                            if (Logger.IsFatalEnabled)
                            {
                                Logger.Fatal("Failed to initialize the server", e);
                                Logger.Fatal(message);
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
                    if (Logger.IsInfoEnabled)
                    {
                        Logger.Info("Server has shut down");
                        Thread.Sleep(3000);
                    }

                    ShutdownCompleteMre.Set();
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
                if (Logger.IsErrorEnabled)
                    Logger.Error(msg, exception);
                Console.WriteLine($"{msg} Exception: {exception}");
            }
        }

        private static void InitializeThreadPoolThreads(RavenConfiguration configuration)
        {
            ThreadPool.GetMinThreads(out var workerThreads, out var completionPortThreads);

            int effectiveMinWorkerThreads = configuration.Server.ThreadPoolMinWorkerThreads ?? 2 * workerThreads;
            int effectiveMinCompletionPortThreads = configuration.Server.ThreadPoolMinCompletionPortThreads ?? 2 * completionPortThreads;

            ThreadPool.SetMinThreads(effectiveMinWorkerThreads, effectiveMinCompletionPortThreads);

            if ((configuration.Server.ThreadPoolMinWorkerThreads != null || configuration.Server.ThreadPoolMinCompletionPortThreads != null) && Logger.IsInfoEnabled)
                Logger.Info($"Thread Pool configuration was modified by calling {nameof(ThreadPool.SetMinThreads)}. Current values: workerThreads - {effectiveMinWorkerThreads}, completionPortThreads - {effectiveMinCompletionPortThreads}.");


            if (configuration.Server.ThreadPoolMaxWorkerThreads != null || configuration.Server.ThreadPoolMaxCompletionPortThreads != null)
            {
                ThreadPool.GetMaxThreads(out workerThreads, out completionPortThreads);

                int effectiveMaxWorkerThreads = configuration.Server.ThreadPoolMaxWorkerThreads ?? workerThreads;
                int effectiveMaxCompletionPortThreads = configuration.Server.ThreadPoolMaxCompletionPortThreads ?? completionPortThreads;

                ThreadPool.SetMaxThreads(effectiveMaxWorkerThreads, effectiveMaxCompletionPortThreads);

                if (Logger.IsInfoEnabled)
                    Logger.Info($"Thread Pool configuration was modified by calling {nameof(ThreadPool.SetMaxThreads)}. Current values: workerThreads - {effectiveMaxWorkerThreads}, completionPortThreads - {effectiveMaxCompletionPortThreads}.");

            }
        }

        public static ManualResetEvent ShutdownServerMre = new ManualResetEvent(false);
        public static ManualResetEvent RestartServerMre = new ManualResetEvent(false);
        public static ManualResetEvent ShutdownCompleteMre = new ManualResetEvent(false);
        public static Action RestartServer;

        public static bool IsRunningNonInteractive;

        public static bool RunAsNonInteractive()
        {
            IsRunningNonInteractive = true;

            if (Logger.IsInfoEnabled)
                Logger.Info("Server is running as non-interactive.");

            Console.WriteLine("Running non-interactive.");

            if (CommandLineSwitches.LogToConsole)
                RavenConsoleTarget.Enable();

            AssemblyLoadContext.Default.Unloading += s =>
            {
                RavenConsoleTarget.Disable();
                if (ShutdownServerMre.WaitOne(0))
                    return; // already done
                Console.WriteLine("Received graceful exit request...");
                ShutdownServerMre.Set();
                ShutdownCompleteMre.WaitOne(TimeSpan.FromSeconds(30)); // on linux we have to keep Unloading event exit last
            };

            ShutdownServerMre.WaitOne();
            if (RestartServerMre.WaitOne(0))
            {
                RestartServerMre.Reset();
                ShutdownServerMre.Reset();
                return true;
            }
            return false;
        }

        private static bool RunInteractive(RavenServer server)
        {
            //stop dumping logs
            RavenConsoleTarget.Disable();

            AssemblyLoadContext.Default.Unloading += s =>
            {
                if (IsRunningNonInteractive)
                    return;

                RavenConsoleTarget.Disable();
                if (ShutdownServerMre.WaitOne(0))
                    return; // already done
                Console.WriteLine();
                Console.WriteLine("Received graceful exit request (interactive mode)...");
                // On interactive mode, Console.ReadLine holds the application from closing and disposing.
                // We are about to force dispose here RavenServer, although running inside 'using' statement
                try
                {
                    if (Logger.IsInfoEnabled)
                    {
                        Logger.Info("Server is about to shut down (interactive mode)");
                        Thread.Sleep(3000);
                    }

                    server.Dispose();
                    Console.WriteLine("Shutdown completed (interactive mode)");
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error during shutdown (interactive mode)");
                    Console.WriteLine(e);
                }
                // Environment.Exit will cause halt here
            };

            bool consoleColoring = true;
            if (server.Configuration.Embedded.ParentProcessId.HasValue)
            {
                //When opening an embedded server we must disable console coloring to avoid exceptions,
                //due to the fact, we redirect standard input from the console.
                consoleColoring = false;
            }

            return new RavenCli().Start(server, Console.Out, Console.In, consoleColoring, false);
        }

        public static void WriteServerStatsAndWaitForEsc(RavenServer server)
        {
            var workingSetText = PlatformDetails.RunningOnPosix == false ? "working set" : "    RSS    ";
            Console.WriteLine("Showing stats, press any key to close...");
            Console.WriteLine($"    {workingSetText}     | native mem      | managed mem     | mmap size          | scratch dirty  | reqs/sec       | docs (all dbs)");
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
                Console.Write($" | {stats.TotalScratchDirty,-14} ");
                Console.Write($"| {Math.Round(reqCounter.OneSecondRate, 1),-14:#,#.#;;0} ");

                long allDocs = 0;
                foreach (var kvp in server.ServerStore.DatabasesLandlord.DatabasesCache)
                {
                    var task = kvp.Value;
                    if (task.Status != TaskStatus.RanToCompletion)
                        continue;

                    try
                    {
                        allDocs += task.Result.DocumentsStorage.GetNumberOfDocuments();
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
                    .Where(x => x.CpuUsage >= cpuUsageThreshold))
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
