using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.Loader;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Http;
using Raven.Client.Util;
using Raven.Debug.StackTrace;
using Raven.Server;
using Raven.Server.Commercial;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes.Static.NuGet;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.Restore;
using Raven.Server.Rachis;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Utils.Features;
using Sparrow.Collections;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Server;
using Sparrow.Server.Platform;
using Sparrow.Utils;
using Tests.Infrastructure;
using Tests.Infrastructure.Utils;
using Voron.Exceptions;
using Xunit.Abstractions;
using XunitLogger;

namespace FastTests
{
    public abstract class TestBase : ParallelTestBase
    {
        private static int _counter;

        private const string ServerName = "Raven.Tests.Core.Server";

        private static readonly ConcurrentSet<string> GlobalPathsToDelete = new(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentSet<string> _localPathsToDelete = new(StringComparer.OrdinalIgnoreCase);

        private static RavenServer _globalServer;

        protected static bool IsGlobalServer(RavenServer server)
        {
            return _globalServer == server;
        }

        private RavenServer _localServer;

        protected bool IsGlobalOrLocalServer(RavenServer server)
        {
            return _globalServer == server || _localServer == server;
        }

        protected List<RavenServer> Servers = new();

        private static readonly object ServerLocker = new();

        private bool _doNotReuseServer;

        private int _disposeTimeout = 60000;

        private IDictionary<string, string> _customServerSettings;

        private static readonly int ReservedPortStartRange = 23000;
        private static readonly int ReservedPortEndRange = 32000;
        private static int LastUsedReservedPort = ReservedPortStartRange;

        public static int GetReservedPort()
        {
            var port = Interlocked.Increment(ref LastUsedReservedPort);
            if (port > ReservedPortEndRange)
                throw new InvalidOperationException($"No more reserved ports left between [{ReservedPortStartRange} - {ReservedPortEndRange}], consider increase the range but be aware of ephemeral port range of each OS.");

            return port;
        }

        public static void IgnoreProcessorAffinityChanges(bool ignore)
        {
            LicenseManager.IgnoreProcessorAffinityChanges = ignore;
        }

        static unsafe TestBase()
        {
            IgnoreProcessorAffinityChanges(ignore: true);
            //RequestExecutor.HttpClientFactory = RavenServerHttpClientFactory.Instance;
            LicenseManager.AddLicenseStatusToLicenseLimitsException = true;
            RachisStateMachine.EnableDebugLongCommit = true;
            RavenServer.SkipCertificateDispose = true;

            NativeMemory.GetCurrentUnmanagedThreadId = () => (ulong)Pal.rvn_get_current_thread_id();
            ZstdLib.CreateDictionaryException = message => new VoronErrorException(message);

            Lucene.Net.Util.UnmanagedStringArray.Segment.AllocateMemory = NativeMemory.AllocateMemory;
            Lucene.Net.Util.UnmanagedStringArray.Segment.FreeMemory = NativeMemory.Free;

            BackupTask.DateTimeFormat = "yyyy-MM-dd-HH-mm-ss-fffffff";
            RestorePointsBase.BackupFolderRegex = new Regex(@"([0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{2}(-[0-9]{2}-[0-9]{7})?).ravendb-(.+)-([A-Za-z]+)-(.+)$", RegexOptions.Compiled);
            RestorePointsBase.FileNameRegex = new Regex(@"([0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{2}(-[0-9]{2}-[0-9]{7})?)", RegexOptions.Compiled);

            var packagesPath = new PathSetting(RavenTestHelper.NewDataPath("NuGetPackages", 0, forceCreateDir: true));
            GlobalPathsToDelete.Add(packagesPath.FullPath);
            MultiSourceNuGetFetcher.Instance.Initialize(packagesPath, "https://api.nuget.org/v3/index.json", allowPreleasePackages: true);

            IOExtensions.AfterGc += (s, x) =>
            {
                Console.WriteLine($"Execution of GC due to IO failure on path '{x.Path}' took {x.Duration} (attempt: {x.Attempt})");
            };

            LowMemoryNotification.Instance.SupportsCompactionOfLargeObjectHeap = true;

#if DEBUG2
            TaskScheduler.UnobservedTaskException += (sender, args) =>
            {
                if (args.Observed)
                    return;

                var e = args.Exception.ExtractSingleInnerException();

                var sb = new StringBuilder();
                sb.AppendLine("===== UNOBSERVED TASK EXCEPTION =====");
                sb.AppendLine(e.ExceptionToString(null));
                sb.AppendLine("=====================================");

                Console.WriteLine(sb.ToString());
            };
#endif
            if (PlatformDetails.RunningOnPosix == false &&
                PlatformDetails.Is32Bits) // RavenDB-13655
            {
                ThreadPool.SetMinThreads(25, 25);
                ThreadPool.SetMaxThreads(125, 125);
            }
            else
            {
                ThreadPool.SetMinThreads(250, 250);
            }

            RequestExecutor.RemoteCertificateValidationCallback += (sender, cert, chain, errors) => true;
        }

        protected TestBase(ITestOutputHelper output) : base(output)
        {
        }

        protected string GetDatabaseName([CallerMemberName] string caller = null)
        {
            if (caller != null && caller.Contains(".ctor"))
                throw new InvalidOperationException(
                    $"{nameof(GetDatabaseName)} was invoked from within {GetType().Name} constructor. This is an indication that you're trying to generate" +
                    " a database within a test class constructor. This is forbidden because this database will be generated but the test won't run until" +
                    $" it gets the semaphore at {nameof(InitializeAsync)} also the constructor is invoked per test method and it is not shared between tests" +
                    " so there is no value in generating the database from the constructor.");

            var name = caller != null ? $"{caller}_{Interlocked.Increment(ref _counter)}" : Guid.NewGuid().ToString("N");
            return name;
        }

        public void DoNotReuseServer(IDictionary<string, string> customSettings = null)
        {
            _customServerSettings = customSettings;
            _doNotReuseServer = true;
        }

        protected string GetTempFileName()
        {
            var tmp = Path.GetTempFileName();

            _localPathsToDelete.Add(tmp);

            return tmp;
        }

        public async Task<DocumentDatabase> GetDatabase(string databaseName)
        {
            return await GetDatabase(Server, databaseName);
        }

        protected static async Task<DocumentDatabase> GetDatabase(RavenServer ravenServer, string databaseName)
        {
            var database = await ravenServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName).ConfigureAwait(false);
            if (database == null)
            {
                // Throw and get more info why database is null
                using (ravenServer.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                {
                    context.OpenReadTransaction();
                    var lastCommit = ravenServer.ServerStore.Engine.GetLastCommitIndex(context);
                    var doc = ravenServer.ServerStore.Cluster.Read(context, "db/" + databaseName.ToLowerInvariant());
                    throw new InvalidOperationException("For " + databaseName + ". Database is null and database record is: " + (doc == null ? "null" : doc.ToString()) +
                                                        " Last commit: " + lastCommit);
                }
            }

            return database;
        }

        public RavenServer Server
        {
            get
            {
                if (_localServer != null)
                {
                    if (_localServer.Disposed)
                        throw new ObjectDisposedException("Someone disposed the local server!");

                    return _localServer;
                }

                if (_doNotReuseServer)
                {
                    UseNewLocalServer();
                    _doNotReuseServer = false;

                    return _localServer;
                }

                if (_globalServer != null)
                {
                    if (_globalServer.Disposed)
                        throw new ObjectDisposedException("Someone disposed the global server!");
                    _localServer = _globalServer;

                    return _localServer;
                }
                lock (ServerLocker)
                {
                    if (_globalServer == null || _globalServer.Disposed)
                    {
                        var globalServer = GetNewServer(new ServerCreationOptions { RegisterForDisposal = false }, "Global");
                        using (var currentProcess = Process.GetCurrentProcess())
                        {
                            Console.WriteLine(
                                $"\tTo attach debugger to test process ({(PlatformDetails.Is32Bits ? "x86" : "x64")}), use proc-id: {currentProcess.Id}. Url {globalServer.WebUrl}");
                        }

                        AssemblyLoadContext.Default.Unloading += UnloadServer;
                        _globalServer = globalServer;
                    }
                    _localServer = _globalServer;
                }
                return _globalServer;
            }
        }

        private static void CheckServerLeak()
        {
            foreach (var leakedServer in LeakedServers)
            {
                if (leakedServer.Key.Disposed)
                    continue;

                Console.WriteLine($"[ Leak!! ] The test {leakedServer.Value} leaks a server.");
            }
        }

        private static void UnloadServer(AssemblyLoadContext obj)
        {
            try
            {
                lock (ServerLocker)
                {
                    var copyGlobalServer = _globalServer;
                    _globalServer = null;
                    if (copyGlobalServer == null)
                        return;

                    try
                    {
                        using (copyGlobalServer.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            var databases = copyGlobalServer
                                .ServerStore
                                .Cluster
                                .ItemsStartingWith(context, Constants.Documents.Prefix, 0, int.MaxValue)
                                .ToList();

                            if (databases.Count > 0)
                            {
                                var sb = new StringBuilder();
                                sb.AppendLine("List of non-deleted databases:");

                                foreach (var t in databases)
                                {
                                    var databaseName = t.ItemName.Substring(Constants.Documents.Prefix.Length);

                                    try
                                    {
                                        AsyncHelpers.RunSync(() => copyGlobalServer.ServerStore.DeleteDatabaseAsync(databaseName, hardDelete: true, null, Guid.NewGuid().ToString()));
                                    }
                                    catch (Exception)
                                    {
                                        // ignored
                                    }

                                    sb
                                        .Append("- ")
                                        .AppendLine(databaseName);
                                }

                                Console.WriteLine(sb.ToString());
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"Could not retrieve list of non-deleted databases. Exception: {e}");
                    }

                    DisposeServer(copyGlobalServer);

                    GC.Collect(2);
                    GC.WaitForPendingFinalizers();

                    var exceptionAggregator = new ExceptionAggregator("Failed to cleanup test databases");

                    RavenTestHelper.DeletePaths(GlobalPathsToDelete, exceptionAggregator);

                    exceptionAggregator.ThrowIfNeeded();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
            finally
            {
                CheckServerLeak();
            }
        }

        public void UseNewLocalServer(IDictionary<string, string> customSettings = null, bool? runInMemory = null, string customConfigPath = null, [CallerMemberName] string caller = null)
        {
            if (_localServer != _globalServer && _globalServer != null)
            {
                DisposeServer(_localServer, _disposeTimeout);
                _localServer = null;
            }

            var co = new ServerCreationOptions
            {
                CustomSettings = customSettings ?? _customServerSettings,
                RunInMemory = runInMemory,
                CustomConfigPath = customConfigPath,
                RegisterForDisposal = false
            };
            _localServer = GetNewServer(co, caller);
        }

        private readonly object _getNewServerSync = new();
        protected List<RavenServer> ServersForDisposal = new();

        public class ServerCreationOptions
        {
            private IDictionary<string, string> _customSettings;

            public IDictionary<string, string> CustomSettings
            {
                get => _customSettings;
                set
                {
                    AssertNotFrozen();
                    _customSettings = value;
                }
            }

            private bool _deletePrevious = true;

            public bool DeletePrevious
            {
                get => _deletePrevious;
                set
                {
                    AssertNotFrozen();
                    _deletePrevious = value;
                }
            }

            private bool? _runInMemory = null;

            public bool? RunInMemory
            {
                get => _runInMemory;
                set
                {
                    AssertNotFrozen();
                    _runInMemory = value;
                }
            }

            private string _dataDirectory;

            public string DataDirectory
            {
                get => _dataDirectory;
                set
                {
                    AssertNotFrozen();
                    _dataDirectory = value;
                }
            }

            private string _customConfigPath;

            public string CustomConfigPath
            {
                get => _customConfigPath;
                set
                {
                    AssertNotFrozen();
                    _customConfigPath = value;
                }
            }

            private bool _registerForDisposal = true;

            public bool RegisterForDisposal
            {
                get => _registerForDisposal;
                set
                {
                    AssertNotFrozen();
                    _registerForDisposal = value;
                }
            }

            private string _nodeTag;

            public string NodeTag
            {
                get => _nodeTag;
                set
                {
                    AssertNotFrozen();
                    _nodeTag = value;
                }
            }

            private readonly bool _frozen;

            private void AssertNotFrozen()
            {
                if (_frozen)
                    throw new InvalidOperationException("ServerCreationOptions are frozen and cannot be changed.");
            }

            public ServerCreationOptions(bool frozen = false)
            {
                _frozen = frozen;
            }

            private static readonly Lazy<ServerCreationOptions> _default = new(() => new ServerCreationOptions(frozen: true));
            public static ServerCreationOptions Default => _default.Value;

            public Action<ServerStore> BeforeDatabasesStartup;
        }

        private static readonly ConcurrentDictionary<RavenServer, string> LeakedServers = new();

        protected virtual RavenServer GetNewServer(ServerCreationOptions options = null, [CallerMemberName] string caller = null)
        {
            if (options == null)
            {
                options = ServerCreationOptions.Default;
            }

            lock (_getNewServerSync)
            {
                var configuration = RavenConfiguration.CreateForServer(Guid.NewGuid().ToString(), options.CustomConfigPath);

                configuration.SetSetting(RavenConfiguration.GetKey(x => x.Replication.ReplicationMinimalHeartbeat), "1");
                configuration.SetSetting(RavenConfiguration.GetKey(x => x.Replication.RetryReplicateAfter), "3");
                configuration.SetSetting(RavenConfiguration.GetKey(x => x.Replication.RetryMaxTimeout), "3");
                configuration.SetSetting(RavenConfiguration.GetKey(x => x.Cluster.AddReplicaTimeout), "10");

                if (options.CustomSettings != null)
                {
                    foreach (var setting in options.CustomSettings)
                        configuration.SetSetting(setting.Key, setting.Value);
                }

                var hasServerUrls = options.CustomSettings != null && options.CustomSettings.ContainsKey(RavenConfiguration.GetKey(x => x.Core.ServerUrls));
                var hasDataDirectory = options.CustomSettings != null && options.CustomSettings.ContainsKey(RavenConfiguration.GetKey(x => x.Core.DataDirectory));
                var hasFeaturesAvailability = options.CustomSettings != null && options.CustomSettings.ContainsKey(RavenConfiguration.GetKey(x => x.Core.FeaturesAvailability));
                var hasRunInMemory = options.CustomSettings != null && options.CustomSettings.ContainsKey(RavenConfiguration.GetKey(x => x.Core.RunInMemory));

                configuration.Initialize();

                configuration.Logs.Mode = LogMode.None;
                configuration.Server.Name = ServerName;
                configuration.Server.MaxTimeForTaskToWaitForDatabaseToLoad = new TimeSetting(60, TimeUnit.Seconds);
                configuration.Licensing.EulaAccepted = true;

                if (hasRunInMemory == false)
                    configuration.Core.RunInMemory = options.RunInMemory ?? true;
                else if (options.RunInMemory != null)
                    ThrowOnDuplicateConfiguration(nameof(ServerCreationOptions.RunInMemory));

                if (hasServerUrls == false)
                    configuration.Core.ServerUrls = new[] { "http://127.0.0.1:0" };

                if (hasDataDirectory == false)
                {
                    string dataDirectory = null;
                    if (options.DataDirectory == null)
                        dataDirectory = NewDataPath(prefix: $"GetNewServer-{options.NodeTag}", forceCreateDir: true);
                    else
                    {
                        if (Path.IsPathRooted(options.DataDirectory) == false)
                            throw new InvalidOperationException($"{nameof(ServerCreationOptions)}.{nameof(ServerCreationOptions.DataDirectory)} path needs to be rooted. Was: {options.DataDirectory}");

                        dataDirectory = options.DataDirectory;
                    }

                    configuration.Core.DataDirectory = configuration.Core.DataDirectory.Combine(dataDirectory);
                }
                else if (options.DataDirectory != null)
                    ThrowOnDuplicateConfiguration(nameof(ServerCreationOptions.DataDirectory));

                if (hasFeaturesAvailability == false)
                    configuration.Core.FeaturesAvailability = FeaturesAvailability.Experimental;

                if (options.DeletePrevious)
                    IOExtensions.DeleteDirectory(configuration.Core.DataDirectory.FullPath);

                var server = new RavenServer(configuration)
                {
                    ThrowOnLicenseActivationFailure = true,
                    DebugTag = caller
                };

                try
                {
                    if (options.BeforeDatabasesStartup != null)
                        server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().BeforeHandleClusterDatabaseChanged = options.BeforeDatabasesStartup;

                    server.Initialize();
                    server.ServerStore.ValidateFixedPort = false;

                    if (options.RegisterForDisposal)
                        ServersForDisposal.Add(server);

                    if (LeakedServers.TryAdd(server, server.DebugTag))
                        server.AfterDisposal += () => LeakedServers.TryRemove(server, out _);

                    return server;
                }
                catch
                {
                    server.Dispose();
                    throw;
                } 
            }
        }

        private void ThrowOnDuplicateConfiguration(string config)
        {
            throw new InvalidOperationException($"You cannot set {config} in both, {nameof(ServerCreationOptions)}.{nameof(ServerCreationOptions.CustomSettings)} and {nameof(ServerCreationOptions)}.{config}");
        }

        protected static string UseFiddlerUrl(string url)
        {
            if (Debugger.IsAttached && Process.GetProcessesByName("fiddler").Any())
                url = url.Replace("127.0.0.1", "localhost.fiddler");

            return url;
        }

        protected static string[] UseFiddler(string url)
        {
            if (Debugger.IsAttached && Process.GetProcessesByName("fiddler").Any())
                url = url.Replace("127.0.0.1", "localhost.fiddler");

            return new[] { url };
        }

        protected static void OpenBrowser(string url)
        {
            Console.WriteLine(url);

            if (PlatformDetails.RunningOnPosix == false)
            {
                Process.Start(new ProcessStartInfo("cmd", $"/c start \"Stop & look at studio\" \"{url}\""));
                return;
            }

            if (PlatformDetails.RunningOnMacOsx)
            {
                Process.Start("open", url);
                return;
            }

            Process.Start("xdg-open", url);
        }

        protected string NewDataPath([CallerMemberName] string prefix = null, string suffix = null, bool forceCreateDir = false)
        {
            if (suffix != null)
                prefix += suffix;
            var path = RavenTestHelper.NewDataPath(prefix, 0, forceCreateDir);

            GlobalPathsToDelete.Add(path);
            _localPathsToDelete.Add(path);

            return path;
        }

        protected abstract void Dispose(ExceptionAggregator exceptionAggregator);

        public override void Dispose()
        {
            GC.SuppressFinalize(this);

            base.Dispose();

            var exceptionAggregator = new ExceptionAggregator("Could not dispose test");

            var testOutcomeAnalyzer = new TestOutcomeAnalyzer(Context);
            var shouldSaveDebugPackage = testOutcomeAnalyzer.ShouldSaveDebugPackage();

            exceptionAggregator.Execute(() =>
            {
                if (_globalServer?.ServerStore.Observer?.Suspended == true)
                    throw new InvalidOperationException("The observer is suspended for the global server!");
            });

            Dispose(exceptionAggregator);

            DownloadAndSaveDebugPackage(shouldSaveDebugPackage, _globalServer, exceptionAggregator, Context);

            if (_localServer != null && _localServer != _globalServer)
            {
                DownloadAndSaveDebugPackage(shouldSaveDebugPackage, _localServer, exceptionAggregator, Context);

                exceptionAggregator.Execute(() =>
                {
                    DisposeServer(_localServer, _disposeTimeout);
                    _localServer = null;
                });
            }

            for (int i = 0; i < ServersForDisposal.Count; i++)
            {
                var serverForDisposal = ServersForDisposal[i];

                if (i == 0)
                    DownloadAndSaveDebugPackage(shouldSaveDebugPackage, serverForDisposal, exceptionAggregator, Context);

                exceptionAggregator.Execute(() => DisposeServer(serverForDisposal, _disposeTimeout));
            }

            ServersForDisposal = null;

            RavenTestHelper.DeletePaths(_localPathsToDelete, exceptionAggregator);

            exceptionAggregator.ThrowIfNeeded();
        }

        private static void DownloadAndSaveDebugPackage(bool shouldSaveDebugPackage, RavenServer server, ExceptionAggregator exceptionAggregator, Context context)
        {
            if (shouldSaveDebugPackage == false)
                return;

            if (server == null || server.Disposed)
                return;

            exceptionAggregator.Execute(() => DebugPackageHandler.DownloadAndSave(server, context));
        }

        internal void SetServerDisposeTimeout(int timeout)
        {
            _disposeTimeout = timeout;
        }

        protected static void DisposeServer(RavenServer server, int timeoutInMs = 60_000)
        {
            AsyncHelpers.RunSync(() => DisposeServerAsync(server, timeoutInMs));
        }

        protected static async Task DisposeServerAsync(RavenServer server, int timeoutInMs = 60_000)
        {
            if (server == null)
                return;

            if (server.Disposed)
                return;

            var url = server.WebUrl;
            var debugTag = server.DebugTag;
            var timeout = TimeSpan.FromMilliseconds(timeoutInMs);

            using (await DebugHelper.GatherVerboseDatabaseDisposeInformationAsync(server, timeoutInMs))
            using (var mre = new AsyncManualResetEvent())
            {
                server.AfterDisposal += () => mre.Set();
                var task = Task.Run(server.Dispose);

                if (await mre.WaitAsync(timeout) == false)
                    await ThrowCouldNotDisposeServerExceptionAsync(url, debugTag, timeout);

                await task;
            }
        }

        private static async Task ThrowCouldNotDisposeServerExceptionAsync(string url, string debugTag, TimeSpan timeout)
        {
            using (var process = Process.GetCurrentProcess())
            using (var ms = new MemoryStream())
            using (var outputWriter = new StreamWriter(ms, leaveOpen: true))
            {
                var sb = new StringBuilder($"Could not dispose server with URL '{url}' and DebugTag: '{debugTag}' in '{timeout}'.");

                try
                {
                    await StackTracer.ShowStackTrace(process.Id, 1500, outputPath: null, outputWriter, threadIds: null);
                    ms.Position = 0;

                    using (var outputReader = new StreamReader(ms, leaveOpen: true))
                    {
                        var stackTraces = outputReader.ReadToEnd();
                        var tempPath = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid()}.stacks.json");

                        File.WriteAllText(tempPath, stackTraces);
                        Console.WriteLine(stackTraces);

                        sb.Append($" StackTraces available at: '{tempPath}'");
                    }
                }
                catch (Exception e)
                {
                    sb.Append($" Failed to retrieve StackTraces: {e}");
                }

                throw new InvalidOperationException(sb.ToString());
            }
        }
    }
}
