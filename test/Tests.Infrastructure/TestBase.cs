using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Loader;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Collections;
using Sparrow.Logging;

namespace FastTests
{
    public abstract class TestBase : LinuxRaceConditionWorkAround, IDisposable
    {
        public const string ServerName = "Raven.Tests.Core.Server";

        private static readonly ConcurrentSet<string> PathsToDelete = new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

        private static RavenServer _globalServer;
        private RavenServer _localServer;

        private static readonly object ServerLocker = new object();

        private bool _doNotReuseServer;
        public void DoNotReuseServer() => _doNotReuseServer = true;

        private static int _serverCounter;

        public Task<DocumentDatabase> GetDatabase(string databaseName)
        {
            return Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
        }

        public RavenServer Server
        {
            get
            {
                if (_localServer != null)
                    return _localServer;

                if (_doNotReuseServer)
                {
                    _localServer = GetNewServer();
                    return _localServer;
                }

                if (_globalServer != null)
                {
                    _localServer = _globalServer;
                    return _localServer;
                }
                lock (ServerLocker)
                {
                    if (_globalServer == null)
                    {
                        Console.WriteLine("\tTo attach debugger to test process, use process id: {0}", Process.GetCurrentProcess().Id);
                        var globalServer = GetNewServer();
                        AssemblyLoadContext.Default.Unloading += context =>
                        {
                            globalServer.Dispose();

                            GC.Collect(2);
                            GC.WaitForPendingFinalizers();

                            var exceptionAggregator = new ExceptionAggregator("Failed to cleanup test databases");

                            RavenTestHelper.DeletePaths(PathsToDelete, exceptionAggregator);

                            exceptionAggregator.ThrowIfNeeded();
                        };
                        _globalServer = globalServer;
                    }
                    _localServer = _globalServer;
                }
                return _globalServer;
            }
        }

        protected static RavenServer GetNewServer()
        {
            var configuration = new RavenConfiguration(null, ResourceType.Server);
            configuration.Initialize();
            configuration.DebugLog.LogMode = LogMode.None;
            configuration.Core.ServerUrl = "http://127.0.0.1:0";
            configuration.Server.Name = ServerName;
            configuration.Core.RunInMemory = true;
            configuration.Core.DataDirectory = configuration.Core.DataDirectory.Combine($"Tests{Interlocked.Increment(ref _serverCounter)}");
            configuration.Server.MaxTimeForTaskToWaitForDatabaseToLoad = new TimeSetting(60, TimeUnit.Seconds);
            configuration.Storage.AllowOn32Bits = true;

            IOExtensions.DeleteDirectory(configuration.Core.DataDirectory.FullPath);

            var server = new RavenServer(configuration);
            server.Initialize();

            // TODO: Make sure to properly handle this when this is resolved:
            // TODO: https://github.com/dotnet/corefx/issues/5205
            // TODO: AssemblyLoadContext.GetLoadContext(typeof(RavenTestBase).GetTypeInfo().Assembly).Unloading +=

            return server;
        }

        protected static string UseFiddler(string url)
        {
            if (Debugger.IsAttached && Process.GetProcessesByName("fiddler").Any())
                return url.Replace("127.0.0.1", "localhost.fiddler");

            return url;
        }

        protected static void OpenBrowser(string url)
        {
            Console.WriteLine(url);

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                Process.Start(new ProcessStartInfo("cmd", $"/c start \"Stop & look at studio\" \"{url}\"")); // Works ok on windows
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                Process.Start("xdg-open", url); // Works ok on linux
            }
            else
            {
                Console.WriteLine("Do it yourself!");
            }
        }

        protected string NewDataPath([CallerMemberName] string prefix = null, string suffix = null, bool forceCreateDir = false)
        {
            if (suffix != null)
                prefix += suffix;
            var path = RavenTestHelper.NewDataPath(prefix, _serverCounter, forceCreateDir);

            PathsToDelete.Add(path);
            return path;
        }

        protected abstract void Dispose(ExceptionAggregator exceptionAggregator);

        public virtual void Dispose()
        {
            GC.SuppressFinalize(this);

            var exceptionAggregator = new ExceptionAggregator("Could not dispose test");

            Dispose(exceptionAggregator);

            if (_localServer != null)
            {
                if (_doNotReuseServer)
                {
                    exceptionAggregator.Execute(() =>
                    {
                        _localServer.Dispose();
                        _localServer = null;
                    });
                }

                exceptionAggregator.ThrowIfNeeded();
            }
        }
    }
}