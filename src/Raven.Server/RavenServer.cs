using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;

using Raven.Abstractions.Logging;
using Raven.Client.Data;
using Raven.Database.Util;
using Raven.Server.Config;
using Raven.Server.Documents.BulkInsert;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server
{
    public class RavenServer : IDisposable
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(RavenServer));

        public readonly RavenConfiguration Configuration;

        public ConcurrentDictionary<string, AccessToken> AccessTokensById = new ConcurrentDictionary<string, AccessToken>();
        public ConcurrentDictionary<string, AccessToken> AccessTokensByName = new ConcurrentDictionary<string, AccessToken>();

        public Timer Timer;

        public readonly ServerStore ServerStore;

        private IWebHost _webHost;
        private TcpListener _tcpListener;
        private readonly UnmanagedBuffersPool _unmanagedBuffersPool = new UnmanagedBuffersPool("TcpConnectionPool");
        public LoggerSetup LoggerSetup { get; }

        public RavenServer(RavenConfiguration configuration)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));

            Configuration = configuration;
            if (Configuration.Initialized == false)
                throw new InvalidOperationException("Configuration must be initialized");

            LoggerSetup = new LoggerSetup(Configuration.DebugLog.Path, Configuration.DebugLog.LogMode, Configuration.DebugLog.RetentionTime.AsTimeSpan);
            ServerStore = new ServerStore(Configuration, LoggerSetup);
            Metrics = new MetricsCountersManager(ServerStore.MetricsScheduler);
            Timer = new Timer(ServerMaintenanceTimerByMinute, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
        }

        private void ServerMaintenanceTimerByMinute(object state)
        {
            foreach (var accessToken in AccessTokensById.Values)
            {
                if (accessToken.IsExpired == false)
                    continue;

                AccessToken _;
                if (AccessTokensById.TryRemove(accessToken.Token, out _))
                {
                    AccessTokensByName.TryRemove(accessToken.Name, out _);
                }
            }
        }

        public void Initialize()
        {
            var sp = Stopwatch.StartNew();
            try
            {
                ServerStore.Initialize();
            }
            catch (Exception e)
            {
                Log.FatalException("Could not open the server store", e);
                throw;
            }

            if (Log.IsDebugEnabled)
            {
                Log.Debug("Server store started took {0:#,#;;0} ms", sp.ElapsedMilliseconds);
            }
            sp.Restart();

            Router = new RequestRouter(RouteScanner.Scan(), this);

            try
            {
                _webHost = new WebHostBuilder()
                    .CaptureStartupErrors(captureStartupErrors: true)
                    .UseKestrel(options => { })
                    .UseUrls(Configuration.Core.ServerUrl)
                    .UseStartup<RavenServerStartup>()
                    .ConfigureServices(services => services.AddSingleton(Router))
                    // ReSharper disable once AccessToDisposedClosure
                    .Build();
                _tcpListener = new TcpListener(IPAddress.Loopback, 9999);//TODO: Make this configurable based on server url
                Log.Info("Initialized Server...");
            }
            catch (Exception e)
            {
                Log.FatalException("Could not configure server", e);
                throw;
            }

            if (Log.IsDebugEnabled)
            {
                Log.Debug("Configuring HTTP server took {0:#,#;;0} ms", sp.ElapsedMilliseconds);
            }

            try
            {
                _webHost.Start();
                _tcpListener.Start();
                for (int i = 0; i < 4; i++)
                {
                    ListenToNewTcpConnection();
                }
            }
            catch (Exception e)
            {
                Log.FatalException("Could not start server", e);
                throw;
            }
        }

        private void ListenToNewTcpConnection()
        {
            Task.Run(async () =>
            {
                TcpClient tcpClient;
                try
                {
                    tcpClient = await _tcpListener.AcceptTcpClientAsync();
                }
                catch (Exception e)
                {
                    //TODO: logging
                    Console.WriteLine("Failed to accept tcp connection", e);
                    return;
                }
                ListenToNewTcpConnection();

                try
                {
                    tcpClient.NoDelay = true;
                    tcpClient.ReceiveBufferSize = 32 * 1024;
                    tcpClient.SendBufferSize = 4096;
                    using (var stream = tcpClient.GetStream())
                    using (var context = new JsonOperationContext(_unmanagedBuffersPool))
                    {
                        var reader = context.ReadForMemory(stream, "tcp command");
                        string db;
                        if (reader.TryGet("Database", out db) == false)
                        {
                            throw new InvalidOperationException("Could not read Database property from the tcp command");
                        }
                        var databasesLandlord = ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(db);
                        if (databasesLandlord == null)
                        {
                            throw new InvalidOperationException("There is no database named " + db);
                        }
                        var documentDatabase = databasesLandlord.Result;//TODO: should probably avoid doing that, at a minimum, have a timeout if this is the first request
                        using (var bulkInsert = new BulkInsertConnection(documentDatabase, context, stream))
                        {
                            bulkInsert.Execute();
                        }
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("Failed to process tcp connection", e);// todo: logging
                }
                finally
                {
                    try
                    {
                        tcpClient.Dispose();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e); // todo: logging
                    }
                }

            });
        }

        public RequestRouter Router { get; private set; }
        public MetricsCountersManager Metrics { get; private set; }

        public void Dispose()
        {
            Metrics?.Dispose();
            LoggerSetup?.Dispose();
            _webHost?.Dispose();
            _tcpListener?.Stop();
            ServerStore?.Dispose();
            Timer?.Dispose();
            _unmanagedBuffersPool?.Dispose();
        }
    }
}