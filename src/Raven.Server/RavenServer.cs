using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Client.Data;
using Raven.Database.Util;
using Raven.Server.Config;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
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
        private Task<TcpListener> _tcpListenerTask;
        private readonly UnmanagedBuffersPool _unmanagedBuffersPool = new UnmanagedBuffersPool("TcpConnectionPool");
        private readonly Logger _tcpLogger;
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

            _tcpLogger = LoggerSetup.GetLogger<RavenServer>("<TcpServer>");
        }

        public async Task<IPEndPoint> GetTcpServerPortAsync()
        {
            var tcpListener = await _tcpListenerTask;
            return ((IPEndPoint)tcpListener.LocalEndpoint);
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
                _tcpListenerTask = StartTcpListener();
            }
            catch (Exception e)
            {
                Log.FatalException("Could not start server", e);
                throw;
            }
        }

        private async Task<TcpListener> StartTcpListener()
        {
            if (_tcpLogger.IsInfoEnabled)
            {
                Log.Info($"Tcp Server will listen on {Configuration.Core.TcpServerUrl}");
            }

            var uri = new Uri(Configuration.Core.TcpServerUrl);

            var ipAddress = await GetTcpListenAddress(uri);

            var port = uri.IsDefaultPort ? 9090 : uri.Port;
            if (Log.IsDebugEnabled)
            {
                Log.Info($"Tcp Server will bind to {ipAddress} at {port}");
            }
            try
            {
                var listener = new TcpListener(ipAddress, port);
                listener.Start();
                for (int i = 0; i < 4; i++)
                {
                    ListenToNewTcpConnection();
                }
                return listener;
            }
            catch (Exception e)
            {
                if (_tcpLogger.IsOperationsEnabled)
                {
                    _tcpLogger.Operations(
                        $"Failed to start tcp server on {Configuration.Core.TcpServerUrl}, tcp listening disabled", e);
                }
                throw;
            }
        }

        private async Task<IPAddress> GetTcpListenAddress(Uri uri)
        {
            IPAddress ipAddress;

            if (IPAddress.TryParse(uri.DnsSafeHost, out ipAddress))
                return ipAddress;

            switch (uri.DnsSafeHost)
            {
                case "*":
                case "+":
                    return IPAddress.Any;
                case "localhost":
                    return IPAddress.Loopback;
                default:
                    try
                    {
                        var ipHostEntry = await Dns.GetHostEntryAsync(uri.DnsSafeHost);

                        if (ipHostEntry.AddressList.Length == 0)
                            throw new InvalidOperationException("The specified tcp server hostname has no entries: " +
                                                                uri.DnsSafeHost);
                        return ipHostEntry.AddressList[0]; // TODO: bind to all of the entries
                    }
                    catch (Exception e)
                    {
                        if (_tcpLogger.IsOperationsEnabled)
                        {
                            _tcpLogger.Operations(
                                $"Failed to resolve ip address to bind to for {Configuration.Core.TcpServerUrl}, tcp listening disabled",
                                e);
                        }
                        throw;
                    }
            }
        }

        private void ListenToNewTcpConnection()
        {
            Task.Run(async () =>
            {
                TcpClient tcpClient;
                try
                {
                    var tcpListener = await _tcpListenerTask;
                    tcpClient = await tcpListener.AcceptTcpClientAsync();
                }
                catch (ObjectDisposedException)
                {
                    // shutting down
                    return;
                }
                catch (Exception e)
                {
                    if (_tcpLogger.IsInfoEnabled)
                    {
                        _tcpLogger.Info("Failed to accept new tcp connection", e);
                    }
                    return;
                }
                ListenToNewTcpConnection();
                NetworkStream stream = null;
                JsonOperationContext context = null;
                JsonOperationContext.MultiDocumentParser multiDocumentParser = null;
                try
                {
                    tcpClient.NoDelay = true;
                    tcpClient.ReceiveBufferSize = 32 * 1024;
                    tcpClient.SendBufferSize = 4096;
                    stream = tcpClient.GetStream();
                    context = new JsonOperationContext(_unmanagedBuffersPool);
                    multiDocumentParser = context.ParseMultiFrom(stream);
                    try
                    {
                        var header = JsonDeserialization.TcpConnectionHeaderMessage(await multiDocumentParser.ParseToMemoryAsync());

                        var databaseLoadingTask = ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(header.DatabaseName);
                        if (databaseLoadingTask == null)
                            throw new InvalidOperationException("There is no database named " + header.DatabaseName);

                        if (await Task.WhenAny(databaseLoadingTask, Task.Delay(5000)) != databaseLoadingTask)
                            throw new InvalidOperationException(
                                $"Timeout when loading database {header.DatabaseName}, try again later");

                        var documentDatabase = await databaseLoadingTask;
                        switch (header.Operation)
                        {
                            case TcpConnectionHeaderMessage.OperationTypes.BulkInsert:
                                BulkInsertConnection.Run(documentDatabase, context, stream, tcpClient, multiDocumentParser);
                                break;
                            case TcpConnectionHeaderMessage.OperationTypes.Subscription:
                                SubscriptionConnection.SendSubscriptionDocuments(documentDatabase, context, stream, tcpClient, multiDocumentParser);
                                break;
                            case TcpConnectionHeaderMessage.OperationTypes.Replication:
                                documentDatabase.DocumentReplicationLoader.AcceptIncomingConnection(header, multiDocumentParser, stream);
                                break;
                            default:
                                throw new InvalidOperationException("Unknown operation for tcp " + header.Operation);
                        }

                        tcpClient = null; // the connection handler will dispose this, it is not its responsability
                        stream = null;
                        context = null;
                        multiDocumentParser = null;
                    }
                    catch (Exception e)
                    {
                        if (_tcpLogger.IsInfoEnabled)
                        {
                            _tcpLogger.Info("Failed to process TCP connection run", e);
                        }
                        if (context != null)
                        {
                            using (var errorWriter = new BlittableJsonTextWriter(context, stream))
                            {
                                context.Write(errorWriter, new DynamicJsonValue
                                {
                                    ["Type"] = "Error",
                                    ["Exception"] = e.ToString()
                                });
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    if (_tcpLogger.IsInfoEnabled)
                    {
                        _tcpLogger.Info("Failure when processing tcp connection", e);
                    }
                }
                finally
                {
                    try
                    {
                        multiDocumentParser?.Dispose();
                    }
                    catch (Exception)
                    {

                    }
                    try
                    {
                        stream?.Dispose();
                    }
                    catch (Exception)
                    {
                    }
                    try
                    {
                        tcpClient?.Dispose();
                    }
                    catch (Exception)
                    {
                    }
                    try
                    {
                        context?.Dispose();
                    }
                    catch (Exception)
                    {
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
            if (_tcpListenerTask != null)
            {
                if (_tcpListenerTask.IsCompleted)
                {
                    _tcpListenerTask.Result.Stop();
                }
                else
                {
                    var exception = _tcpListenerTask.Exception;
                    if (exception != null && _tcpLogger.IsInfoEnabled)
                    {
                        _tcpLogger.Info("Cannot dispose of tcp server because it has errored", exception);
                    }
                }
            }
            ServerStore?.Dispose();
            Timer?.Dispose();
            _unmanagedBuffersPool?.Dispose();
        }
    }
}