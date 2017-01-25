using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.Extensions.DependencyInjection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Replication;
using Raven.Client.Data;
using Raven.Client.Json;
using Raven.NewClient.Client.Exceptions.Database;
using Raven.Server.Alerts;
using Raven.Server.Commercial;
using Raven.Server.Config;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Exceptions;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.BackgroundTasks;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server
{
    public class RavenServer : IDisposable
    {
        private static Logger _logger;

        public readonly RavenConfiguration Configuration;

        public ConcurrentDictionary<string, AccessToken> AccessTokensById = new ConcurrentDictionary<string, AccessToken>();
        public ConcurrentDictionary<string, AccessToken> AccessTokensByName = new ConcurrentDictionary<string, AccessToken>();
        
        public Timer ServerMaintenanceTimer;

        public readonly ServerStore ServerStore;

        private IWebHost _webHost;
        private Task<TcpListenerStatus> _tcpListenerTask;
        private readonly Logger _tcpLogger;

        private readonly LatestVersionCheck _latestVersionCheck;

        public RavenServer(RavenConfiguration configuration)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));

            Configuration = configuration;
            if (Configuration.Initialized == false)
                throw new InvalidOperationException("Configuration must be initialized");

            ServerStore = new ServerStore(Configuration);
            Metrics = new MetricsCountersManager();
            ServerMaintenanceTimer = new Timer(ServerMaintenanceTimerByMinute, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));

            _logger = LoggingSource.Instance.GetLogger<RavenServer>("Raven/Server");
            _tcpLogger = LoggingSource.Instance.GetLogger<RavenServer>("<TcpServer>");

            _latestVersionCheck = new LatestVersionCheck(ServerStore);
        }

        public async Task<TcpListenerStatus> GetTcpServerStatusAsync()
        {
            return await _tcpListenerTask;
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
                if (_logger.IsOperationsEnabled)
                    _logger.Operations("Could not open the server store", e);
                throw;
            }

            if (_logger.IsInfoEnabled)
                _logger.Info(string.Format("Server store started took {0:#,#;;0} ms", sp.ElapsedMilliseconds));

            sp.Restart();

            Router = new RequestRouter(RouteScanner.Scan(), this);

            try
            {
                _webHost = new WebHostBuilder()
                    .CaptureStartupErrors(captureStartupErrors: true)
                    .UseKestrel(options =>
                    {
                        options.ShutdownTimeout = TimeSpan.FromSeconds(1);
                    })
                    .UseUrls(Configuration.Core.ServerUrl)
                    .UseStartup<RavenServerStartup>()
                    .ConfigureServices(services =>
                    {
                        services.AddSingleton(Router);
                        services.AddSingleton(this);
                        services.Configure<FormOptions>(options =>
                        {
                            options.MultipartBodyLengthLimit = long.MaxValue;
                        });
                    })
                    // ReSharper disable once AccessToDisposedClosure
                    .Build();
                if (_logger.IsInfoEnabled)
                    _logger.Info("Initialized Server...");
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Could not configure server", e);
                throw;
            }

            if (_logger.IsInfoEnabled)
                _logger.Info(string.Format("Configuring HTTP server took {0:#,#;;0} ms", sp.ElapsedMilliseconds));

            try
            {
                _webHost.Start();

                var serverAddressesFeature = _webHost.ServerFeatures.Get<IServerAddressesFeature>();
                WebUrls = serverAddressesFeature.Addresses.ToArray();

                _tcpListenerTask = StartTcpListener();
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations("Could not start server", e);
                throw;
            }

            try
            {
                _latestVersionCheck.Initialize();
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Could not setup latest version check.", e);
            }

            try
            {
                LicenseManager.Initialize();
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Could not setup license check.", e);

                ServerStore.Alerts.AddAlert(new Alert
                {
                    Type = AlertType.LicenseManagerInitializationError,
                    Key = nameof(AlertType.LicenseManagerInitializationError),
                    Severity = AlertSeverity.Info,
                    Content = new LicenseManager.InitializationErrorAlertContent(e),
                    Message = LicenseManager.InitializationErrorAlertContent.FormatMessage()
                });
            }
        }

        public string[] WebUrls { get; set; }

        private readonly JsonContextPool _tcpContextPool = new JsonContextPool();

        public class TcpListenerStatus
        {
            public readonly List<TcpListener> Listeners = new List<TcpListener>();
            public int Port;
        }

        private async Task<TcpListenerStatus> StartTcpListener()
        {
            string host = "<unknown>";
            var port = 0;
            var status = new TcpListenerStatus();
            try
            {
                host = new Uri(Configuration.Core.ServerUrl).DnsSafeHost;
                if (string.IsNullOrWhiteSpace(Configuration.Core.TcpServerUrl) == false)
                {
                    short shortPort;
                    if (short.TryParse(Configuration.Core.TcpServerUrl, out shortPort))
                    {
                        port = shortPort;
                    }
                    else
                    {
                        var uri = new Uri(Configuration.Core.TcpServerUrl);
                        host = uri.DnsSafeHost;
                        if (uri.IsDefaultPort == false)
                            port = uri.Port;
                    }
                }

                foreach (var ipAddress in await GetTcpListenAddresses(host))
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"RavenDB TCP is configured to use {Configuration.Core.TcpServerUrl} and bind to {ipAddress} at {port}");

                    var listener = new TcpListener(ipAddress, port);
                    status.Listeners.Add(listener);
                    listener.Server.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
                    try
                    {
                        listener.Start();
                    }
                    catch (Exception ex)
                    {
                        throw new IOException("Unable to start tcp listener on " + ipAddress + " on port " + port, ex);
                    }
                    var listenerLocalEndpoint = (IPEndPoint) listener.LocalEndpoint;
                    status.Port = listenerLocalEndpoint.Port;

                    for (int i = 0; i < 4; i++)
                    {
                        ListenToNewTcpConnection(listener);
                    }
                }
                return status;
            }
            catch (Exception e)
            {
                if (_tcpLogger.IsOperationsEnabled)
                {
                    _tcpLogger.Operations($"Failed to start tcp server on tcp://{host}:{port}, tcp listening disabled", e);
                }

                foreach (var tcpListener in status.Listeners)
                {
                    tcpListener.Stop();
                }
               
                throw;
            }
        }

        private async Task<IPAddress[]> GetTcpListenAddresses(string host)
        {
            IPAddress ipAddress;

            if (IPAddress.TryParse(host, out ipAddress))
                return new[] { ipAddress };

            switch (host)
            {
                case "*":
                case "+":
                    return new[] { IPAddress.Any };
                case "localhost":
                    return new[] { IPAddress.Loopback };
                default:
                    try
                    {
                        var ipHostEntry = await Dns.GetHostEntryAsync(host);

                        if (ipHostEntry.AddressList.Length == 0)
                            throw new InvalidOperationException("The specified tcp server hostname has no entries: " +
                                                                host);
                        return ipHostEntry.AddressList;
                    }
                    catch (Exception e)
                    {
                        if (_tcpLogger.IsOperationsEnabled)
                        {
                            _tcpLogger.Operations(
                                $"Failed to resolve ip address to bind to for {host}, tcp listening disabled",
                                e);
                        }
                        throw;
                    }
            }
        }
        
        private void ListenToNewTcpConnection(TcpListener listener)
        {
            Task.Run(async () =>
            {
                TcpClient tcpClient;
                try
                {
                    tcpClient = await listener.AcceptTcpClientAsync();
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
                ListenToNewTcpConnection(listener);
                TcpConnectionOptions tcp = null;
                try
                {
                    tcpClient.NoDelay = true;
                    tcpClient.ReceiveBufferSize = 32 * 1024;
                    tcpClient.SendBufferSize = 4096;
                    var stream = tcpClient.GetStream();
                    tcp = new TcpConnectionOptions
                    {
                        ContextPool = _tcpContextPool,
                        Stream = stream,
                        TcpClient = tcpClient,
                        PinnedBuffer = JsonOperationContext.ManagedPinnedBuffer.LongLivedInstance(),
                    };

                    try
                    {
                        TcpConnectionHeaderMessage header;
                        JsonOperationContext context;
                        using (_tcpContextPool.AllocateOperationContext(out context))
                        {
                            using (var headerJson = await context.ParseToMemoryAsync(
                                stream, 
                                "tcp-header",
                                BlittableJsonDocumentBuilder.UsageMode.None, 
                                tcp.PinnedBuffer
                                ))
                            {
                                header = JsonDeserializationClient.TcpConnectionHeaderMessage(headerJson);
                                if (_logger.IsInfoEnabled)
                                {
                                    _logger.Info($"New {header.Operation} TCP connection to {header.DatabaseName} from {tcpClient.Client.RemoteEndPoint}");
                                }
                            }
                        }

                        tcp.Operation = header.Operation;
                        var databaseLoadingTask = ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(header.DatabaseName);
                        if (databaseLoadingTask == null)
                        {
                            ThrowNoSuchDatabase(header);
                            return;// never hit
                        }

                        var databaseLoadTimeout = ServerStore.DatabasesLandlord.DatabaseLoadTimeout;

                        if (databaseLoadingTask.IsCompleted == false)
                        {
                            var resultingTask = await Task.WhenAny(databaseLoadingTask, Task.Delay(databaseLoadTimeout));
                            if (resultingTask != databaseLoadingTask)
                                ThrowTimeoutOnDatabaseLoad(header);
                        }

                        tcp.DocumentDatabase = await databaseLoadingTask;
                        tcp.DocumentDatabase.RunningTcpConnections.Add(tcp);

                        switch (header.Operation)
                        {
                            case TcpConnectionHeaderMessage.OperationTypes.BulkInsert:
                                BulkInsertConnection.Run(tcp);
                                break;
                            case TcpConnectionHeaderMessage.OperationTypes.Subscription:
                                SubscriptionConnection.SendSubscriptionDocuments(tcp);
                                break;
                            case TcpConnectionHeaderMessage.OperationTypes.Replication:
                                var documentReplicationLoader = tcp.DocumentDatabase.DocumentReplicationLoader;
                                documentReplicationLoader.AcceptIncomingConnection(tcp);
                                break;
                            case TcpConnectionHeaderMessage.OperationTypes.TopologyDiscovery:
                                var responder = new TopologyRequestHandler();
                                responder.AcceptIncomingConnectionAndRespond(tcp);
                                break;
                            default:
                                throw new InvalidOperationException("Unknown operation for tcp " + header.Operation);
                        }

                        //since the responsers to TCP connections mostly continue to run
                        //beyond this point, no sense to dispose the connection now, so set it to null.
                        //this way the responders are responsible to dispose the connection and the context                    
                        tcp = null;
                    }
                    catch (Exception e)
                    {
                        if (_tcpLogger.IsInfoEnabled)
                        {
                            _tcpLogger.Info("Failed to process TCP connection run", e);
                        }
                        if (tcp != null)
                        {
                            JsonOperationContext context;
                            using (_tcpContextPool.AllocateOperationContext(out context))
                            using (var errorWriter = new BlittableJsonTextWriter(context, tcp.Stream))
                            {
                                context.Write(errorWriter, new DynamicJsonValue
                                {
                                    ["Type"] = "Error",
                                    ["Exception"] = e.ToString(),
                                    ["Message"] = e.Message
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
                    tcp?.Dispose();
                }

            });
        }

        private static void ThrowTimeoutOnDatabaseLoad(TcpConnectionHeaderMessage header)
        {
            throw new DatabaseLoadTimeoutException($"Timeout when loading database {header.DatabaseName}, try again later");
        }

        private static void ThrowNoSuchDatabase(TcpConnectionHeaderMessage header)
        {
            throw new DatabaseDoesNotExistException("There is no database named " + header.DatabaseName);
        }

        public RequestRouter Router { get; private set; }
        public MetricsCountersManager Metrics { get; private set; }

        public bool Disposed { get; private set; }

        public void Dispose()
        {
            if (Disposed)
                return;
            Disposed = true;
            Metrics?.Dispose();
            _webHost?.Dispose();
            if (_tcpListenerTask != null)
            {
                if (_tcpListenerTask.IsCompleted)
                {
                    CloseTcpListeners(_tcpListenerTask.Result.Listeners);
                }
                else
                {
                    if (_tcpListenerTask.Exception != null)
                    {
                        if(_tcpLogger.IsInfoEnabled)
                            _tcpLogger.Info("Cannot dispose of tcp server because it has errored", _tcpListenerTask.Exception);
                    }
                    else
                    {
                        _tcpListenerTask.ContinueWith(t =>
                        {
                            CloseTcpListeners(t.Result.Listeners);
                        }, TaskContinuationOptions.OnlyOnRanToCompletion);
                    }
                }
            }

            ServerStore?.Dispose();
            ServerMaintenanceTimer?.Dispose();
            _latestVersionCheck?.Dispose();
        }

        private void CloseTcpListeners(List<TcpListener> listeners)
        {
            foreach (var tcpListener in listeners)
            {
                try
                {
                    tcpListener.Stop();
                }
                catch (Exception e)
                {
                    if (_tcpLogger.IsInfoEnabled)
                        _tcpLogger.Info("Failed to properly dispose the tcp listener", e);
                }
            }
            
        }
    }
}