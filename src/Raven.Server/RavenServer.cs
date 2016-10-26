using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Client.Data;
using Raven.Client.Json;
using Raven.Database.Util;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Collections;
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
        
        public Timer Timer;

        public readonly ServerStore ServerStore;

        private IWebHost _webHost;
        private Task<List<TcpListener>> _tcpListenerTask;
        private readonly Logger _tcpLogger;

        public RavenServer(RavenConfiguration configuration)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));

            Configuration = configuration;
            if (Configuration.Initialized == false)
                throw new InvalidOperationException("Configuration must be initialized");

            ServerStore = new ServerStore(Configuration);
            Metrics = new MetricsCountersManager();
            Timer = new Timer(ServerMaintenanceTimerByMinute, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
            _logger = LoggingSource.Instance.GetLogger<RavenServer>("Raven/Server");
            _tcpLogger = LoggingSource.Instance.GetLogger<RavenServer>("<TcpServer>");
        }

        public async Task<int> GetTcpServerPortAsync()
        {
            var tcpListeners = await _tcpListenerTask;
            return ((IPEndPoint)tcpListeners[0].LocalEndpoint).Port;
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
                        options.ThreadCount = Environment.ProcessorCount;
                    })
                    .UseUrls(Configuration.Core.ServerUrl)
                    .UseStartup<RavenServerStartup>()
                    .ConfigureServices(services =>
                    {
                        services.AddSingleton(Router);
                        services.AddSingleton(this);
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
                _tcpListenerTask = StartTcpListener();
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations("Could not start server", e);
                throw;
            }
        }

        private JsonContextPool _tcpContextPool = new JsonContextPool();
        private async Task<List<TcpListener>> StartTcpListener()
        {
            var listeners = new List<TcpListener>();
            try
            {
                var uri = new Uri(Configuration.Core.TcpServerUrl);
                var port = uri.IsDefaultPort ? 9090 : uri.Port;
                foreach (var ipAddress in await GetTcpListenAddresses(uri))
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"RavenDB TCP is configured to use {Configuration.Core.TcpServerUrl} and bind to {ipAddress} at {port}");

                    var listener = new TcpListener(ipAddress, port);
                    listeners.Add(listener);
                    listener.Start();
                    for (int i = 0; i < 4; i++)
                    {
                        ListenToNewTcpConnection(listener);
                    }
                }
                return listeners;
            }
            catch (Exception e)
            {
                foreach (var tcpListener in listeners)
                {
                    tcpListener.Stop();
                }
                if (_tcpLogger.IsOperationsEnabled)
                {
                    _tcpLogger.Operations(
                        $"Failed to start tcp server on {Configuration.Core.TcpServerUrl}, tcp listening disabled", e);
                }
                throw;
            }
        }


        private async Task<IPAddress[]> GetTcpListenAddresses(Uri uri)
        {
            IPAddress ipAddress;

            if (IPAddress.TryParse(uri.DnsSafeHost, out ipAddress))
                return new[] { ipAddress };

            switch (uri.DnsSafeHost)
            {
                case "*":
                case "+":
                    return new[] { IPAddress.Any };
                case "localhost":
                    return new[] { IPAddress.Loopback };
                default:
                    try
                    {
                        var ipHostEntry = await Dns.GetHostEntryAsync(uri.DnsSafeHost);

                        if (ipHostEntry.AddressList.Length == 0)
                            throw new InvalidOperationException("The specified tcp server hostname has no entries: " +
                                                                uri.DnsSafeHost);
                        return ipHostEntry.AddressList;
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
                    tcp = new TcpConnectionOptions()
                    {
                        Stream = stream,
                        TcpClient = tcpClient,
                        DisposeOnConnectionClose =
                        {
                            stream,
                            tcpClient
                        }
                    };
                    tcp.DisposeOnConnectionClose.Add(
                        _tcpContextPool.AllocateOperationContext(out tcp.Context)
                        );


                    tcp.MultiDocumentParser = tcp.Context.ParseMultiFrom(stream);

                    try
                    {
                        var header = JsonDeserializationClient.TcpConnectionHeaderMessage(await tcp.MultiDocumentParser.ParseToMemoryAsync());
                        tcp.Operation = header.Operation;
                        var databaseLoadingTask = ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(header.DatabaseName);
                        if (databaseLoadingTask == null)
                            throw new InvalidOperationException("There is no database named " + header.DatabaseName);

                        if (await Task.WhenAny(databaseLoadingTask, Task.Delay(5000)) != databaseLoadingTask)
                            throw new InvalidOperationException(
                                $"Timeout when loading database {header.DatabaseName}, try again later");

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
                            default:
                                throw new InvalidOperationException("Unknown operation for tcp " + header.Operation);
                        }

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
                            using (var errorWriter = new BlittableJsonTextWriter(tcp.Context, tcp.Stream))
                            {
                                tcp.Context.Write(errorWriter, new DynamicJsonValue
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
                    tcp?.Dispose();
                }

            });
        }

        public RequestRouter Router { get; private set; }
        public MetricsCountersManager Metrics { get; private set; }

        private bool _disposed;

        public void Dispose()
        {
            if (_disposed)
                return;
            _disposed = true;
            Metrics?.Dispose();
            _webHost?.Dispose();
            if (_tcpListenerTask != null)
            {
                if (_tcpListenerTask.IsCompleted)
                {
                    CloseTcpListeners(_tcpListenerTask.Result);
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
                            CloseTcpListeners(t.Result);
                        }, TaskContinuationOptions.OnlyOnRanToCompletion);
                    }
                }
            }
            ServerStore?.Dispose();
            Timer?.Dispose();
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