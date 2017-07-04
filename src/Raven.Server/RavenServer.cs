using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Pipes;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Server.Kestrel;
using Microsoft.Extensions.DependencyInjection;
using Raven.Client.Exceptions.Database;
using Raven.Client.Json.Converters;
using Raven.Client.Server.Tcp;
using Raven.Server.Config;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Categories;
using Raven.Server.Documents;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Json;
using Raven.Server.Rachis;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Maintenance;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using AccessModes = Raven.Client.Server.Operations.ApiKeys.AccessModes;
using AccessToken = Raven.Server.Web.Authentication.AccessToken;
using System.Reflection;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Platform;

namespace Raven.Server
{
    public class RavenServer : IDisposable
    {
        static RavenServer()
        {
            //TODO: When this method become available, update to call directly
            var setMinThreads = (Func<int, int, bool>)typeof(ThreadPool).GetTypeInfo().GetMethod("SetMinThreads")
             .CreateDelegate(typeof(Func<int, int, bool>));

            setMinThreads(250, 250);
        }

        private static readonly Logger _logger = LoggingSource.Instance.GetLogger<RavenServer>("Raven/Server");

        public readonly RavenConfiguration Configuration;

        public ConcurrentDictionary<string, AccessToken> AccessTokenCache = new ConcurrentDictionary<string, AccessToken>();

        public Timer ServerMaintenanceTimer;

        public readonly ServerStore ServerStore;

        private IWebHost _webHost;
        private Task<TcpListenerStatus> _tcpListenerTask;
        private readonly Logger _tcpLogger;

        public event Action AfterDisposal;

        public RavenServer(RavenConfiguration configuration)
        {
            JsonDeserializationValidator.Validate();

            Configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));

            if (Configuration.Initialized == false)
                throw new InvalidOperationException("Configuration must be initialized");

            ServerStore = new ServerStore(Configuration, this);
            Metrics = new MetricsCountersManager();
            ServerMaintenanceTimer = new Timer(ServerMaintenanceTimerByMinute, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));

            _tcpLogger = LoggingSource.Instance.GetLogger<RavenServer>("<TcpServer>");
        }
        
        public Task<TcpListenerStatus> GetTcpServerStatusAsync()
        {
            return _tcpListenerTask;
        }

        private void ServerMaintenanceTimerByMinute(object state)
        {
            foreach (var accessToken in AccessTokenCache.Values)
            {
                if (accessToken.IsExpired == false)
                    continue;

                AccessTokenCache.TryRemove(accessToken.Token, out var _);
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
            //We start the server pipe from program.cs so we won't generate one per server in our test environment 
            if (Pipe != null)
            {
                Task.Factory.StartNew(ListenToPipe);
            }
            Router = new RequestRouter(RouteScanner.Scan(), this);

            try
            {
                Action<KestrelServerOptions> kestrelOptions = options => options.ShutdownTimeout = TimeSpan.FromSeconds(1);

                if (Configuration.Security.CertificatePath != null)
                {
                    ServerCertificate = LoadCertificate(Configuration.Security);
                    
                    kestrelOptions += options => options.UseHttps(ServerCertificate.Certificate);
                    
                    // Enforce https in all network activities
                    if (Configuration.Core.ServerUrl.StartsWith("http:", StringComparison.OrdinalIgnoreCase))
                        throw new InvalidOperationException($"When the `{RavenConfiguration.GetKey(x => x.Security.CertificatePath)}` is specified, the `{RavenConfiguration.GetKey(x => x.Core.ServerUrl)}` must be using https, but was " + Configuration.Core.ServerUrl);
                }

                _webHost = new WebHostBuilder()
                    .CaptureStartupErrors(captureStartupErrors: true)
                    .UseKestrel(kestrelOptions)
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

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Initialized Server... {string.Join(", ", WebUrls)}");
                
                _tcpListenerTask = StartTcpListener();
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations("Could not start server", e);
                throw;
            }
        }

        internal async Task ListenToPipe()
        {
            lock (this)
            {
                while (true)
                {
                    try
                    {
                        await Pipe.WaitForConnectionAsync();
                        using (var reader = new StreamReader(Pipe))
                        using (var writer = new StreamWriter(Pipe))
                        {
                            try
                            {
                                var msg = reader.ReadLine();
                                var tokens = msg.Split(new[] {' '}, StringSplitOptions.RemoveEmptyEntries);

                                if (tokens.Length < 1)
                                {
                                    var reply = "Expected 'trust' or 'init' but got nothing";
                                    PipeLogAndReply(writer, reply);
                                    continue;
                                }

                                switch (tokens[0])
                                {
                                    case "trust":
                                        if (tokens.Length < 3)
                                        {
                                            msg = $"Expected 'trust' followed by public key and tag but didn't get both of them";
                                            PipeLogAndReply(writer, msg);
                                            continue;
                                        }
                                        var k = $"Raven/Sign/Public/{tokens[2]}";
                                        ServerStore.PutSecretKey(tokens[1], k, true);
                                        writer.WriteLine($"Server {tokens[2]} public key {tokens[1]} was installed successfully");
                                        writer.Flush();
                                        break;
                                    case "init":
                                        var (api_key, pub_key) = await ServerStore.GetApiKeyAndPublicKey();
                                        writer.WriteLine($"{api_key} public key={pub_key}");
                                        writer.Flush();
                                        break;
                                    default:
                                        var reply = $"Provided command {tokens[0]} isn't supported";
                                        PipeLogAndReply(writer, reply);
                                        continue;

                                }
                            }
                            catch (Exception e)
                            {
                                var msg = "Got an exception trying to communicate through the pipe";
                                if (_logger.IsInfoEnabled)
                                {
                                    _logger.Info(msg, e);
                                }
                                PipeLogAndReply(writer, $"{msg}{Environment.NewLine}{e}");
                                continue;
                            }
                        }

                    }
                    catch (ObjectDisposedException)
                    {
                        //Server shutting down
                        return;
                    }
                    catch (Exception e)
                    {
                        if (_logger.IsInfoEnabled)
                        {
                            _logger.Info("Got an exception trying to connect to server pipe", e);
                        }
                    }
                    try
                    {
                        //From what i read we should not re-use a pipe.
                        Pipe.Dispose();
                        OpenPipe();
                    }
                    catch (Exception e)
                    {
                        if (_logger.IsInfoEnabled)
                        {
                            _logger.Info("Got an exception trying to re-connect to server pipe", e);
                        }
                        break;
                    }
                }
            }
        }

        private static void PipeLogAndReply(StreamWriter writer, string reply)
        {
            writer.Write(reply);
            writer.Flush();
            if (_logger.IsInfoEnabled)
            {
                _logger.Info(reply);
            }
        }

        public string[] WebUrls { get; set; }

        private readonly JsonContextPool _tcpContextPool = new JsonContextPool();
        
        internal CertificateHolder ServerCertificate;

        public class CertificateHolder
        {
            public string CertificateForClients;
            public X509Certificate2 Certificate;
        }

        private static CertificateHolder LoadCertificate(SecurityConfiguration config)
        {
            try
            {
                var loadedCertificate = config.CertificatePassword == null
                    ? new X509Certificate2(File.ReadAllBytes(config.CertificatePath))
                    : new X509Certificate2(File.ReadAllBytes(config.CertificatePath), config.CertificatePassword);

                return new CertificateHolder
                {
                    Certificate = loadedCertificate,
                    CertificateForClients = Convert.ToBase64String(loadedCertificate.Export(X509ContentType.Cert))
                };
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Could not load certificate file {config.CertificatePath}, please check the path and password", e);
            }
        }
        
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
                    ushort shortPort;
                    if (ushort.TryParse(Configuration.Core.TcpServerUrl, out shortPort))
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
                bool successfullyBoundToAtLeastOne = false;
                var errors = new List<Exception>();
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
                        var msg = "Unable to start tcp listener on " + ipAddress + " on port " + port;
                        errors.Add(new IOException(msg, ex));
                        if (_logger.IsOperationsEnabled)
                            _logger.Operations(msg, ex);
                        continue;
                    }
                    successfullyBoundToAtLeastOne = true;
                    var listenerLocalEndpoint = (IPEndPoint)listener.LocalEndpoint;
                    status.Port = listenerLocalEndpoint.Port;
                    // when binding to multiple interfaces and the port is 0, use
                    // the same port across all interfaces
                    port = listenerLocalEndpoint.Port;
                    for (int i = 0; i < 4; i++)
                    {
                        ListenToNewTcpConnection(listener);
                    }
                }

                if(successfullyBoundToAtLeastOne == false)
                {
                    if (errors.Count == 1)
                        throw errors[0];
                    throw new AggregateException(errors);
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
            if (IPAddress.TryParse(host, out IPAddress ipAddress))
                return new[] { ipAddress };

            switch (host)
            {
                case "*":
                case "+":
                    return new[] { IPAddress.Any };
                case "localhost":
                case "localhost.fiddler":
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
                try
                {
                    tcpClient.NoDelay = true;
                    tcpClient.ReceiveBufferSize = 32 * 1024;
                    tcpClient.SendBufferSize = 4096;
                    Stream stream = tcpClient.GetStream();
                    stream = await AuthenticateAsServerIfSslNeeded(stream);
                    var tcp = new TcpConnectionOptions
                    {
                        ContextPool = _tcpContextPool,
                        Stream = stream,
                        TcpClient = tcpClient,
                        PinnedBuffer = JsonOperationContext.ManagedPinnedBuffer.LongLivedInstance(),
                    };

                    try
                    {
                        TcpConnectionHeaderMessage header;
                        using (_tcpContextPool.AllocateOperationContext(out JsonOperationContext context))
                        {
                            using (var headerJson = await context.ParseToMemoryAsync(
                                stream,
                                "tcp-header",
                                BlittableJsonDocumentBuilder.UsageMode.None,
                                tcp.PinnedBuffer,
                                ServerStore.ServerShutdown,
                                // we don't want to allow external (and anonymous) users to send us unlimited data
                                // a maximum of 2 KB for the header is big enough to include any valid header that
                                // we can currently think of
                                maxSize: 1024*2
                                ))
                            {
                                header = JsonDeserializationClient.TcpConnectionHeaderMessage(headerJson);
                                if (_logger.IsInfoEnabled)
                                {
                                    _logger.Info($"New {header.Operation} TCP connection to {header.DatabaseName ?? "the cluster node"} from {tcpClient.Client.RemoteEndPoint}");
                                }
                            }
                            if (TryAuthorize(context, Configuration, tcp.Stream, header) == false)
                            {
                                var msg =
                                    $"New {header.Operation} TCP connection to {header.DatabaseName ?? "the cluster node"} from {tcpClient.Client.RemoteEndPoint}" +
                                    $" is not authorized to access {header.DatabaseName ?? "the cluster node"}";
                                if (_logger.IsInfoEnabled)
                                {
                                    _logger.Info(msg);
                                }
                                throw new UnauthorizedAccessException(msg);
                            }
                        }

                        if (await DispatchServerWideTcpConnection(tcp, header))
                        {
                            tcp = null; //do not keep reference -> tcp will be disposed by server-wide connection handlers
                            return;
                        }

                        await DispatchDatabaseTcpConnection(tcp, header);
                    }
                    catch (Exception e)
                    {
                        if (_tcpLogger.IsInfoEnabled)
                            _tcpLogger.Info("Failed to process TCP connection run", e);

                        SendErrorIfPossible(tcp, e);
                    }
                }
                catch (Exception e)
                {
                    if (_tcpLogger.IsInfoEnabled)
                    {
                        _tcpLogger.Info("Failure when processing tcp connection", e);
                    }
                }
            });
        }

        private void SendErrorIfPossible(TcpConnectionOptions tcp, Exception e)
        {
            var tcpStream = tcp?.Stream;
            if (tcpStream == null)
                return;

            try
            {
                using (_tcpContextPool.AllocateOperationContext(out JsonOperationContext context))
                using (var errorWriter = new BlittableJsonTextWriter(context, tcpStream))
                {
                    context.Write(errorWriter, new DynamicJsonValue
                    {
                        ["Type"] = "Error",
                        ["Exception"] = e.ToString(),
                        ["Message"] = e.Message
                    });
                }
            }
            catch (Exception inner)
            {
                if (_tcpLogger.IsInfoEnabled)
                    _tcpLogger.Info("Failed to send error in TCP connection", inner);
            }
        }

        private ClusterMaintenanceWorker _clusterMaintenanceWorker;

        private async Task<bool> DispatchServerWideTcpConnection(TcpConnectionOptions tcp, TcpConnectionHeaderMessage header)
        {
            tcp.Operation = header.Operation;
            if (tcp.Operation == TcpConnectionHeaderMessage.OperationTypes.Cluster)
            {
                ServerStore.ClusterAcceptNewConnection(tcp.Stream);
                return true;
            }

            if (tcp.Operation == TcpConnectionHeaderMessage.OperationTypes.Heartbeats)
            {
                // check for the term          
                using (_tcpContextPool.AllocateOperationContext(out JsonOperationContext context))
                using (var headerJson = await context.ParseToMemoryAsync(
                    tcp.Stream,
                    "maintenance-heartbeat-header",
                    BlittableJsonDocumentBuilder.UsageMode.None,
                    tcp.PinnedBuffer
                ))
                {
                    
                    var maintenanceHeader = JsonDeserializationRachis<ClusterMaintenanceSupervisor.ClusterMaintenanceConnectionHeader>.Deserialize(headerJson);
                    
                    if (_clusterMaintenanceWorker?.CurrentTerm > maintenanceHeader.Term)
                    {
                        if (_tcpLogger.IsInfoEnabled)
                        {
                            _tcpLogger.Info($"Request for maintenance with term {maintenanceHeader.Term} was rejected, " +
                                            $"because we are already connected to the recent leader with the term {_clusterMaintenanceWorker.CurrentTerm}");
                        }
                        tcp.Dispose();
                        return true;
                    }
                    var old = _clusterMaintenanceWorker;
                    using (old)
                    {
                        _clusterMaintenanceWorker = new ClusterMaintenanceWorker(tcp, ServerStore.ServerShutdown, ServerStore, maintenanceHeader.Term);
                        _clusterMaintenanceWorker.Start();
                    }
                    return true;
                }
                
            }
            return false;
        }

        private async Task<bool> DispatchDatabaseTcpConnection(TcpConnectionOptions tcp, TcpConnectionHeaderMessage header)
        {
            var databaseLoadingTask = ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(header.DatabaseName);
            if (databaseLoadingTask == null)
            {
                DatabaseDoesNotExistException.Throw(header.DatabaseName);
                return true;
            }

            var databaseLoadTimeout = ServerStore.DatabasesLandlord.DatabaseLoadTimeout;

            if (databaseLoadingTask.IsCompleted == false)
            {
                var resultingTask = await Task.WhenAny(databaseLoadingTask, Task.Delay(databaseLoadTimeout));
                if (resultingTask != databaseLoadingTask)
                {
                    ThrowTimeoutOnDatabaseLoad(header);
                }
            }

            tcp.DocumentDatabase = await databaseLoadingTask;
            if(tcp.DocumentDatabase == null)
                DatabaseDoesNotExistException.Throw(header.DatabaseName);

            Debug.Assert(tcp.DocumentDatabase != null);

            if (tcp.DocumentDatabase.DatabaseShutdown.IsCancellationRequested)
                ThrowDatabaseShutdown(tcp.DocumentDatabase);

            tcp.DocumentDatabase.RunningTcpConnections.Add(tcp);
            switch (header.Operation)
            {

                case TcpConnectionHeaderMessage.OperationTypes.Subscription:
                    SubscriptionConnection.SendSubscriptionDocuments(tcp);
                    break;
                case TcpConnectionHeaderMessage.OperationTypes.Replication:
                    var documentReplicationLoader = tcp.DocumentDatabase.ReplicationLoader;
                    documentReplicationLoader.AcceptIncomingConnection(tcp);
                    break;
                default:
                    throw new InvalidOperationException("Unknown operation for TCP " + header.Operation);
            }

            //since the responses to TCP connections mostly continue to run
            //beyond this point, no sense to dispose the connection now, so set it to null.
            //this way the responders are responsible to dispose the connection and the context                    
            tcp = null;
            return false;
        }

        private async Task<Stream> AuthenticateAsServerIfSslNeeded(Stream stream)
        {
            if (ServerCertificate != null)
            {
                SslStream sslStream = new SslStream(stream, false, (sender, certificate, chain, errors) =>
                {
                    return errors == SslPolicyErrors.None ||
                           // it is fine that the client doesn't have a cert, we just care that they
                           // are connecting to us securely
                           errors == SslPolicyErrors.RemoteCertificateNotAvailable;
                });
                stream = sslStream;
                await sslStream.AuthenticateAsServerAsync(ServerCertificate.Certificate, true, SslProtocols.Tls12, false);
            }

            return stream;
        }

        private bool TryAuthorize(JsonOperationContext context, RavenConfiguration configuration, Stream stream, TcpConnectionHeaderMessage header)
        {
            using (var writer = new BlittableJsonTextWriter(context, stream))
            {
                if (configuration.Server.AnonymousUserAccessMode == AnonymousUserAccessModeValues.Admin
                    && header.AuthorizationToken == null)
                {
                    ReplyStatus(writer, nameof(TcpConnectionHeaderResponse.AuthorizationStatus.Success));
                    return true;
                }
                
                var sigBase64Size = Sparrow.Utils.Base64.CalculateAndValidateOutputLength(Sodium.crypto_sign_bytes());
                if (header.AuthorizationToken == null || header.AuthorizationToken.Length < sigBase64Size + 8 /* sig length + prefix */)
                {
                    ReplyStatus(writer, nameof(TcpConnectionHeaderResponse.AuthorizationStatus.AuthorizationTokenRequired));
                    return false;
                }

                AccessToken accessToken;
                if (RequestRouter.TryGetAccessToken(this, header.AuthorizationToken, sigBase64Size, out accessToken) == false)
                {
                    if (accessToken.IsExpired)
                    {
                        ReplyStatus(writer, nameof(TcpConnectionHeaderResponse.AuthorizationStatus.ExpiredAuthorizationToken));
                        return false;
                    }
                    ReplyStatus(writer, nameof(TcpConnectionHeaderResponse.AuthorizationStatus.BadAuthorizationToken));
                    return false;
                }

                AccessModes mode;
                var hasValue =
                    accessToken.AuthorizedDatabases.TryGetValue(header.DatabaseName, out mode) ||
                    accessToken.AuthorizedDatabases.TryGetValue("*", out mode);

                if (hasValue == false)
                    mode = AccessModes.None;

                switch (mode)
                {
                    case AccessModes.None:
                        ReplyStatus(writer, nameof(TcpConnectionHeaderResponse.AuthorizationStatus.Forbidden));
                        return false;
                    case AccessModes.ReadOnly:
                        ReplyStatus(writer, nameof(TcpConnectionHeaderResponse.AuthorizationStatus.ForbiddenReadOnly));
                        return false;
                    case AccessModes.ReadWrite:
                    case AccessModes.Admin:
                        ReplyStatus(writer, nameof(TcpConnectionHeaderResponse.AuthorizationStatus.Success));
                        return true;
                    default:
                        throw new ArgumentOutOfRangeException("Unknown access mode: " + mode);
                }
            }

        }


        private static void ReplyStatus(BlittableJsonTextWriter writer, string status)
        {
            writer.WriteStartObject();
            writer.WritePropertyName(nameof(TcpConnectionHeaderResponse.Status));
            writer.WriteString(status);
            writer.WriteEndObject();
            writer.Flush();
        }

        private static void ThrowDatabaseShutdown(DocumentDatabase database)
        {
            throw new DatabaseDisabledException($"Database {database.Name} was shutdown.");
        }

        private static void ThrowTimeoutOnDatabaseLoad(TcpConnectionHeaderMessage header)
        {
            throw new DatabaseLoadTimeoutException($"Timeout when loading database {header.DatabaseName}, try again later");
        }

        public RequestRouter Router { get; private set; }
        public MetricsCountersManager Metrics { get; private set; }

        public bool Disposed { get; private set; }
        internal NamedPipeServerStream Pipe { get; set; }

        public void Dispose()
        {
            if (Disposed)
                return;
            lock (this)
            {
                if (Disposed)
                    return;

                Disposed = true;
                Pipe?.Dispose();
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
                            if (_tcpLogger.IsInfoEnabled)
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

                AfterDisposal?.Invoke();
            }
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

        public const string PipePrefix = "raven-control-pipe-";
        public void OpenPipe()
        {
            var pipeName = PipePrefix + Process.GetCurrentProcess().Id;

            Pipe = new NamedPipeServerStream(pipeName, PipeDirection.InOut, 1, PipeTransmissionMode.Byte,
                PipeOptions.None, 1024, 1024);
        }
    }
}