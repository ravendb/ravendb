using System;
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
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Json;
using Raven.Server.Rachis;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Maintenance;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using System.Reflection;
using System.Security.Claims;
using System.Runtime.InteropServices;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Microsoft.AspNetCore.Server.Kestrel.Filter;
using Microsoft.AspNetCore.Server.Kestrel.Https;
using Org.BouncyCastle.Pkcs;
using Raven.Client;
using Raven.Client.Extensions;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.ServerWide.Tcp;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils.Cli;
using Sparrow.Platform;
using Sparrow.Platform.Posix;
using Voron.Platform.Posix;

namespace Raven.Server
{
    public class RavenServer : IDisposable
    {
        static RavenServer()
        {
            
        }

        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<RavenServer>("Raven/Server");

        public readonly RavenConfiguration Configuration;

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

            _tcpLogger = LoggingSource.Instance.GetLogger<RavenServer>("<TcpServer>");
        }

        public Task<TcpListenerStatus> GetTcpServerStatusAsync()
        {
            return _tcpListenerTask;
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
                if (Logger.IsOperationsEnabled)
                    Logger.Operations("Could not open the server store", e);
                throw;
            }

            if (Logger.IsInfoEnabled)
                Logger.Info(string.Format("Server store started took {0:#,#;;0} ms", sp.ElapsedMilliseconds));

            sp.Restart();
            ListenToPipe().IgnoreUnobservedExceptions();
            Router = new RequestRouter(RouteScanner.Scan(), this);

            try
            {
                Action<KestrelServerOptions> kestrelOptions = options => options.ShutdownTimeout = TimeSpan.FromSeconds(1);
                bool certificateLoaded = LoadCertificate();
                if (certificateLoaded)
                {
                    kestrelOptions += options =>
                    {
                        var filterOptions = new HttpsConnectionFilterOptions
                        {
                            ServerCertificate = ServerCertificateHolder.Certificate,
                            CheckCertificateRevocation = true,
                            ClientCertificateMode = ClientCertificateMode.AllowCertificate,
                            SslProtocols = SslProtocols.Tls12,
                            ClientCertificateValidation = (X509Certificate2 cert, X509Chain chain, SslPolicyErrors errors) =>
                                    // Here we are explicitly ignoring trust chain issues for client certificates
                                    // this is because we don't actually require trust, we just use the certificate
                                    // as a way to authenticate. The admin is going to tell us which specific certs
                                    // we can trust anyway, so we can ignore such errors.
                                    errors == SslPolicyErrors.RemoteCertificateChainErrors ||
                                    errors == SslPolicyErrors.None
                        };

                        options.ConnectionFilter = new AuthenticatingFilter(this, new HttpsConnectionFilter(filterOptions, new NoOpConnectionFilter()));
                    };
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
                if (Logger.IsInfoEnabled)
                    Logger.Info("Could not configure server", e);
                throw;
            }

            if (Logger.IsInfoEnabled)
                Logger.Info(string.Format("Configuring HTTP server took {0:#,#;;0} ms", sp.ElapsedMilliseconds));

            try
            {
                _webHost.Start();

                var serverAddressesFeature = _webHost.ServerFeatures.Get<IServerAddressesFeature>();
                WebUrls = serverAddressesFeature.Addresses.ToArray();

                if (Logger.IsInfoEnabled)
                    Logger.Info($"Initialized Server... {string.Join(", ", WebUrls)}");

                _tcpListenerTask = StartTcpListener();
            }
            catch (Exception e)
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations("Could not start server", e);
                throw;
            }
        }

        private bool LoadCertificate()
        {
            var certificateLoaded = false;
            try
            {
                if (string.IsNullOrEmpty(Configuration.Security.CertificateExec) == false)
                {
                    ServerCertificateHolder = ServerStore.Secrets.LoadCertificateWithExecutable();
                    certificateLoaded = true;
                }
                else if (string.IsNullOrEmpty(Configuration.Security.CertificatePath) == false)
                {
                    ServerCertificateHolder = ServerStore.Secrets.LoadCertificateFromPath();
                    certificateLoaded = true;
                }
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Unable to start the server due to  invalid certificate configuration! Admin assistance required.", e);
            }

            return certificateLoaded;
        }

        private async Task ListenToPipe()
        {
            // We start the server pipe only when running as a server
            // so we won't generate one per server in our test environment 
            if (Pipe == null)
                return;
            try
            {
                while (true)
                {
                    await Pipe.WaitForConnectionAsync();
                    var reader = new StreamReader(Pipe);
                    var writer = new StreamWriter(Pipe);
                    try
                    {
                        var cli = new RavenCli();
                        var restart = cli.Start(this, writer, reader, false, null);
                        if (restart)
                        {
                            writer.WriteLine("Restarting Server...<DELIMETER_RESTART>");
                            Program.ResetServerMre.Set();
                            Program.ShutdownServerMre.Set();
                            // server restarting
                            return;
                        }

                        writer.WriteLine("Shutting Down Server...<DELIMETER_RESTART>");
                        Program.ShutdownServerMre.Set();
                        // server shutting down
                        return;
                    }
                    catch (Exception e)
                    {
                        if (Logger.IsInfoEnabled)
                        {
                            Logger.Info("Got an exception inside cli (internal error) while in pipe connection", e);
                        }
                    }

                    Pipe.Disconnect();
                }
            }
            catch (ObjectDisposedException)
            {
                //Server shutting down
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                {
                    Logger.Info("Got an exception trying to connect to server pipe", e);
                }
            }
        }

        public class AuthenticateConnection : IHttpAuthenticationFeature
        {
            public Dictionary<string, DatabaseAccess> AuthorizedDatabases = new Dictionary<string, DatabaseAccess>(StringComparer.OrdinalIgnoreCase);
            private Dictionary<string, DatabaseAccess> _caseSensitiveAuthorizedDatabases = new Dictionary<string, DatabaseAccess>();
            public X509Certificate2 Certificate;
            public CertificateDefinition Definition;

            public bool CanAccess(string db, bool requireAdmin)
            {
                if (Status == AuthenticationStatus.Expired || Status == AuthenticationStatus.NotYetValid)
                    return false;

                if (Status == AuthenticationStatus.ServerAdmin)
                    return true;

                if (db == null)
                    return false;

                if (Status != AuthenticationStatus.Allowed)
                    return false;

                if (_caseSensitiveAuthorizedDatabases.TryGetValue(db, out var mode))
                    return mode == DatabaseAccess.Admin || !requireAdmin;

                if (AuthorizedDatabases.TryGetValue(db, out mode) == false)
                    return false;

                // Technically speaking, since this is per connection, this is single threaded. But I'm 
                // worried about race conditions here if we move to HTTP 2.0 at some point. At that point,
                // we'll probably want to handle this concurrently, and the cost of adding it in this manner
                // is pretty small for most cases anyway
                _caseSensitiveAuthorizedDatabases = new Dictionary<string, DatabaseAccess>(_caseSensitiveAuthorizedDatabases)
                {
                    {db, mode}
                };

                return mode == DatabaseAccess.Admin || !requireAdmin;
            }

            ClaimsPrincipal IHttpAuthenticationFeature.User { get; set; }

            IAuthenticationHandler IHttpAuthenticationFeature.Handler { get; set; }

            public AuthenticationStatus Status;
        }

        public class AuthenticatingFilter : IConnectionFilter
        {
            private readonly RavenServer _server;
            private readonly HttpsConnectionFilter _httpsConnectionFilter;

            public AuthenticatingFilter(RavenServer server, HttpsConnectionFilter httpsConnectionFilter)
            {
                _server = server;
                _httpsConnectionFilter = httpsConnectionFilter;
            }

            private class DummyHttpRequestFeature : IHttpRequestFeature
            {
                public string Protocol { get; set; }
                public string Scheme { get; set; }
                public string Method { get; set; }
                public string PathBase { get; set; }
                public string Path { get; set; }
                public string QueryString { get; set; }
                public string RawTarget { get; set; }
                public IHeaderDictionary Headers { get; set; }
                public Stream Body { get; set; }
            }

            public async Task OnConnectionAsync(ConnectionFilterContext context)
            {
                await _httpsConnectionFilter.OnConnectionAsync(context);
                var old = context.PrepareRequest;

                var featureCollection = new FeatureCollection();
                featureCollection.Set<IHttpRequestFeature>(new DummyHttpRequestFeature());
                old?.Invoke(featureCollection);

                var tls = featureCollection.Get<ITlsConnectionFeature>();
                var certificate = tls?.ClientCertificate;
                var authenticationStatus = _server.AuthenticateConnectionCertificate(certificate);

                // build the token
                context.PrepareRequest = features =>
                {
                    old?.Invoke(features);
                    features.Set<IHttpAuthenticationFeature>(authenticationStatus);
                };
            }

        }

        private AuthenticateConnection AuthenticateConnectionCertificate(X509Certificate2 certificate)
        {
            var authenticationStatus = new AuthenticateConnection {Certificate = certificate};
            if (certificate == null)
            {
                authenticationStatus.Status = AuthenticationStatus.NoCertificateProvided;
            }
            else if (certificate.NotAfter < DateTime.UtcNow)
            {
                authenticationStatus.Status = AuthenticationStatus.Expired;
            }
            else if (certificate.NotBefore > DateTime.UtcNow)
            {
                authenticationStatus.Status = AuthenticationStatus.NotYetValid;
            }
            else if (certificate.Equals(ServerCertificateHolder.Certificate))
            {
                authenticationStatus.Status = AuthenticationStatus.ServerAdmin;
            }
            else
            {
                using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                {
                    var certKey = Constants.Certificates.Prefix + certificate.Thumbprint;
                    BlittableJsonReaderObject cert;
                    using (ctx.OpenReadTransaction())
                    {
                        cert = ServerStore.Cluster.Read(ctx, certKey) ??
                               ServerStore.Cluster.GetLocalState(ctx, certKey);
                    }
                    if (cert == null)
                    {
                        authenticationStatus.Status = AuthenticationStatus.UnfamiliarCertificate;
                    }
                    else
                    {
                        var definition = JsonDeserializationServer.CertificateDefinition(cert);
                        authenticationStatus.Definition = definition;
                        if (definition.ServerAdmin)
                        {
                            authenticationStatus.Status = AuthenticationStatus.ServerAdmin;
                        }
                        else
                        {
                            authenticationStatus.Status = AuthenticationStatus.Allowed;
                            foreach (var kvp in definition.Permissions)
                            {
                                authenticationStatus.AuthorizedDatabases.Add(kvp.Key, kvp.Value);
                            }
                        }
                    }
                }
            }
            return authenticationStatus;
        }


        public string[] WebUrls { get; set; }

        private readonly JsonContextPool _tcpContextPool = JsonContextPool.Shared;

        internal CertificateHolder ServerCertificateHolder = new CertificateHolder();

        public class CertificateHolder
        {
            public string CertificateForClients;
            public X509Certificate2 Certificate;
            public AsymmetricKeyEntry PrivateKey;
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
                    if (ushort.TryParse(Configuration.Core.TcpServerUrl, out ushort shortPort))
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
                    if (Logger.IsInfoEnabled)
                        Logger.Info($"RavenDB TCP is configured to use {Configuration.Core.TcpServerUrl} and bind to {ipAddress} at {port}");

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
                        if (Logger.IsOperationsEnabled)
                            Logger.Operations(msg, ex);
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

                if (successfullyBoundToAtLeastOne == false)
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
                return new[] {ipAddress};

            switch (host)
            {
                case "*":
                case "+":
                    return new[] {IPAddress.Any};
                case "localhost":
                case "localhost.fiddler":
                    return new[] {IPAddress.Loopback};
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
                                maxSize: 1024 * 2
                            ))
                            {
                                header = JsonDeserializationClient.TcpConnectionHeaderMessage(headerJson);
                                if (Logger.IsInfoEnabled)
                                {
                                    Logger.Info(
                                        $"New {header.Operation} TCP connection to {header.DatabaseName ?? "the cluster node"} from {tcpClient.Client.RemoteEndPoint}");
                                }
                            }
                            var authSuccessful = TryAuthorize(Configuration, tcp.Stream, header, out var err);


                            using (var writer = new BlittableJsonTextWriter(context, stream))
                            {
                                writer.WriteStartObject();
                                writer.WritePropertyName(nameof(TcpConnectionHeaderResponse.AuthorizationSuccessful));
                                writer.WriteBool(authSuccessful);
                                if (err != null)
                                {
                                    writer.WriteComma();
                                    writer.WritePropertyName(nameof(TcpConnectionHeaderResponse.Message));
                                    writer.WriteString(err);
                                }
                                writer.WriteEndObject();
                                writer.Flush();
                            }

                            if (authSuccessful == false)
                            {
                                if (Logger.IsInfoEnabled)
                                {
                                    Logger.Info(
                                        $"New {header.Operation} TCP connection to {header.DatabaseName ?? "the cluster node"} from {tcpClient.Client.RemoteEndPoint}" +
                                        $" is not authorized to access {header.DatabaseName ?? "the cluster node"} because {err}");
                                }
                                return; // cannot proceed
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
            if (tcp.DocumentDatabase == null)
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
            // ReSharper disable once RedundantAssignment
            tcp = null;
            return false;
        }

        private async Task<Stream> AuthenticateAsServerIfSslNeeded(Stream stream)
        {
            if (ServerCertificateHolder.Certificate != null)
            {
                var sslStream = new SslStream(stream, false, (sender, certificate, chain, errors) =>
                    // it is fine that the client doesn't have a cert, we just care that they
                    // are connecting to us securely. At any rate, we'll ensure that if certificate
                    // is required, we'll validate that it is one of the expected ones on the server
                    // and that the client is authorized to do so. 
                    // Otherwise, we'll generate an error, but we'll do that at a higher level then
                    // SSL, because that generate a nicer error for the user to read then just aborted
                    // connection because SSL negotation failed.
                        true);
                stream = sslStream;

                await sslStream.AuthenticateAsServerAsync(ServerCertificateHolder.Certificate, true, SslProtocols.Tls12, false);
            }

            return stream;
        }

        private bool TryAuthorize(RavenConfiguration configuration, Stream stream, TcpConnectionHeaderMessage header, out string msg)
        {
            msg = null;

            if (configuration.Security.AuthenticationEnabled == false)
                return true;

            if (!(stream is SslStream sslStream))
            {
                msg = "TCP connection is required to use SSL when authentication is enabled";
                return false;
            }

            var certificate = (X509Certificate2)sslStream.RemoteCertificate;
            var auth = AuthenticateConnectionCertificate(certificate);

            switch (auth.Status)
            {
                case AuthenticationStatus.Expired:
                    msg = "The provided client certificate " + certificate.FriendlyName + " is expired on " + certificate.NotAfter;
                    return false;
                case AuthenticationStatus.NotYetValid:
                    msg = "The provided client certificate " + certificate.FriendlyName + " is not yet valid because it starts on " + certificate.NotBefore;
                    return false;
                case AuthenticationStatus.ServerAdmin:
                    msg = "Admin can do it all";
                    return true;
                case AuthenticationStatus.Allowed:
                    switch (header.Operation)
                    {
                        case TcpConnectionHeaderMessage.OperationTypes.Cluster:
                        case TcpConnectionHeaderMessage.OperationTypes.Heartbeats:
                            msg = header.Operation + " is a server wide operation and the certificate " + certificate.FriendlyName + "is not ServerAdmin";
                            return false;
                        case TcpConnectionHeaderMessage.OperationTypes.Subscription:
                        case TcpConnectionHeaderMessage.OperationTypes.Replication:
                            if (auth.CanAccess(header.DatabaseName, requireAdmin: false))
                                return true;
                            msg = "The certificate " + certificate.FriendlyName + " does not allow access to " + header.DatabaseName;
                            return false;
                        default:
                            throw new InvalidOperationException("Unknown operation " + header.Operation);
                    }
                default:
                    msg = "Cannot allow access to a certificate with status: " + auth.Status;
                    return false;
            }
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
                var ea = new ExceptionAggregator("Failed to properly close RavenServer");

                ea.Execute(() => Pipe?.Dispose());
                ea.Execute(() => Metrics?.Dispose());
                ea.Execute(() => _webHost?.Dispose());
                if (_tcpListenerTask != null)
                {
                    ea.Execute(() =>
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
                    });
                }

                ea.Execute(() => ServerStore?.Dispose());
                ea.Execute(() => ServerMaintenanceTimer?.Dispose());
                ea.Execute(() => AfterDisposal?.Invoke());

                ea.ThrowIfNeeded();
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
            var pipeDir = Path.Combine(Path.GetTempPath(), "ravendb-pipe");

            if (PlatformDetails.RunningOnPosix)
            {
                try
                {
                    if (Directory.Exists(pipeDir) == false)
                    {
                        const FilePermissions mode = FilePermissions.S_IRWXU;
                        var rc = Syscall.mkdir(pipeDir, (ushort)mode);
                        if (rc != 0)
                            throw new IOException($"Unable to create directory {pipeDir} with permission {mode}. LastErr={Marshal.GetLastWin32Error()}");
                    }




                    foreach (var pipeFile in Directory.GetFiles(pipeDir, PipePrefix + "*"))
                    {
                        try
                        {
                            File.Delete(pipeFile);
                        }
                        catch (Exception e)
                        {
                            if (Logger.IsInfoEnabled)
                                Logger.Info("Unable to delete old pipe file " + pipeFile, e);
                        }
                    }
                }
                catch (Exception ex)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info("Unable to list old pipe files for deletion", ex);
                }
            }

            var pipeName = PipePrefix + Process.GetCurrentProcess().Id;
            Pipe = new NamedPipeServerStream(pipeName, PipeDirection.InOut, 1, PipeTransmissionMode.Byte,
                PipeOptions.Asynchronous, 1024, 1024);

            if (PlatformDetails.RunningOnPosix
            ) // TODO: remove this if and after https://github.com/dotnet/corefx/issues/22141 (both in RavenServer.cs and AdminChannel.cs)
            {
                var pathField = Pipe.GetType().GetField("_path", BindingFlags.NonPublic | BindingFlags.Instance);
                if (pathField == null)
                {
                    throw new InvalidOperationException("Unable to set the proper path for the admin pipe, admin channel will not be available");
                }
                pathField.SetValue(Pipe, Path.Combine(pipeDir, pipeName));
            }
        }

        public enum AuthenticationStatus
        {
            None,
            NoCertificateProvided,
            UnfamiliarCertificate,
            Allowed,
            ServerAdmin,
            Expired,
            NotYetValid
        }
    }
}
