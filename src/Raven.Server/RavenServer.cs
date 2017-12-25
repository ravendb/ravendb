using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Pipes;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http.Features;
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
using System.Security.Claims;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Microsoft.AspNetCore.ResponseCompression;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Newtonsoft.Json;
using Org.BouncyCastle.Pkcs;
using Raven.Client;
using Raven.Client.Exceptions.Security;
using Raven.Client.Extensions;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.ServerWide.Tcp;
using Raven.Server.Commercial;
using Raven.Server.Documents.Patch;
using Raven.Server.Https;
using Raven.Server.Monitoring.Snmp;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.ResponseCompression;
using Sparrow;

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
        private readonly Logger _tcpLogger;

        public event Action AfterDisposal;

        public readonly ServerStatistics Statistics;

        public RavenServer(RavenConfiguration configuration)
        {
            JsonDeserializationValidator.Validate();

            Configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            if (Configuration.Initialized == false)
                throw new InvalidOperationException("Configuration must be initialized");

            Statistics = new ServerStatistics();

            AdminScripts = new ScriptRunnerCache(null, Configuration)
            {
                EnableClr = true
            };

            ServerStore = new ServerStore(Configuration, this);
            Metrics = new MetricCounters();

            _tcpLogger = LoggingSource.Instance.GetLogger<RavenServer>("<TcpServer>");
        }

        public TcpListenerStatus GetTcpServerStatus()
        {
            return _tcpListenerStatus;
        }

        public void Initialize()
        {
            var sp = Stopwatch.StartNew();
            Certificate = LoadCertificate() ?? new CertificateHolder();
   
            try
            {
                ServerStore.Initialize();

                ServerStore.EnsureRegistrationOfCertificatesForTrustedIssuers(Certificate.Certificate);
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
            ListenToPipes().IgnoreUnobservedExceptions();
            Router = new RequestRouter(RouteScanner.Scan(), this);

            try
            {
                ListenEndpoints = GetServerAddressesAndPort();

                void ConfigureKestrel(KestrelServerOptions options)
                {
                    options.Limits.MaxRequestLineSize = (int)Configuration.Http.MaxRequestLineSize.GetValue(SizeUnit.Bytes);
                    options.Limits.MaxRequestBodySize = null;       // no limit!
                    options.Limits.MinResponseDataRate = null;      // no limit!
                    options.Limits.MinRequestBodyDataRate = null;   // no limit!

                    if (Configuration.Http.MinDataRatePerSecond.HasValue && Configuration.Http.MinDataRateGracePeriod.HasValue)
                    {
                        options.Limits.MinResponseDataRate = new MinDataRate(Configuration.Http.MinDataRatePerSecond.Value.GetValue(SizeUnit.Bytes), Configuration.Http.MinDataRateGracePeriod.Value.AsTimeSpan);
                        options.Limits.MinRequestBodyDataRate = new MinDataRate(Configuration.Http.MinDataRatePerSecond.Value.GetValue(SizeUnit.Bytes), Configuration.Http.MinDataRateGracePeriod.Value.AsTimeSpan);
                    }

                    if (Configuration.Http.MaxRequestBufferSize.HasValue)
                        options.Limits.MaxRequestBufferSize = Configuration.Http.MaxRequestBufferSize.Value.GetValue(SizeUnit.Bytes);

                    if (Certificate.Certificate != null)
                    {
                        _httpsConnectionAdapter = new HttpsConnectionAdapter();
                        _httpsConnectionAdapter.SetCertificate(Certificate.Certificate);
                        _refreshClusterCertificate = new Timer(RefreshClusterCertificate);
                        var adapter = new AuthenticatingAdapter(this, _httpsConnectionAdapter);

                        foreach (var address in ListenEndpoints.Addresses)
                        {
                            options.Listen(address, ListenEndpoints.Port, listenOptions => { listenOptions.ConnectionAdapters.Add(adapter); });
                        }
                    }
                }

                _webHost = new WebHostBuilder()
                    .CaptureStartupErrors(captureStartupErrors: true)
                    .UseKestrel(ConfigureKestrel)
                    .UseUrls(Configuration.Core.ServerUrls)
                    .UseStartup<RavenServerStartup>()
                    .UseShutdownTimeout(TimeSpan.FromSeconds(1))
                    .ConfigureServices(services =>
                    {
                        if (Configuration.Http.UseResponseCompression)
                        {
                            services.Configure<ResponseCompressionOptions>(options =>
                            {
                                options.EnableForHttps = Configuration.Http.AllowResponseCompressionOverHttps;
                                options.Providers.Add(typeof(GzipCompressionProvider));
                                options.Providers.Add(typeof(DeflateCompressionProvider));
                            });

                            services.Configure<GzipCompressionProviderOptions>(options =>
                            {
                                options.Level = Configuration.Http.GzipResponseCompressionLevel;
                            });

                            services.Configure<DeflateCompressionProviderOptions>(options =>
                            {
                                options.Level = Configuration.Http.DeflateResponseCompressionLevel;
                            });

                            services.AddResponseCompression();
                        }

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
                WebUrl = GetWebUrl(serverAddressesFeature.Addresses.First()).TrimEnd('/');

                if (Logger.IsInfoEnabled)
                    Logger.Info($"Initialized Server... {WebUrl}");

                _tcpListenerStatus = StartTcpListener();

                ServerStore.TriggerDatabases();

                StartSnmp();

                _refreshClusterCertificate?.Change(TimeSpan.FromMinutes(1), TimeSpan.FromHours(1));
            }
            catch (Exception e)
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations("Could not start server", e);
                throw;
            }
        }

        private Task _currentRefreshTask = Task.CompletedTask;

        public void RefreshClusterCertificate(object state)
        {
            // If the setup mode is anything but SetupMode.LetsEncrypt, we'll
            // check if the certificate changed and if so we'll update it immediately
            // on the local node (only). Admin is responsible for registering the new
            // certificate in the cluster and updating all the nodes

            // If the setup mode is SetupMode.LetsEncrypt, we'll check if we need to
            // update it, and if so, we'll re-generate the certificate then we'll
            // distribute it via the cluster. We'll update the cert only when all nodes
            // confirm they got it (or if there are less than 3 days to spare).


            var currentCertificate = Certificate;
            if (currentCertificate == null)
            {
                return; // shouldn't happen, but just in case
            }
            var currentRefreshTask = _currentRefreshTask;
            if (currentRefreshTask.IsCompleted == false)
            {
                _refreshClusterCertificate?.Change(TimeSpan.FromMinutes(1), TimeSpan.FromHours(1));
                return;
            }
            var refreshCertificate = new Task(async () =>
            {
                await DoActualCertificateRefresh(currentCertificate);
            });
            if (Interlocked.CompareExchange(ref _currentRefreshTask, currentRefreshTask, refreshCertificate) != currentRefreshTask)
                return;
            refreshCertificate.Start();
        }

        private async Task DoActualCertificateRefresh(CertificateHolder currentCertificate)
        {
            try
            {
                CertificateHolder newCertificate = null;
                try
                {
                    newCertificate = LoadCertificate();
                }
                catch (Exception e)
                {
                    if (Logger.IsOperationsEnabled)
                        Logger.Operations("Tried to load certificate as part of refresh check, but got an error!", e);
                }
                if (newCertificate == null)
                {
                    if (Logger.IsOperationsEnabled)
                        Logger.Operations("Tried to load certificate as part of refresh check, but got a null back, but got a valid certificate on startup!");
                    return;
                }

                if (newCertificate.Certificate.Thumbprint != currentCertificate.Certificate.Thumbprint)
                {
                    if (Interlocked.CompareExchange(ref Certificate, newCertificate, currentCertificate) == currentCertificate)
                        _httpsConnectionAdapter.SetCertificate(newCertificate.Certificate);
                    return;
                }
                if (Configuration.Core.SetupMode != SetupMode.LetsEncrypt)
                    return;

                if (ServerStore.IsLeader() == false)
                    return;

                // we need to see if there is already an ongoing process
                using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var certUpdate = ServerStore.Cluster.GetItem(context, "server/cert");
                    if (certUpdate != null
                        && certUpdate.TryGet("Thumbprint", out string thumbprint)
                        && thumbprint != currentCertificate.Certificate.Thumbprint)
                    {
                        // we are already in the process of updating the certificate, so we need
                        // to nudge all the nodes in the cluster in case we have a node that didn't
                        // confirm to us but the time to replace has already passed.
                        await ServerStore.SendToLeaderAsync(new RecheckStatusOfServerCertificateCommand());
                        return;
                    }
                }

                    // same certificate, but now we need to see if we are need to auto update it
                    var remainingDays = (currentCertificate.Certificate.NotAfter - DateTime.Now).TotalDays;
                    if (remainingDays > 30)
                        return; // nothing to do, the certs are the same and we have enough time

                    // we want to setup all the renewals for Saturday so we'll have reduce the amount of cert renwals that are counted against our renewals
                    // but if we have less than 20 days, we'll try anyway
                    if (DateTime.Today.DayOfWeek != DayOfWeek.Saturday && remainingDays > 20)
                        return;

                try
                {
                    newCertificate = await RefreshLetsEncryptCertificate(currentCertificate);
                }
                catch (Exception e)
                {
                    if (Logger.IsOperationsEnabled)
                        Logger.Operations("Failed to update certificate from Lets Encrypt", e);
                    return;
                }
                await StartCertificateReplicationAsync(newCertificate.Certificate);
            }
            catch (Exception e)
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations("Failure when trying to refresh certificate", e);
            }
        }

        public async Task StartCertificateReplicationAsync(X509Certificate2 newCertificate)
        {
            // the process of updating a new certificate is the same as deleting database
            // we first send the certificate to all the nodes, then we get aknowledgments
            // about that from them, and we replace only when they are are confirmed
            // to have been successful.
            // However, if we have less than 3 days for renewing the cert, we'll replace
            // immediately.

            // we first register it as a valid cluster node certificate in the cluster
            await ServerStore.RegisterServerCertificateInCluster(newCertificate, "Cluster-Wide Certificate");
            var base64Cert = Convert.ToBase64String(newCertificate.Export(X509ContentType.Pkcs12, (string)null));
            await ServerStore.SendToLeaderAsync(new InstallUpdatedServerCertificateCommand
            {
                Certificate = base64Cert
            });
        }

        private async Task<CertificateHolder> RefreshLetsEncryptCertificate(CertificateHolder existing)
        {
            var hosts = SetupManager.GetCertificateAlternativeNames(existing.Certificate).ToArray();

            var substring = hosts[0].Substring(0, hosts[0].Length - SetupManager.RavenDbDomain.Length - 1);
            var domainEnd = substring.LastIndexOf('.');

            var license = ServerStore.LoadLicense();

            HttpResponseMessage response;
            try
            {
                var licensePayload = JsonConvert.SerializeObject(new
                {
                    License = license
                });
                var content = new StringContent(licensePayload, Encoding.UTF8, "application/json");

                response = await ApiHttpClient.Instance.PostAsync("/api/v1/dns-n-cert/user-domains", content).ConfigureAwait(false);

                response.EnsureSuccessStatusCode();

            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to validate user's license as part of Let's Encrypt certificate refresh", e);
            }

            var userDomainsResult = JsonConvert.DeserializeObject<UserDomainsResult>(await response.Content.ReadAsStringAsync());

            var domain = substring.Substring(domainEnd + 1);

            if (userDomainsResult.Domains.Any(userDomain => string.Equals(userDomain.Key, domain, StringComparison.OrdinalIgnoreCase)) == false)
                throw new InvalidOperationException("The license provided does not have access to the domain: " + domain);
            
            var setupInfo = new SetupInfo
            {
                Domain = domain,
                ModifyLocalServer = false, // N/A here
                RegisterClientCert = false, // N/A here
                Password = null,
                Certificate = null,
                License = license,
                Email = userDomainsResult.Email,
                NodeSetupInfos = new Dictionary<string, SetupInfo.NodeInfo>()
            };

            var fullDomainPortion = domain + "." + SetupManager.RavenDbDomain;

            foreach (var host in hosts) // we just need the keys here
            {
                var key = host.Substring(0, host.Length - fullDomainPortion.Length - 1);
                setupInfo.NodeSetupInfos[key] = new SetupInfo.NodeInfo();
            }

            var cert = await SetupManager.RefreshLetsEncryptTask(setupInfo, ServerStore, ServerStore.ServerShutdown);

            return SecretProtection.ValidateCertificateAndCreateCertificateHolder("Let's Encrypt Refresh", cert, Convert.FromBase64String(setupInfo.Certificate), setupInfo.Password);
        }

        private (IPAddress[] Addresses, int Port) GetServerAddressesAndPort()
        {
            int? port = null;
            var addresses = new HashSet<IPAddress>();
            foreach (var serverUrl in Configuration.Core.ServerUrls)
            {
                var uri = new Uri(serverUrl);
                port = uri.Port;

                var host = uri.DnsSafeHost;
                foreach (var ipAddress in GetListenIpAddresses(host))
                    addresses.Add(ipAddress);
            }

            return (addresses.ToArray(), port.Value);
        }

        private string GetWebUrl(string kestrelUrl)
        {
            var serverUri = new Uri(Configuration.Core.ServerUrls[0]);
            if (serverUri.IsDefaultPort == false && serverUri.Port == 0)
            {
                var kestrelUri = new Uri(kestrelUrl);
                return new UriBuilder(serverUri)
                {
                    Port = kestrelUri.Port
                }.Uri.ToString();
            }
            return Configuration.Core.ServerUrls[0];
        }

        private CertificateHolder LoadCertificate()
        {
            try
            {
                if (string.IsNullOrEmpty(Configuration.Security.CertificatePath) == false)
                    return ServerStore.Secrets.LoadCertificateFromPath(Configuration.Security.CertificatePath, Configuration.Security.CertificatePassword);
                if (string.IsNullOrEmpty(Configuration.Security.CertificateExec) == false)
                    return ServerStore.Secrets.LoadCertificateWithExecutable(Configuration.Security.CertificateExec, Configuration.Security.CertificateExecArguments);

                return null;
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Unable to start the server due to invalid certificate configuration! Admin assistance required.", e);
            }
        }

        private Task ListenToPipes()
        {
            return Task.WhenAll(
                Pipes.ListenToLogStreamPipe(this, LogStreamPipe),
                Pipes.ListenToAdminConsolePipe(this, AdminConsolePipe));
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

                if (Status == AuthenticationStatus.Operator || Status == AuthenticationStatus.ClusterAdmin)
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

            public string WrongProtocolMessage;

            private AuthenticationStatus _status;

            public AuthenticationStatus Status
            {
                get
                {
                    if (WrongProtocolMessage != null)
                        ThrowException();
                    return _status;
                }
                set => _status = value;
            }

            private void ThrowException()
            {
                throw new InsufficientTransportLayerProtectionException(WrongProtocolMessage);
            }
        }

        internal AuthenticateConnection AuthenticateConnectionCertificate(X509Certificate2 certificate)
        {
            var authenticationStatus = new AuthenticateConnection { Certificate = certificate };
            if (certificate == null)
            {
                authenticationStatus.Status = AuthenticationStatus.NoCertificateProvided;
            }
            else if (certificate.NotAfter.ToUniversalTime() < DateTime.UtcNow)
            {
                authenticationStatus.Status = AuthenticationStatus.Expired;
            }
            else if (certificate.NotBefore.ToUniversalTime() > DateTime.UtcNow)
            {
                authenticationStatus.Status = AuthenticationStatus.NotYetValid;
            }
            else if (certificate.Equals(Certificate.Certificate))
            {
                authenticationStatus.Status = AuthenticationStatus.ClusterAdmin;
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
                        if (definition.SecurityClearance == SecurityClearance.ClusterAdmin)
                        {
                            authenticationStatus.Status = AuthenticationStatus.ClusterAdmin;
                        }
                        else if (definition.SecurityClearance == SecurityClearance.ClusterNode)
                        {
                            authenticationStatus.Status = AuthenticationStatus.ClusterAdmin;
                        }
                        else if (definition.SecurityClearance == SecurityClearance.Operator)
                        {
                            authenticationStatus.Status = AuthenticationStatus.Operator;
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

        public string WebUrl { get; private set; }

        private readonly JsonContextPool _tcpContextPool = new JsonContextPool();

        internal CertificateHolder Certificate;

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

        private void StartSnmp()
        {
            _snmpWatcher = new SnmpWatcher(this);
            _snmpWatcher.Execute();
        }

        public TcpListenerStatus StartTcpListener()
        {
            var port = 0;
            var status = new TcpListenerStatus();

            var tcpServerUrl = Configuration.Core.TcpServerUrls;
            if (tcpServerUrl == null)
            {
                foreach (var serverUrl in Configuration.Core.ServerUrls)
                {
                    var host = new Uri(serverUrl).DnsSafeHost;

                    StartListeners(host, port, status);
                }
            }
            else if (tcpServerUrl.Length == 1 && ushort.TryParse(tcpServerUrl[0], out ushort shortPort))
            {
                foreach (var serverUrl in Configuration.Core.ServerUrls)
                {
                    var host = new Uri(serverUrl).DnsSafeHost;

                    StartListeners(host, shortPort, status);
                }
            }
            else
            {
                foreach (var tcpUrl in tcpServerUrl)
                {
                    var uri = new Uri(tcpUrl);
                    var host = uri.DnsSafeHost;
                    if (uri.IsDefaultPort == false)
                        port = uri.Port;

                    StartListeners(host, port, status);
                }
            }

            return status;
        }

        private void StartListeners(string host, int port, TcpListenerStatus status)
        {
            try
            {
                bool successfullyBoundToAtLeastOne = false;
                var errors = new List<Exception>();
                foreach (var ipAddress in GetListenIpAddresses(host))
                {
                    if (Configuration.Core.TcpServerUrls != null && Logger.IsInfoEnabled)
                        Logger.Info($"RavenDB TCP is configured to use {string.Join(", ", Configuration.Core.TcpServerUrls)} and bind to {ipAddress} at {port}");

                    var listener = new TcpListener(ipAddress, status.Port != 0 ? status.Port : port);
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

        public IPAddress[] GetListenIpAddresses(string host)
        {
            if (IPAddress.TryParse(host, out IPAddress ipAddress))
                return new[] { ipAddress };

            switch (host)
            {
                case "localhost.fiddler":
                    return GetListenIpAddresses("localhost");
                default:
                    try
                    {
                        var ipHostEntry = Dns.GetHostEntry(host);

                        if (ipHostEntry.AddressList.Length == 0)
                            throw new InvalidOperationException("The specified tcp server hostname has no entries: " + host);
                        return ipHostEntry
                            .AddressList
                            .Where(x => x.IsIPv6LinkLocal == false)
                            .ToArray();
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
                        PinnedBuffer = JsonOperationContext.ManagedPinnedBuffer.LongLivedInstance()
                    };

                    try
                    {
                        TcpConnectionHeaderMessage header = null;
                        string error = null;
                        int version = 0;
                        bool matchingVersion = false;
                        using (_tcpContextPool.AllocateOperationContext(out JsonOperationContext context))
                        {
                            int retries = TcpConnectionHeaderMessage.NumberOfRetriesForSendingTcpHeader;
                            while (retries > 0 )
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
                                    //In the case where we have missmatched version but the other side doesn't know how to handle it.
                                    if (header.Operation == TcpConnectionHeaderMessage.OperationTypes.Drop)
                                    {
                                        if (Logger.IsInfoEnabled)
                                        {
                                            Logger.Info(
                                                $"Got a request to drop TCP connection to {header.DatabaseName ?? "the cluster node"} from {tcpClient.Client.RemoteEndPoint} reason:{header.Info}");
                                        }
                                        return;
                                    }
                                }
                                matchingVersion = MatchingOperationVersion(header, out error, out version);
                                if (matchingVersion == false)
                                {
                                    RespondToTcpConnection(stream, context, error, TcpConnectionStatus.TcpVersionMismatch, version);
                                    if (Logger.IsInfoEnabled)
                                    {
                                        Logger.Info(
                                            $"New {header.Operation} TCP connection to {header.DatabaseName ?? "the cluster node"} from {tcpClient.Client.RemoteEndPoint} failed because:" +
                                            $" {error} retries left:{retries}");
                                    }
                                    retries--;
                                    continue;
                                }
                                break;
                            }
                            if (matchingVersion == false)
                            {
                                //Couldn't agree on tcp operation version, will drop the connection now.
                                return;
                            }
                            bool authSuccessful = TryAuthorize(Configuration, tcp.Stream, header, out var err);
                            //At this stage the error is not relevant.
                            RespondToTcpConnection(stream, context, null, authSuccessful ? TcpConnectionStatus.Ok : TcpConnectionStatus.AuthorizationFailed, version);

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

        private static void RespondToTcpConnection(Stream stream, JsonOperationContext context, string error, TcpConnectionStatus status,int version)
        {
            using (var writer = new BlittableJsonTextWriter(context, stream))
            {
                writer.WriteStartObject();
                writer.WritePropertyName(nameof(TcpConnectionHeaderResponse.Status));
                writer.WriteString(status.ToString());
                writer.WriteComma();
                writer.WritePropertyName(nameof(TcpConnectionHeaderResponse.Version));
                writer.WriteInteger(version);
                if (error != null)
                {
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(TcpConnectionHeaderResponse.Message));
                    writer.WriteString(error);
                }
                writer.WriteEndObject();
                writer.Flush();
            }
        }

        private bool MatchingOperationVersion(TcpConnectionHeaderMessage header, out string error,out int version)
        {
            version = TcpConnectionHeaderMessage.GetOperationTcpVersion(header.Operation);
            if (version == header.OperationVersion)
            {
                error = null;
                return true;
            }
            error = $"Message of type {header.Operation} version should be {version} but got a message with version {header.OperationVersion}";
            return false;
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

        // This is used for admin scripts only
        public readonly ScriptRunnerCache AdminScripts;

        private TcpListenerStatus _tcpListenerStatus;
        private SnmpWatcher _snmpWatcher;
        private Timer _refreshClusterCertificate;
        private HttpsConnectionAdapter _httpsConnectionAdapter;
        public (IPAddress[] Addresses, int Port) ListenEndpoints { get; private set; }

        internal void SetCertificate(X509Certificate2 certificate, byte[] rawBytes, string password)
        {
            var certificateHolder = Certificate;
            var newCertHolder = SecretProtection.ValidateCertificateAndCreateCertificateHolder("Auto Update", certificate, rawBytes, password);
            if (Interlocked.CompareExchange(ref Certificate, newCertHolder, certificateHolder) == certificateHolder)
            {
                _httpsConnectionAdapter.SetCertificate(certificate);
                if (newCertHolder.CertificateForClients != null)
                {
                    try
                    {
                        CertificateUtils.RegisterCertificateInOperatingSystem(new X509Certificate2(Convert.FromBase64String(newCertHolder.CertificateForClients)));
                    }
                    catch (Exception e)
                    {
                        if (Logger.IsOperationsEnabled)
                            Logger.Operations($"Failed to register new certificate in the operating system", e);
                    }
                }
            }
        }

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
            if (Certificate.Certificate != null)
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

                await sslStream.AuthenticateAsServerAsync(Certificate.Certificate, true, SslProtocols.Tls12, false);
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
                case AuthenticationStatus.ClusterAdmin:
                case AuthenticationStatus.Operator:
                    msg = "Admin can do it all";
                    return true;
                case AuthenticationStatus.Allowed:
                    switch (header.Operation)
                    {
                        case TcpConnectionHeaderMessage.OperationTypes.Cluster:
                        case TcpConnectionHeaderMessage.OperationTypes.Heartbeats:
                            msg = header.Operation + " is a server wide operation and the certificate " + certificate.FriendlyName + "is not ClusterAdmin/Operator";
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
        public MetricCounters Metrics { get; }

        public bool Disposed { get; private set; }

        internal NamedPipeServerStream AdminConsolePipe { get; set; }

        internal NamedPipeServerStream LogStreamPipe { get; set; }

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

                ea.Execute(() => _refreshClusterCertificate?.Dispose());
                ea.Execute(() => AdminConsolePipe?.Dispose());
                ea.Execute(() => LogStreamPipe?.Dispose());
                ea.Execute(() => Metrics?.Dispose());
                ea.Execute(() => _webHost?.Dispose());
                if (_tcpListenerStatus != null)
                {
                    ea.Execute(() => CloseTcpListeners(_tcpListenerStatus.Listeners));
                }

                ea.Execute(() => ServerStore?.Dispose());
                ea.Execute(() =>
                {
                    try
                    {
                        _currentRefreshTask?.Wait();
                    }
                    catch (OperationCanceledException)
                    {
                    }
                    catch (AggregateException ae) when (ae.InnerException is OperationCanceledException)
                    {
                    }
                });
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

        public void OpenPipes()
        {
            Pipes.CleanupOldPipeFiles();
            LogStreamPipe = Pipes.OpenLogStreamPipe();
            AdminConsolePipe = Pipes.OpenAdminConsolePipe();
        }

        public enum AuthenticationStatus
        {
            None,
            NoCertificateProvided,
            UnfamiliarCertificate,
            Allowed,
            Operator,
            ClusterAdmin,
            Expired,
            NotYetValid
        }
    }
}
