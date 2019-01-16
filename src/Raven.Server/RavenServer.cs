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
using System.Security.Claims;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Microsoft.AspNetCore.ResponseCompression;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;
using Org.BouncyCastle.Pkcs;
using Raven.Client;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Security;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.ServerWide.Tcp;
using Raven.Client.Util;
using Raven.Server.Commercial;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Https;
using Raven.Server.Json;
using Raven.Server.Monitoring.Snmp;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Rachis;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Maintenance;
using Raven.Server.Utils;
using Raven.Server.Utils.Cpu;
using Raven.Server.Web.ResponseCompression;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Platform;

namespace Raven.Server
{
    public class RavenServer : IDisposable
    {
        static RavenServer()
        {
            UnhandledExceptions.Track(Logger);
        }

        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<RavenServer>("Server");

        public readonly RavenConfiguration Configuration;

        public Timer ServerMaintenanceTimer;

        public SystemTime Time = new SystemTime();

        public readonly ServerStore ServerStore;

        private IWebHost _webHost;

        private IWebHost _redirectingWebHost;

        private readonly Logger _tcpLogger;

        public event Action AfterDisposal;

        public readonly ServerStatistics Statistics;

        public event EventHandler ServerCertificateChanged;

        public ICpuUsageCalculator CpuUsageCalculator;

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

            _tcpLogger = LoggingSource.Instance.GetLogger<RavenServer>("Server/TCP");
        }

        public TcpListenerStatus GetTcpServerStatus()
        {
            return _tcpListenerStatus;
        }

        public void Initialize()
        {
            var sp = Stopwatch.StartNew();
            Certificate = LoadCertificate() ?? new CertificateHolder();

            CpuUsageCalculator = string.IsNullOrEmpty(Configuration.Monitoring.CpuUsageMonitorExec) 
                ? CpuHelper.GetOSCpuUsageCalculator()
                : CpuHelper.GetExtensionPointCpuUsageCalculator(_tcpContextPool, Configuration.Monitoring, ServerStore.NotificationCenter) ;

            CpuUsageCalculator.Init();

            if (Logger.IsInfoEnabled)
                Logger.Info(string.Format("Server store started took {0:#,#;;0} ms", sp.ElapsedMilliseconds));

            sp.Restart();
            ListenToPipes().IgnoreUnobservedExceptions();
            Router = new RequestRouter(RouteScanner.AllRoutes, this);

            try
            {
                ListenEndpoints = GetServerAddressesAndPort();

                void ConfigureKestrel(KestrelServerOptions options)
                {
                    options.Limits.MaxRequestLineSize = (int)Configuration.Http.MaxRequestLineSize.GetValue(SizeUnit.Bytes);
                    options.Limits.MaxRequestBodySize = null; // no limit!
                    options.Limits.MinResponseDataRate = null; // no limit!
                    options.Limits.MinRequestBodyDataRate = null; // no limit!

                    if (Configuration.Http.MinDataRatePerSecond.HasValue && Configuration.Http.MinDataRateGracePeriod.HasValue)
                    {
                        options.Limits.MinResponseDataRate = new MinDataRate(Configuration.Http.MinDataRatePerSecond.Value.GetValue(SizeUnit.Bytes),
                            Configuration.Http.MinDataRateGracePeriod.Value.AsTimeSpan);
                        options.Limits.MinRequestBodyDataRate = new MinDataRate(Configuration.Http.MinDataRatePerSecond.Value.GetValue(SizeUnit.Bytes),
                            Configuration.Http.MinDataRateGracePeriod.Value.AsTimeSpan);
                    }

                    if (Configuration.Http.MaxRequestBufferSize.HasValue)
                        options.Limits.MaxRequestBufferSize = Configuration.Http.MaxRequestBufferSize.Value.GetValue(SizeUnit.Bytes);

                    if (Certificate.Certificate != null)
                    {
                        _httpsConnectionAdapter = new HttpsConnectionAdapter();
                        _httpsConnectionAdapter.SetCertificate(Certificate.Certificate);
                        _refreshClusterCertificate = new Timer(RefreshClusterCertificateTimerCallback);
                        var adapter = new AuthenticatingAdapter(this, _httpsConnectionAdapter);

                        foreach (var address in ListenEndpoints.Addresses)
                        {
                            options.Listen(address, ListenEndpoints.Port, listenOptions => { listenOptions.ConnectionAdapters.Add(adapter); });
                        }
                    }
                }

                var webHostBuilder = new WebHostBuilder()
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

                            services.Configure<GzipCompressionProviderOptions>(options => { options.Level = Configuration.Http.GzipResponseCompressionLevel; });

                            services.Configure<DeflateCompressionProviderOptions>(options => { options.Level = Configuration.Http.DeflateResponseCompressionLevel; });

                            services.AddResponseCompression();
                        }

                        services.AddSingleton(Router);
                        services.AddSingleton(this);
                        services.Configure<FormOptions>(options => { options.MultipartBodyLengthLimit = long.MaxValue; });
                    });

                if (Configuration.Http.UseLibuv)
                    webHostBuilder = webHostBuilder.UseLibuv();

                _webHost = webHostBuilder.Build();
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

                if (Certificate.Certificate != null)
                {
                    try
                    {
                        AssertServerCanContactItselfWhenAuthIsOn(Certificate.Certificate)
                            .IgnoreUnobservedExceptions()
                            // here we wait a bit, just enough so for normal servers
                            // we'll be successful, but not enough to hang the server
                            // startup if there is some issue talking to the node because
                            // of firewall, ssl issues, etc. 
                            .Wait(250);
                    }
                    catch (Exception)
                    {
                        // the .Wait() can throw as well, so we'll ignore any
                        // errors here, it all goes to the log anyway
                    }

                    var port = new Uri(Configuration.Core.ServerUrls[0]).Port;
                    if (port == 443 && Configuration.Security.DisableHttpsRedirection == false)
                    {
                        RedirectsHttpTrafficToHttps();
                    }

                    SecretProtection.AddCertificateChainToTheUserCertificateAuthorityStoreAndCleanExpiredCerts(Certificate.Certificate, Certificate.Certificate.Export(X509ContentType.Cert), Configuration.Security.CertificatePassword);
                }

                if (Logger.IsInfoEnabled)
                    Logger.Info($"Initialized Server... {WebUrl}");

                _tcpListenerStatus = StartTcpListener();

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

        private void RedirectsHttpTrafficToHttps()
        {
            try
            {
                var serverUrlsToRedirect = Configuration.Core.ServerUrls.Select(serverUrl => new Uri(serverUrl))
                    .Select(newUri => new UriBuilder(newUri)
                    {
                        Scheme = "http",
                        Port = 80
                    }.Uri.ToString())
                    .ToArray();

                if (Logger.IsOperationsEnabled)
                    Logger.Operations($"HTTPS is on. Setting up a new web host to redirect incoming HTTP traffic on port 80 to HTTPS on port 443. The new web host is listening to { string.Join(", ", serverUrlsToRedirect) }");

                var webHostBuilder = new WebHostBuilder()
                    .UseKestrel()
                    .UseUrls(serverUrlsToRedirect)
                    .UseStartup<RedirectServerStartup>()
                    .UseShutdownTimeout(TimeSpan.FromSeconds(1));

                if (Configuration.Http.UseLibuv)
                    webHostBuilder = webHostBuilder.UseLibuv();

                _redirectingWebHost = webHostBuilder.Build();

                _redirectingWebHost.Start();
            }
            catch (Exception e)
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations("Failed to create a webhost to redirect HTTP traffic to HTTPS", e);
            }
        }

        private async Task AssertServerCanContactItselfWhenAuthIsOn(X509Certificate2 certificateCertificate)
        {
            var url = Configuration.Core.PublicServerUrl.HasValue ? Configuration.Core.PublicServerUrl.Value.UriValue : WebUrl;

            try
            {
                using (var httpMessageHandler = new HttpClientHandler())
                {
                    httpMessageHandler.SslProtocols = SslProtocols.Tls12;

                    if (Logger.IsOperationsEnabled)
                        Logger.Operations($"During server initialization, validating that the server can authenticate with itself using {url}.");

                    // Using the server certificate as a client certificate to test if we can talk to ourselves
                    httpMessageHandler.ClientCertificates.Add(certificateCertificate);
                    using (var client = new HttpClient(httpMessageHandler)
                    {
                        BaseAddress = new Uri(url),
                        Timeout = TimeSpan.FromSeconds(15)
                    })
                    {
                        await client.GetAsync("/setup/alive");
                    }
                }
            }
            catch (Exception e)
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations($"Server failed to contact itself @ {url}. " +
                                      $"This can happen if PublicServerUrl is not the same as the domain in the certificate or you have other certificate errors. " +
                                      "Trying again, this time with a RemoteCertificateValidationCallback which allows connections with the same certificate.", e);

                try
                {
                    using (var httpMessageHandler = new HttpClientHandler())
                    {
                        // Try again, this time the callback should allow the connection.
                        httpMessageHandler.ServerCertificateCustomValidationCallback += CertificateCallback;
                        httpMessageHandler.SslProtocols = SslProtocols.Tls12;
                        httpMessageHandler.ClientCertificates.Add(certificateCertificate);

                        using (var client = new HttpClient(httpMessageHandler)
                        {
                            BaseAddress = new Uri(url),
                            Timeout = TimeSpan.FromSeconds(15)
                        })
                        {
                            var response = await client.GetAsync("/setup/alive");

                            if (response.IsSuccessStatusCode)
                            {
                                // It worked, let's register this callback globally in the RequestExecutor
                                if (RequestExecutor.HasServerCertificateCustomValidationCallback == false)
                                {
                                    RequestExecutor.RemoteCertificateValidationCallback += CertificateCallback;
                                }
                            }

                            if (Logger.IsOperationsEnabled)
                                Logger.Operations($"Successful connection with RemoteCertificateValidationCallback to {url}.");
                        }
                    }
                }
                catch (Exception e2)
                {
                    if (Logger.IsOperationsEnabled)
                        Logger.Operations($"Server failed to contact itself @ {url} even though RemoteCertificateValidationCallback allows connections with the same certificate.", e2);
                }
            }
        }

        bool CertificateCallback(object sender, X509Certificate cert, X509Chain chain, SslPolicyErrors errors)
        {
            if (errors == SslPolicyErrors.None || errors == SslPolicyErrors.RemoteCertificateChainErrors) // self-signed is acceptable
                return true;

            var cert2 = HttpsConnectionAdapter.ConvertToX509Certificate2(cert);
            // We trust ourselves
            return cert2?.Thumbprint == Certificate?.Certificate?.Thumbprint;
        }

        private Task _currentRefreshTask = Task.CompletedTask;

        public Task RefreshTask => _currentRefreshTask;

        public void RefreshClusterCertificateTimerCallback(object state)
        {
            RefreshClusterCertificate(state);
        }

        public bool RefreshClusterCertificate(object state)
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
            if (currentCertificate.Certificate == null)
            {
                return false; // shouldn't happen, but just in case
            }

            var forceRenew = state as bool? ?? false;

            var currentRefreshTask = _currentRefreshTask;
            if (currentRefreshTask.IsCompleted == false)
            {
                _refreshClusterCertificate?.Change(TimeSpan.FromMinutes(1), TimeSpan.FromHours(1));
                return false;
            }

            var refreshCertificate = new Task(async () => { await DoActualCertificateRefresh(currentCertificate, forceRenew: forceRenew); });
            if (Interlocked.CompareExchange(ref _currentRefreshTask, currentRefreshTask, refreshCertificate) != currentRefreshTask)
                return false;

            refreshCertificate.Start();

            return true;
        }

        private async Task DoActualCertificateRefresh(CertificateHolder currentCertificate, bool forceRenew = false)
        {
            try
            {
                CertificateHolder newCertificate;
                var msg = "Tried to load certificate as part of refresh check, and got a null back, but got a valid certificate on startup!";
                try
                {
                    newCertificate = LoadCertificate();
                    if (newCertificate == null)
                    {
                        if (Logger.IsOperationsEnabled)
                            Logger.Operations(msg);

                        ServerStore.NotificationCenter.Add(AlertRaised.Create(
                            null,
                            CertificateReplacement.CertReplaceAlertTitle,
                            msg,
                            AlertType.Certificates_ReplaceError,
                            NotificationSeverity.Error));
                        return;
                    }
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Tried to load certificate as part of refresh check, but got an error!", e);
                }

                if (newCertificate.Certificate.Thumbprint != currentCertificate.Certificate.Thumbprint)
                {
                    if (Interlocked.CompareExchange(ref Certificate, newCertificate, currentCertificate) == currentCertificate)
                        _httpsConnectionAdapter.SetCertificate(newCertificate.Certificate);
                    ServerCertificateChanged?.Invoke(this, EventArgs.Empty);
                    return;
                }

                if (Configuration.Core.SetupMode != SetupMode.LetsEncrypt)
                    return;

                if (ServerStore.IsLeader() == false)
                    return;

                if (ClusterCommandsVersionManager.ClusterCommandsVersions.TryGetValue(nameof(ConfirmServerCertificateReplacedCommand), out var commandVersion) == false)
                    throw new InvalidOperationException($"Failed to get the command version of '{nameof(ConfirmServerCertificateReplacedCommand)}'.");

                if (ClusterCommandsVersionManager.CurrentClusterMinimalVersion < commandVersion)
                    throw new ClusterNodesVersionMismatchException("It is not possible to refresh/replace the cluster certificate in the current cluster topology. Please make sure that all the cluster nodes have an equal or newer version than the command version." +
                                                                   $"Cluster Version: {ClusterCommandsVersionManager.CurrentClusterMinimalVersion}, Command Version: {commandVersion}.");

                // we need to see if there is already an ongoing process
                using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var certUpdate = ServerStore.Cluster.GetItem(context, CertificateReplacement.CertificateReplacementDoc);
                    if (certUpdate != null)
                    {
                        if (certUpdate.TryGet(nameof(CertificateReplacement.Confirmations), out int confirmations) == false)
                            throw new InvalidOperationException($"Expected to get '{nameof(CertificateReplacement.Confirmations)}' count");

                        var nodesInCluster = ServerStore.GetClusterTopology(context).AllNodes.Count;
                        if (nodesInCluster > confirmations)
                        {
                            // we are already in the process of updating the certificate, so we need
                            // to nudge all the nodes in the cluster to check the replacement state.
                            // If a node confirmed but failed with the actual replacement (e.g. file permissions)
                            // this will make sure it will try again in the next round (1 hour).
                            await ServerStore.SendToLeaderAsync(new RecheckStatusOfServerCertificateCommand());
                            return;
                        }

                        if (certUpdate.TryGet(nameof(CertificateReplacement.Replaced), out int replaced) == false)
                            replaced = 0;

                        if (nodesInCluster > replaced)
                        {
                            // This is for the case where all nodes confirmed they received the replacement cert but
                            // not all nodes have made the actual change yet.
                            await ServerStore.SendToLeaderAsync(new RecheckStatusOfServerCertificateReplacementCommand());
                        }

                        return;
                    }
                }

                // same certificate, but now we need to see if we need to auto update it
                var (shouldRenew, renewalDate) = CalculateRenewalDate(currentCertificate, forceRenew);
                if (shouldRenew == false)
                {
                    // We don't want an alert here, this happens frequently.
                    if (Logger.IsOperationsEnabled)
                        Logger.Operations($"Renew check: still have time left to renew the server certificate with thumbprint `{currentCertificate.Certificate.Thumbprint}`, estimated renewal date: {renewalDate}");
                    return;
                }                    
                
                if (ServerStore.LicenseManager.GetLicenseStatus().Type == LicenseType.Developer && forceRenew == false)
                {
                    msg = "It's time to renew your Let's Encrypt server certificate but automatic renewal is turned off when using the developer license. Go to the certificate page in the studio and trigger the renewal manually.";
                    ServerStore.NotificationCenter.Add(AlertRaised.Create(
                        null,
                        CertificateReplacement.CertReplaceAlertTitle,
                        msg,
                        AlertType.Certificates_DeveloperLetsEncryptRenewal,
                        NotificationSeverity.Warning));

                    if (Logger.IsOperationsEnabled)
                        Logger.Operations(msg);
                    return;
                }

                byte[] newCertBytes;
                try
                {
                    newCertBytes = await RenewLetsEncryptCertificate(currentCertificate);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Failed to update certificate from Lets Encrypt", e);
                }

                await StartCertificateReplicationAsync(Convert.ToBase64String(newCertBytes), false);
            }
            catch (Exception e)
            {
                var msg = "Failed to replace the server certificate.";
                if (Logger.IsOperationsEnabled)
                    Logger.Operations(msg, e);

                ServerStore.NotificationCenter.Add(AlertRaised.Create(
                    null,
                    CertificateReplacement.CertReplaceAlertTitle,
                    msg,
                    AlertType.Certificates_ReplaceError,
                    NotificationSeverity.Error,
                    details: new ExceptionDetails(e)));
            }
        }

        public (bool ShouldRenew, DateTime RenewalDate) CalculateRenewalDate(CertificateHolder currentCertificate, bool forceRenew)
        {
            // we want to setup all the renewals for Saturdays, 30 days before expiration. This is done to reduce the amount of cert renewals that are counted against our renewals
            // but if we have less than 20 days or user asked to force-renew, we'll try anyway.

            if (forceRenew)
                return (true, DateTime.UtcNow);

            var remainingDays = (currentCertificate.Certificate.NotAfter - Time.GetUtcNow().ToLocalTime()).TotalDays;
            if (remainingDays <= 20)
            {
                return (true, DateTime.UtcNow);
            }

            var firstPossibleDate = currentCertificate.Certificate.NotAfter.ToUniversalTime().AddDays(-30);
            
            // We can do this because saturday is last in the DayOfWeek enum
            var daysUntilSaturday = DayOfWeek.Saturday - firstPossibleDate.DayOfWeek; 
            var firstPossibleSaturday = firstPossibleDate.AddDays(daysUntilSaturday);

            if (firstPossibleSaturday.Date == DateTime.Today)
                return (true, firstPossibleSaturday);

            return (false, firstPossibleSaturday);
        }

        public async Task StartCertificateReplicationAsync(string base64CertWithoutPassword, bool replaceImmediately)
        {
            // We assume that at this point, the password was already stripped out of the certificate.

            // the process of updating a new certificate is the same as deleting a database
            // we first send the certificate to all the nodes, then we get acknowledgments
            // about that from them, and we replace only when they are confirmed to have been
            // successful. However, if we have less than 3 days for renewing the cert or if 
            // replaceImmediately is true, we'll replace immediately

            try
            {
                byte[] certBytes;
                try
                {
                    certBytes = Convert.FromBase64String(base64CertWithoutPassword);
                }
                catch (Exception e)
                {
                    throw new ArgumentException($"Unable to parse the {nameof(base64CertWithoutPassword)} property, expected a Base64 value", e);
                }

                X509Certificate2 newCertificate;
                try
                {
                    newCertificate = new X509Certificate2(certBytes, (string)null, X509KeyStorageFlags.Exportable | X509KeyStorageFlags.PersistKeySet | X509KeyStorageFlags.MachineKeySet);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Failed to load the new certificate.", e);
                }

                if (Logger.IsOperationsEnabled)
                    Logger.Operations("Got new certificate from Lets Encrypt! Starting certificate replication.");

                // During replacement of a cluster certificate, we must have both the new and the old server certificates registered in the server store.
                // This is needed for trust in the case where a node replaced its own certificate while another node still runs with the old certificate.
                // Since both nodes use different certificates, they will only trust each other if the certs are registered in the server store.
                // When the certificate replacement is finished throughout the cluster, we will delete both these entries.
                await ServerStore.PutValueInClusterAsync(new PutCertificateCommand(Constants.Certificates.Prefix + newCertificate.Thumbprint, new CertificateDefinition
                {
                    Certificate = Convert.ToBase64String(Certificate.Certificate.Export(X509ContentType.Cert)),
                    Thumbprint = Certificate.Certificate.Thumbprint,
                    NotAfter = Certificate.Certificate.NotAfter,
                    Name = "Old Server Certificate - can delete",
                    SecurityClearance = SecurityClearance.ClusterNode
                }));

                var res = await ServerStore.PutValueInClusterAsync(new PutCertificateCommand(Constants.Certificates.Prefix + newCertificate.Thumbprint, new CertificateDefinition
                {
                    Certificate = Convert.ToBase64String(newCertificate.Export(X509ContentType.Cert)),
                    Thumbprint = newCertificate.Thumbprint,
                    NotAfter = newCertificate.NotAfter,
                    Name = "Server Certificate",
                    SecurityClearance = SecurityClearance.ClusterNode
                }));

                await ServerStore.Cluster.WaitForIndexNotification(res.Index);

                await ServerStore.SendToLeaderAsync(new InstallUpdatedServerCertificateCommand
                {
                    Certificate = base64CertWithoutPassword, // includes the private key
                    ReplaceImmediately = replaceImmediately
                });
            }
            catch (Exception e)
            {
                var msg = "Failed to start certificate replication.";
                if (Logger.IsOperationsEnabled)
                    Logger.Operations(msg, e);

                ServerStore.NotificationCenter.Add(AlertRaised.Create(
                    null,
                    CertificateReplacement.CertReplaceAlertTitle,
                    msg,
                    AlertType.Certificates_ReplaceError,
                    NotificationSeverity.Error,
                    details: new ExceptionDetails(e)));
            }
        }

        private async Task<byte[]> RenewLetsEncryptCertificate(CertificateHolder existing)
        {
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

            string usedRootDomain = null;
            foreach (var rd in userDomainsResult.RootDomains)
            {
                if (Configuration.Core.PublicServerUrl.HasValue && Configuration.Core.PublicServerUrl.Value.UriValue.IndexOf(rd, StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    usedRootDomain = rd;
                    break;
                }
            }

            if (usedRootDomain == null)
            {
                if (Configuration.Core.PublicServerUrl.HasValue)
                    throw new InvalidOperationException($"Your license is associated with the following domains: {string.Join(",", userDomainsResult.RootDomains)} " +
                                                        $"but the PublicServerUrl configuration setting is: {Configuration.Core.PublicServerUrl.Value.UriValue}." +
                                                        "There is a mismatch, therefore cannot automatically renew the Lets Encrypt certificate. Please contact support.");

                throw new InvalidOperationException("PublicServerUrl is empty. Cannot automatically renew the Lets Encrypt certificate. Please contact support.");
            }

            if (userDomainsResult.Emails.Contains(Configuration.Security.CertificateLetsEncryptEmail, StringComparer.OrdinalIgnoreCase) == false)
                throw new InvalidOperationException($"Your license is associated with the following emails: {string.Join(",", userDomainsResult.Emails)} " +
                                                    $"but the Security.Certificate.LetsEncrypt.Email configuration setting is: {Configuration.Security.CertificateLetsEncryptEmail}." +
                                                    "There is a mismatch, therefore cannot automatically renew the Lets Encrypt certificate. Please contact support.");

            var hosts = SetupManager.GetCertificateAlternativeNames(existing.Certificate).ToArray();
            var substring = hosts[0].Substring(0, hosts[0].Length - usedRootDomain.Length - 1);
            var domainEnd = substring.LastIndexOf('.');
            var domain = substring.Substring(domainEnd + 1);

            if (userDomainsResult.Domains.Any(userDomain => string.Equals(userDomain.Key, domain, StringComparison.OrdinalIgnoreCase)) == false)
                throw new InvalidOperationException("The license provided does not have access to the domain: " + domain);

            var setupInfo = new SetupInfo
            {
                Domain = domain,
                RootDomain = usedRootDomain,
                ModifyLocalServer = false, // N/A here
                RegisterClientCert = false, // N/A here
                Password = null,
                Certificate = null,
                License = license,
                Email = Configuration.Security.CertificateLetsEncryptEmail,
                NodeSetupInfos = new Dictionary<string, SetupInfo.NodeInfo>()
            };

            var fullDomainPortion = domain + "." + usedRootDomain;

            foreach (var host in hosts) // we just need the keys here
            {
                var key = host.Substring(0, host.Length - fullDomainPortion.Length - 1);
                setupInfo.NodeSetupInfos[key] = new SetupInfo.NodeInfo();
            }

            var cert = await SetupManager.RefreshLetsEncryptTask(setupInfo, ServerStore, ServerStore.ServerShutdown);
            var certBytes = Convert.FromBase64String(setupInfo.Certificate);

            SecretProtection.ValidateCertificateAndCreateCertificateHolder("Let's Encrypt Refresh", cert, certBytes,
                setupInfo.Password, ServerStore);

            return certBytes;
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
                    return ServerStore.Secrets.LoadCertificateFromPath(Configuration.Security.CertificatePath, Configuration.Security.CertificatePassword, ServerStore);
                if (string.IsNullOrEmpty(Configuration.Security.CertificateExec) == false)
                    return ServerStore.Secrets.LoadCertificateWithExecutable(Configuration.Security.CertificateExec, Configuration.Security.CertificateExecArguments, ServerStore);

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
            public int WrittenToAuditLog;

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

            public AuthenticationStatus StatusForAudit => _status;

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
            var authenticationStatus = new AuthenticateConnection
            {
                Certificate = certificate
            };
            var wellKnown = Configuration.Security.WellKnownAdminCertificates;
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
            else if (wellKnown != null && wellKnown.Contains(certificate.Thumbprint, StringComparer.OrdinalIgnoreCase))
            {
                authenticationStatus.Status = AuthenticationStatus.ClusterAdmin;
            }
            else
            {
                using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var certKey = Constants.Certificates.Prefix + certificate.Thumbprint;
                    var cert = ServerStore.Cluster.Read(ctx, certKey) ??
                               ServerStore.Cluster.GetLocalState(ctx, certKey);

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
            SnmpWatcher = new SnmpWatcher(this);
            SnmpWatcher.Execute();
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
                    try
                    {
                        listener.Start();
                    }
                    catch (Exception ex)
                    {
                        var msg = $"Unable to start tcp listener on {ipAddress} on port {port}.{ Environment.NewLine}"+
                        $"Port might be already in use.{ Environment.NewLine}"+ 
                        $"Try running with an unused TCP port.{Environment.NewLine}" +
                        $"You can change the TCP port using one of the following options:{Environment.NewLine}" +
                        $"1) Change the ServerUrl.Tcp property in setting.json file.{Environment.NewLine}" +
                        $"2) Run the server from the command line with --ServerUrl.Tcp option.{Environment.NewLine}" +
                        $"3) Add RAVEN_ServerUrl_Tcp to the Environment Variables.{Environment.NewLine}" +
                        "For more information go to https://ravendb.net/l/EJS81M/4.1";

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
                Logger tcpAuditLog = null;
                try
                {
                    tcpClient = await listener.AcceptTcpClientAsync();
                    tcpAuditLog = LoggingSource.AuditLog.IsInfoEnabled ? LoggingSource.AuditLog.GetLogger("TcpConnections", "Audit") : null;
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
                    var sendTimeout = (int)Configuration.Cluster.TcpConnectionTimeout.AsTimeSpan.TotalMilliseconds;

                    DebuggerAttachedTimeout.SendTimeout(ref sendTimeout);
                    tcpClient.ReceiveTimeout = tcpClient.SendTimeout = sendTimeout;

                    Stream stream = tcpClient.GetStream();
                    X509Certificate2 cert;

                    (stream, cert) = await AuthenticateAsServerIfSslNeeded(stream);

                    using (_tcpContextPool.AllocateOperationContext(out JsonOperationContext ctx))
                    using (ctx.GetManagedBuffer(out var buffer))
                    {
                        var tcp = new TcpConnectionOptions
                        {
                            ContextPool = _tcpContextPool,
                            Stream = stream,
                            TcpClient = tcpClient,
                        };

                        try
                        {
                            TcpConnectionHeaderMessage header;
                            int count = 0, maxRetries = 100;
                            using (_tcpContextPool.AllocateOperationContext(out JsonOperationContext context))
                            {
                                int supported;
                                while (true)
                                {
                                    using (var headerJson = await context.ParseToMemoryAsync(
                                        stream,
                                        "tcp-header",
                                        BlittableJsonDocumentBuilder.UsageMode.None,
                                        buffer,
                                        ServerStore.ServerShutdown,
                                        // we don't want to allow external (and anonymous) users to send us unlimited data
                                        // a maximum of 2 KB for the header is big enough to include any valid header that
                                        // we can currently think of
                                        maxSize: 1024 * 2
                                    ))
                                    {
                                        if (count++ > maxRetries)
                                        {
                                            throw new InvalidOperationException($"TCP negotiation dropped after reaching {maxRetries} retries, header:{headerJson}, this is probably a bug.");
                                        }
                                        header = JsonDeserializationClient.TcpConnectionHeaderMessage(headerJson);

                                        if (Logger.IsInfoEnabled)
                                        {
                                            Logger.Info($"New {header.Operation} TCP connection to {header.DatabaseName ?? "the cluster node"} from {tcpClient.Client.RemoteEndPoint}");
                                        }

                                        //In the case where we have mismatched version but the other side doesn't know how to handle it.
                                        if (header.Operation == TcpConnectionHeaderMessage.OperationTypes.Drop)
                                        {
                                            if (tcpAuditLog != null)
                                                tcpAuditLog.Info($"Got connection from {tcpClient.Client.RemoteEndPoint} with certificate '{cert?.Subject} ({cert?.Thumbprint})'. Dropping connection because: {header.Info}");

                                            if (Logger.IsInfoEnabled)
                                            {
                                                Logger.Info($"Got a request to drop TCP connection to {header.DatabaseName ?? "the cluster node"} " +
                                                            $"from {tcpClient.Client.RemoteEndPoint} reason: {header.Info}");
                                            }
                                            return;
                                        }
                                    }

                                    var status = TcpConnectionHeaderMessage.OperationVersionSupported(header.Operation, header.OperationVersion, out supported);
                                    if (status == TcpConnectionHeaderMessage.SupportedStatus.Supported)
                                        break;

                                    if (status == TcpConnectionHeaderMessage.SupportedStatus.OutOfRange)
                                    {
                                        var msg = $"Protocol '{header.OperationVersion}' for '{header.Operation}' was not found.";
                                        if (tcpAuditLog != null)
                                            tcpAuditLog.Info($"Got connection from {tcpClient.Client.RemoteEndPoint} with certificate '{cert?.Subject} ({cert?.Thumbprint})'. {msg}");

                                        if (Logger.IsInfoEnabled)
                                        {
                                            Logger.Info(
                                                $"Got a request to drop TCP connection to {header.DatabaseName ?? "the cluster node"} from {tcpClient.Client.RemoteEndPoint} " +
                                                $"reason: {msg}");
                                        }

                                        throw new ArgumentException(msg);
                                    }

                                    if (Logger.IsInfoEnabled)
                                    {
                                        Logger.Info(
                                            $"Got a request to establish TCP connection to {header.DatabaseName ?? "the cluster node"} from {tcpClient.Client.RemoteEndPoint} " +
                                            $"Didn't agree on {header.Operation} protocol version: {header.OperationVersion} will request to use version: {supported}.");
                                    }

                                    RespondToTcpConnection(stream, context, $"Not supporting version {header.OperationVersion} for {header.Operation}", TcpConnectionStatus.TcpVersionMismatch, supported);
                                }

                                bool authSuccessful = TryAuthorize(Configuration, tcp.Stream, header, out var err);
                                //At this stage the error is not relevant.
                                RespondToTcpConnection(stream, context, null,
                                    authSuccessful ? TcpConnectionStatus.Ok : TcpConnectionStatus.AuthorizationFailed,
                                    supported);
                                tcp.ProtocolVersion = supported;

                                if (authSuccessful == false)
                                {
                                    if (tcpAuditLog != null)
                                        tcpAuditLog.Info($"Got connection from {tcpClient.Client.RemoteEndPoint} with certificate '{cert?.Subject} ({cert?.Thumbprint})'. Rejecting connection because {err} for {header.Operation} on {header.DatabaseName}.");

                                    if (Logger.IsInfoEnabled)
                                    {
                                        Logger.Info(
                                            $"New {header.Operation} TCP connection to {header.DatabaseName ?? "the cluster node"} from {tcpClient.Client.RemoteEndPoint}" +
                                            $" is not authorized to access {header.DatabaseName ?? "the cluster node"} because {err}");
                                    }

                                    return; // cannot proceed
                                }

                                if (Logger.IsInfoEnabled)
                                {
                                    Logger.Info($"TCP connection from {header.SourceNodeTag ?? tcpClient.Client.RemoteEndPoint.ToString()} " +
                                                $"for '{header.Operation}' is accepted with version {supported}");
                                }
                            }

                            if (tcpAuditLog != null)
                                tcpAuditLog.Info($"Got connection from {tcpClient.Client.RemoteEndPoint} with certificate '{cert?.Subject} ({cert?.Thumbprint})'. Accepted for {header.Operation} on {header.DatabaseName}.");

                            if (await DispatchServerWideTcpConnection(tcp, header, buffer))
                            {
                                tcp = null; //do not keep reference -> tcp will be disposed by server-wide connection handlers
                                return;
                            }

                            await DispatchDatabaseTcpConnection(tcp, header, buffer);
                        }
                        catch (Exception e)
                        {
                            if (_tcpLogger.IsInfoEnabled)
                                _tcpLogger.Info("Failed to process TCP connection run", e);

                            SendErrorIfPossible(tcp, e);
                        }
                        finally
                        {
                            if (tcpAuditLog != null)
                                tcpAuditLog.Info($"Closed TCP connection {tcpClient.Client.RemoteEndPoint} with certificate '{cert?.Subject} ({cert?.Thumbprint})'.");

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
            });
        }
        private static void RespondToTcpConnection(Stream stream, JsonOperationContext context, string error, TcpConnectionStatus status, int version)
        {
            var message = new DynamicJsonValue
            {
                [nameof(TcpConnectionHeaderResponse.Status)] = status.ToString(),
                [nameof(TcpConnectionHeaderResponse.Version)] = version
            };

            if (error != null)
            {
                message[nameof(TcpConnectionHeaderResponse.Message)] = error;
            }

            using (var writer = new BlittableJsonTextWriter(context, stream))
            {
                context.Write(writer, message);
                writer.Flush();
            }
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
        public SnmpWatcher SnmpWatcher;
        private Timer _refreshClusterCertificate;
        private HttpsConnectionAdapter _httpsConnectionAdapter;

        public (IPAddress[] Addresses, int Port) ListenEndpoints { get; private set; }

        internal void SetCertificate(X509Certificate2 certificate, byte[] rawBytes, string password)
        {
            var certificateHolder = Certificate;
            var newCertHolder = SecretProtection.ValidateCertificateAndCreateCertificateHolder("Auto Update", certificate, rawBytes, password, ServerStore);
            if (Interlocked.CompareExchange(ref Certificate, newCertHolder, certificateHolder) == certificateHolder)
            {
                _httpsConnectionAdapter.SetCertificate(certificate);
                ServerCertificateChanged?.Invoke(this, EventArgs.Empty);
            }
        }

        private async Task<bool> DispatchServerWideTcpConnection(TcpConnectionOptions tcp, TcpConnectionHeaderMessage header, JsonOperationContext.ManagedPinnedBuffer buffer)
        {
            tcp.Operation = header.Operation;
            if (tcp.Operation == TcpConnectionHeaderMessage.OperationTypes.Cluster)
            {
                var tcpClient = tcp.TcpClient.Client;
                ServerStore.ClusterAcceptNewConnection(tcp, header,() => tcpClient.Disconnect(false), tcpClient.RemoteEndPoint);
                return true;
            }

            if (tcp.Operation == TcpConnectionHeaderMessage.OperationTypes.Ping)
            {
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
                    buffer
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
                        _clusterMaintenanceWorker = new ClusterMaintenanceWorker(tcp, ServerStore.ServerShutdown, ServerStore, maintenanceHeader.LeaderClusterTag, maintenanceHeader.Term);
                        _clusterMaintenanceWorker.Start();
                    }

                    return true;
                }
            }

            return false;
        }

        private async Task<bool> DispatchDatabaseTcpConnection(TcpConnectionOptions tcp, TcpConnectionHeaderMessage header, JsonOperationContext.ManagedPinnedBuffer bufferToCopy)
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
                    SubscriptionConnection.SendSubscriptionDocuments(tcp, bufferToCopy);
                    break;
                case TcpConnectionHeaderMessage.OperationTypes.Replication:
                    var documentReplicationLoader = tcp.DocumentDatabase.ReplicationLoader;
                    documentReplicationLoader.AcceptIncomingConnection(tcp, bufferToCopy);
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

        private async Task<(Stream Stream, X509Certificate2 Certificate)> AuthenticateAsServerIfSslNeeded(Stream stream)
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
                        // connection because SSL negotiation failed.
                        true);

                await sslStream.AuthenticateAsServerAsync(Certificate.Certificate, true, SslProtocols.Tls12, false);

                return (sslStream, HttpsConnectionAdapter.ConvertToX509Certificate2(sslStream.RemoteCertificate));
            }

            return (stream, null);
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
                        case TcpConnectionHeaderMessage.OperationTypes.TestConnection:
                            if (header.DatabaseName == null)
                            {
                                msg = "Cannot allow access. Database name is empty.";
                                return false;
                            }
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
                ea.Execute(() => _redirectingWebHost?.Dispose());
                ea.Execute(() => _webHost?.Dispose());
                ea.Execute(() => _tcpContextPool?.Dispose());
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
                ea.Execute(() => _clusterMaintenanceWorker?.Dispose());
                ea.Execute(() => CpuUsageCalculator.Dispose());

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
