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
using System.Security.Claims;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Microsoft.AspNetCore.ResponseCompression;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;
using Org.BouncyCastle.Pkcs;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Security;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
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
using Sparrow.Server.Debugging;
using Sparrow.Threading;
using Voron;
using DateTime = System.DateTime;

namespace Raven.Server
{
    public class RavenServer : IDisposable
    {
        static RavenServer()
        {
            DebugStuff.Attach();
            UnhandledExceptions.Track(Logger);
        }

        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<RavenServer>("Server");
        private readonly Logger _authAuditLog = LoggingSource.AuditLog.GetLogger("AuthenticateCertificate", "Audit");
        private TestingStuff _forTestingPurposes;

        public readonly RavenConfiguration Configuration;

        public Timer ServerMaintenanceTimer;

        public SystemTime Time = new SystemTime();

        public readonly ServerStore ServerStore;

        private IWebHost _webHost;

        private IWebHost _redirectingWebHost;

        private readonly Logger _tcpLogger;
        private readonly ExternalCertificateValidator _externalCertificateValidator;
        internal readonly JsonContextPool _tcpContextPool;

        public event Action AfterDisposal;

        public readonly ServerStatistics Statistics;

        public event EventHandler ServerCertificateChanged;

        public ICpuUsageCalculator CpuUsageCalculator;

        internal bool ThrowOnLicenseActivationFailure;

        internal Action<StorageEnvironment> BeforeSchemaUpgrade;

        internal string DebugTag;

        internal CipherSuitesPolicy CipherSuitesPolicy => _httpsConnectionMiddleware?.CipherSuitesPolicy;

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
            MetricCacher = new ServerMetricCacher(this);

            _tcpLogger = LoggingSource.Instance.GetLogger<RavenServer>("Server/TCP");
            _externalCertificateValidator = new ExternalCertificateValidator(this, Logger);
            _tcpContextPool = new JsonContextPool(Configuration.Memory.MaxContextSizeToKeep);
        }

        public TcpListenerStatus GetTcpServerStatus()
        {
            return _tcpListenerStatus;
        }

        public void Initialize()
        {
            var sp = Stopwatch.StartNew();
            Certificate = LoadCertificateAtStartup() ?? new CertificateHolder();

            CpuUsageCalculator = string.IsNullOrEmpty(Configuration.Monitoring.CpuUsageMonitorExec)
                ? CpuHelper.GetOSCpuUsageCalculator()
                : CpuHelper.GetExtensionPointCpuUsageCalculator(_tcpContextPool, Configuration.Monitoring, ServerStore.NotificationCenter);

            CpuUsageCalculator.Init();
            MetricCacher.Initialize();

            if (Logger.IsInfoEnabled)
                Logger.Info($"Server store started took {sp.ElapsedMilliseconds:#,#;;0} ms");

            sp.Restart();
            ListenToPipes().IgnoreUnobservedExceptions();
            Router = new RequestRouter(RouteScanner.AllRoutes, this);

            try
            {
                ListenEndpoints = GetServerAddressesAndPort();

                void ConfigureKestrel(KestrelServerOptions options)
                {
                    options.AllowSynchronousIO = true;

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

                    options.ConfigureEndpointDefaults(listenOptions => listenOptions.Protocols = Configuration.Http.Protocols);

                    if (Certificate.Certificate != null)
                    {
                        _httpsConnectionMiddleware = new HttpsConnectionMiddleware(this, options);
                        _httpsConnectionMiddleware.SetCertificate(Certificate.Certificate);

                        foreach (var address in ListenEndpoints.Addresses)
                        {
                            options.Listen(address, ListenEndpoints.Port, listenOptions =>
                            {
                                listenOptions
                                    .UseHttps()
                                    .Use(_httpsConnectionMiddleware.OnConnectionAsync);
                            });
                        }

                        _refreshClusterCertificate = new Timer(RefreshClusterCertificateTimerCallback);
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
                Logger.Info($"Configuring HTTP server took {sp.ElapsedMilliseconds:#,#;;0} ms");

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

                    ServerCertificateChanged += OnServerCertificateChanged;

                    _externalCertificateValidator.Initialize();

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

                if (Configuration.Server.CpuCreditsBase != null ||
                    Configuration.Server.CpuCreditsMax != null ||
                    Configuration.Server.CpuCreditsExhaustionFailoverThreshold != null ||
                    Configuration.Server.CpuCreditsExhaustionBackgroundTasksThreshold != null ||
                    Configuration.Server.CpuCreditsExec != null)
                {
                    if (Configuration.Server.CpuCreditsBase == null ||
                        Configuration.Server.CpuCreditsMax == null)
                        throw new InvalidOperationException($"Both {RavenConfiguration.GetKey(s => s.Server.CpuCreditsBase)} and {RavenConfiguration.GetKey(s => s.Server.CpuCreditsMax)} must be specified");

                    if (string.IsNullOrEmpty(Configuration.Server.CpuCreditsExec))
                        throw new InvalidOperationException($"CPU credits were configured but missing the {RavenConfiguration.GetKey(s => s.Server.CpuCreditsExec)} key.");

                    CpuCreditsBalance.BaseCredits = Configuration.Server.CpuCreditsBase.Value;
                    CpuCreditsBalance.MaxCredits = Configuration.Server.CpuCreditsMax.Value;
                    CpuCreditsBalance.BackgroundTasksThreshold =
                        // default to 1/4 of the base CPU credits
                        Configuration.Server.CpuCreditsExhaustionBackgroundTasksThreshold ?? CpuCreditsBalance.BaseCredits / 4;
                    CpuCreditsBalance.FailoverThreshold =
                        // default to disabled
                        Configuration.Server.CpuCreditsExhaustionFailoverThreshold ?? -1;

                    // Until we know differently, we don't want to falsely restrict ourselves.
                    CpuCreditsBalance.RemainingCpuCredits = CpuCreditsBalance.MaxCredits;

                    _cpuCreditsMonitoring = PoolOfThreads.GlobalRavenThreadPool.LongRunning(_ =>
                    {
                        try
                        {
                            StartMonitoringCpuCredits();
                        }
                        catch (Exception e)
                        {
                            if (Logger.IsOperationsEnabled)
                                Logger.Operations("Fatal exception occured during cpu credit monitoring", e);
                        }
                    }, null, "CPU Credits Monitoring");
                }

                _refreshClusterCertificate?.Change(TimeSpan.FromMinutes(1), TimeSpan.FromHours(1));
            }
            catch (Exception e)
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations("Could not start server", e);
                throw;
            }
        }

        private void UpdateCertificateExpirationAlert()
        {
            var remainingDays = (Certificate.Certificate.NotAfter - Time.GetUtcNow().ToLocalTime()).TotalDays;
            if (remainingDays <= 0)
            {
                string msg = $"The server certificate has expired on {Certificate.Certificate.NotAfter.ToShortDateString()}.";

                if (Configuration.Core.SetupMode == SetupMode.LetsEncrypt)
                {
                    msg += $" Automatic renewal is no longer possible. Please check the logs for errors and contact support@ravendb.net.";
                }
                
                ServerStore.NotificationCenter.Add(AlertRaised.Create(null, CertificateReplacement.CertReplaceAlertTitle, msg, AlertType.Certificates_Expiration, NotificationSeverity.Error));

                if (Logger.IsOperationsEnabled)
                    Logger.Operations(msg);
            }
            else if (remainingDays <= 20)
            {
                string msg = $"The server certificate will expire on {Certificate.Certificate.NotAfter.ToShortDateString()}. There are only {(int)remainingDays} days left for renewal.";

                if (Configuration.Core.SetupMode == SetupMode.LetsEncrypt)
                {
                    if (ServerStore.LicenseManager.LicenseStatus.CanAutoRenewLetsEncryptCertificate)
                    {
                        msg += " You are using a Let's Encrypt server certificate which was supposed to renew automatically. Please check the logs for errors and contact support@ravendb.net.";
                    }
                    else
                    {
                        msg += " You are using a Let's Encrypt server certificate but automatic renewal is not supported by your license. Go to the certificate page in the studio and trigger the renewal manually.";
                    }
                }

                var severity = remainingDays < 3 ? NotificationSeverity.Error : NotificationSeverity.Warning;

                ServerStore.NotificationCenter.Add(AlertRaised.Create(null, CertificateReplacement.CertReplaceAlertTitle, msg, AlertType.Certificates_Expiration, severity));

                if (Logger.IsOperationsEnabled) 
                    Logger.Operations(msg);
            }
            else
            {
                ServerStore.NotificationCenter.Dismiss(AlertRaised.GetKey(AlertType.Certificates_Expiration, null));
            }
        }

        private void OnServerCertificateChanged(object sender, EventArgs e)
        {
            if (RequestExecutor.HasServerCertificateCustomValidationCallback)
            {
                RequestExecutor.RemoteCertificateValidationCallback -= CertificateCallback;
            }

            try
            {
                AssertServerCanContactItselfWhenAuthIsOn(Certificate.Certificate)
                    .IgnoreUnobservedExceptions()
                    // here we wait a bit, just enough so for normal servers
                    // we'll be successful, but not enough to hang the server
                    // if there is some issue talking to the node because
                    // of firewall, ssl issues, etc.
                    .Wait(250);
            }
            catch (Exception)
            {
                // the .Wait() can throw as well, so we'll ignore any
                // errors here, it all goes to the log anyway
            }

            try
            {
                UpdateCertificateExpirationAlert();
        }
            catch (Exception exception)
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations($"Failed to check the expiration date of the new server certificate '{Certificate.Certificate?.Subject} ({Certificate.Certificate?.Thumbprint})'", exception);
            }
        }

        public void ClearExternalCertificateValidationCache()
        {
            // Can be called from the Admin JS Console
            _externalCertificateValidator?.ClearCache();
        }

        public void ForceSyncCpuCredits()
        {
            CpuCreditsBalance.ForceSync = 1;
        }

        public readonly CpuCreditsState CpuCreditsBalance = new CpuCreditsState();

        public class CpuCreditsState : IDynamicJson
        {
            public bool Used;
            public double BaseCredits;
            public double MaxCredits;
            public double BackgroundTasksThreshold;
            public double FailoverThreshold;
            private double _remainingCpuCredits;
            private  int _forceSync;
            public DateTime LastSyncTime;

            public double RemainingCpuCredits
            {
                get => Interlocked.CompareExchange(ref _remainingCpuCredits, 0, 0); //atomic read of double
                set => Interlocked.Exchange(ref _remainingCpuCredits, value);
            }

            public int ForceSync
            {
                get => Interlocked.CompareExchange(ref _forceSync, 0, 0);
                set => Interlocked.Exchange(ref _forceSync, value);
            }

            public double BackgroundTasksThresholdReleaseValue;
            public double FailoverThresholdReleaseValue;
            public double CreditsGainedPerSecond;
            public double CurrentConsumption;
            public double MachineCpuUsage;
            public double[] History = new double[60 * 60];
            public int HistoryCurrentIndex;

            public MultipleUseFlag BackgroundTasksAlertRaised = new MultipleUseFlag();
            public MultipleUseFlag FailoverAlertRaised = new MultipleUseFlag();

            public DynamicJsonValue ToJson()
            {
                var historyByMinute = new DynamicJsonArray();
                var current = HistoryCurrentIndex;
                int currentMinItems = 0;
                var currentMinuteValue = 0d;
                for (int i = 0; i < History.Length; i++)
                {
                    var val = History[(current + i) % History.Length];
                    currentMinuteValue += val;
                    if (++currentMinItems == 60)
                    {
                        currentMinItems = 0;
                        historyByMinute.Add(currentMinuteValue / 60);
                        currentMinuteValue = 0d;
                    }
                }

                return new DynamicJsonValue
                {
                    [nameof(Used)] = Used,
                    [nameof(BaseCredits)] = BaseCredits,
                    [nameof(MaxCredits)] = MaxCredits,
                    [nameof(FailoverThreshold)] = FailoverThreshold,
                    [nameof(BackgroundTasksThreshold)] = BackgroundTasksThreshold,
                    [nameof(RemainingCpuCredits)] = RemainingCpuCredits,
                    [nameof(BackgroundTasksThresholdReleaseValue)] = BackgroundTasksThresholdReleaseValue,
                    [nameof(FailoverThresholdReleaseValue)] = FailoverThresholdReleaseValue,
                    [nameof(CreditsGainedPerSecond)] = CreditsGainedPerSecond,
                    [nameof(CurrentConsumption)] = CurrentConsumption,
                    [nameof(MachineCpuUsage)] = MachineCpuUsage,
                    [nameof(History)] = historyByMinute,
                    [nameof(LastSyncTime)] = LastSyncTime
                };
            }
        }

        private void StartMonitoringCpuCredits()
        {
            var duringStartup = true;
            CpuCreditsBalance.Used = true;
            CpuCreditsBalance.BackgroundTasksThresholdReleaseValue = CpuCreditsBalance.BackgroundTasksThreshold * 1.25;
            CpuCreditsBalance.FailoverThresholdReleaseValue = CpuCreditsBalance.FailoverThreshold * 1.25;
            CpuCreditsBalance.CreditsGainedPerSecond = CpuCreditsBalance.BaseCredits / 3600;

            int remainingTimeToBackgroundAlert = 15, remainingTimeToFailvoerAlert = 5;
            AlertRaised backgroundTasksAlert = null, failoverAlert = null;

            var sw = Stopwatch.StartNew();
            var startupRetriesSw = Stopwatch.StartNew();
            Stopwatch err = null;

            try
            {
                UpdateCpuCreditsFromExec();
            }
            catch (Exception e)
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations("During CPU credits monitoring, failed to sync the remaining credits.", e);
            }

            while (ServerStore.ServerShutdown.IsCancellationRequested == false)
            {
                try
                {
                    var (overallMachineCpuUsage, _, _) = MetricCacher.GetValue(Raven.Server.Utils.MetricCacher.Keys.Server.CpuUsage, CpuUsageCalculator.Calculate);
                    var utilizationOverAllCores = (overallMachineCpuUsage / 100) * Environment.ProcessorCount;
                    CpuCreditsBalance.CurrentConsumption = utilizationOverAllCores;
                    CpuCreditsBalance.MachineCpuUsage = overallMachineCpuUsage;
                    CpuCreditsBalance.RemainingCpuCredits += CpuCreditsBalance.History[CpuCreditsBalance.HistoryCurrentIndex];
                    CpuCreditsBalance.History[CpuCreditsBalance.HistoryCurrentIndex] = utilizationOverAllCores;

                    CpuCreditsBalance.RemainingCpuCredits -= utilizationOverAllCores; // how much we spent this second
                    CpuCreditsBalance.RemainingCpuCredits += CpuCreditsBalance.CreditsGainedPerSecond; // how much we earned this second

                    if (CpuCreditsBalance.RemainingCpuCredits > CpuCreditsBalance.MaxCredits)
                        CpuCreditsBalance.RemainingCpuCredits = CpuCreditsBalance.MaxCredits;
                    if (CpuCreditsBalance.RemainingCpuCredits < 0)
                        CpuCreditsBalance.RemainingCpuCredits = 0;

                    if (++CpuCreditsBalance.HistoryCurrentIndex >= CpuCreditsBalance.History.Length)
                        CpuCreditsBalance.HistoryCurrentIndex = 0;

                    MaybeRaiseAlert(CpuCreditsBalance.BackgroundTasksThreshold,
                        CpuCreditsBalance.BackgroundTasksThresholdReleaseValue,
                        CpuCreditsBalance.BackgroundTasksAlertRaised,
                        "The CPU credits balance for this instance is nearly exhausted (see /debug/cpu-credits endpoint for details), " +
                        "RavenDB will throttle internal processes to reduce CPU consumption such as indexing, ETL processes and backups.",
                        15,
                        ref backgroundTasksAlert,
                        ref remainingTimeToBackgroundAlert);

                    MaybeRaiseAlert(CpuCreditsBalance.FailoverThreshold,
                        CpuCreditsBalance.FailoverThresholdReleaseValue,
                        CpuCreditsBalance.FailoverAlertRaised,
                        "The CPU credits balance for this instance is nearly exhausted (see /debug/cpu-credits endpoint for details), " +
                        "rejecting requests to databases to alleviate machine load.",
                        5,
                        ref failoverAlert,
                        ref remainingTimeToFailvoerAlert);
                }
                catch (Exception e)
                {
                    if (Logger.IsOperationsEnabled)
                        Logger.Operations("Unhandled exception occured during cpu credit monitoring", e);
                }

                // During startup and until we get a valid result, we retry once a minute
                // After that we retry every sync interval (from configuration) or if ForceSyncCpuCredits() is called
                try
                {
                    if (sw.Elapsed.TotalSeconds >= (int)Configuration.Server.CpuCreditsExecSyncInterval.AsTimeSpan.TotalSeconds 
                        || CpuCreditsBalance.ForceSync == 1
                        || (duringStartup && startupRetriesSw.Elapsed.TotalSeconds >= TimeSpan.FromMinutes(1).TotalSeconds)) // Time to wait between retries = 1 minute
                    {
                        sw.Restart();

                        UpdateCpuCreditsFromExec();

                        CpuCreditsBalance.ForceSync = 0;

                        duringStartup = false;
                        startupRetriesSw = null;
                    }
                }
                catch (Exception e)
                {
                    if (duringStartup)
                        startupRetriesSw.Restart();

                    // If it's the first time, or if we logged the last error more than 15 minutes ago
                    if (err == null || err.Elapsed.TotalMinutes > 15)
                    {
                        if (Logger.IsOperationsEnabled)
                            Logger.Operations("During CPU credits monitoring, failed to sync the remaining credits.", e);
                        if (err == null)
                            err = Stopwatch.StartNew();
                        else
                            err.Restart();
                    }
                }

                try
                {
                    Task.Delay(1000).Wait(ServerStore.ServerShutdown);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }

            void MaybeRaiseAlert(
                double threshold,
                double threadholdReleaseValue,
                MultipleUseFlag alertFlag,
                string alertMessage,
                int defaultTimeToAlert,
                ref AlertRaised alert,
                ref int remainingTimeToAlert)
            {
                if (CpuCreditsBalance.RemainingCpuCredits < threshold)
                {
                    if (alertFlag.IsRaised() == false)
                    {
                        alertFlag.Raise();
                        return;
                    }
                    if (alert == null && remainingTimeToAlert-- > 0)
                    {
                        alert = AlertRaised.Create(null, "CPU credits balance exhausted", alertMessage,
                            AlertType.Throttling_CpuCreditsBalance,
                            NotificationSeverity.Warning);
                        ServerStore.NotificationCenter.Add(alert);
                    }
                }
                if (alertFlag.IsRaised() && CpuCreditsBalance.RemainingCpuCredits > threadholdReleaseValue)
                {
                    alertFlag.Lower();
                    if (alert != null)
                    {
                        ServerStore.NotificationCenter.Dismiss(alert.Id);
                        alert = null;
                        remainingTimeToAlert = defaultTimeToAlert;
                    }
                }
            }
        }

        internal double UpdateCpuCreditsFromExec()
        {
            var response = GetCpuCreditsFromExec();

            if (response.Timestamp < DateTime.UtcNow.AddHours(-1))
                throw new InvalidOperationException($"Cannot sync the remaining CPU credits, got a result with a timestamp of more than one hour ago: {response.Timestamp}.");

            CpuCreditsBalance.RemainingCpuCredits = response.Remaining;
            CpuCreditsBalance.LastSyncTime = response.Timestamp;
            CpuCreditsBalance.HistoryCurrentIndex = 0;
            Array.Clear(CpuCreditsBalance.History, 0, CpuCreditsBalance.History.Length);

            return response.Remaining;
        }

        public class CpuCreditsResponse
        {
            public double Remaining { get; set; }
            public DateTime Timestamp { get; set; }
        }

        private CpuCreditsResponse GetCpuCreditsFromExec()
        {
            var command = Configuration.Server.CpuCreditsExec;
            var arguments = Configuration.Server.CpuCreditsExecArguments;

            var startInfo = new ProcessStartInfo
            {
                FileName = command,
                Arguments = arguments,
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true
            };

            Process process;

            try
            {
                process = Process.Start(startInfo);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Unable to get cpu credits by executing {command} {arguments}. Failed to start process.", e);
            }

            using (var ms = new MemoryStream())
            {
                var readErrors = process.StandardError.ReadToEndAsync();
                var readStdOut = process.StandardOutput.BaseStream.CopyToAsync(ms);
                var timeoutInMs = (int)Configuration.Server.CpuCreditsExecTimeout.AsTimeSpan.TotalMilliseconds;

                string GetStdError()
                {
                    try
                    {
                        return readErrors.Result;
                    }
                    catch
                    {
                        return "Unable to get stdout";
                    }
                }

                try
                {
                    readStdOut.Wait(timeoutInMs);
                    readErrors.Wait(timeoutInMs);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Unable to get cpu credits by executing {command} {arguments}, waited for {timeoutInMs}ms but the process didn't exit. Stderr: {GetStdError()}", e);
                }

                if (process.WaitForExit(timeoutInMs) == false)
                {
                    process.Kill();

                    throw new InvalidOperationException($"Unable to get cpu credits by executing {command} {arguments}, waited for {timeoutInMs}ms but the process didn't exit. Stderr: {GetStdError()}");
                }

                if (process.ExitCode != 0)
                {
                    throw new InvalidOperationException($"Unable to get cpu credits by executing {command} {arguments}, the exit code was {process.ExitCode}. Stderr: {GetStdError()}");
                }

                using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    try
                    {
                        ms.Position = 0;
                        var response = context.ReadForMemory(ms, "cpu-credits-from-script");
                        if (response.TryGet("Error", out string err))
                        {
                            throw new InvalidOperationException("Error from server: " + err);
                        }
                        if (response.GetPropertyIndex(nameof(CpuCreditsResponse.Remaining)) == -1)
                        {
                            throw new InvalidOperationException("Missing required property: " + nameof(CpuCreditsResponse.Remaining));
                        }
                        var cpuCreditsFromExec = JsonDeserializationServer.CpuCreditsResponse(response);
                        return cpuCreditsFromExec;
                    }
                    catch (Exception e)
                    {
                        string s = null;
                        try
                        {
                            if (ms.TryGetBuffer(out var buffer))
                            {
                                s = Encoding.UTF8.GetString(new ReadOnlySpan<byte>(buffer.Array, buffer.Offset, buffer.Count));
                            }
                        }
                        catch
                        {
                            // nothing to do
                        }

                        throw new InvalidOperationException("Failed to get cpu credits: " + s, e);
                    }
                }
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
                    httpMessageHandler.SslProtocols = TcpUtils.SupportedSslProtocols;

                    if (Logger.IsOperationsEnabled)
                        Logger.Operations($"When setting the certificate, validating that the server can authenticate with itself using {url}.");

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
                    if (Logger.IsOperationsEnabled)
                        Logger.Operations($"Successful connection to {url}.");
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
                        httpMessageHandler.SslProtocols = TcpUtils.SupportedSslProtocols;
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

        private bool CertificateCallback(object sender, X509Certificate cert, X509Chain chain, SslPolicyErrors errors)
        {
            if (errors == SslPolicyErrors.None)
                return true;

            var cert2 = HttpsConnectionMiddleware.ConvertToX509Certificate2(cert);

            // We trust ourselves
            if (cert2?.Thumbprint == Certificate?.Certificate?.Thumbprint)
                return true;

            // self-signed is acceptable only if we have the same issuer as the remote certificate
            if (errors == SslPolicyErrors.RemoteCertificateChainErrors)
            {
                X509Certificate2 issuer = chain.ChainElements.Count > 1
                    ? chain.ChainElements[1].Certificate
                    : chain.ChainElements[0].Certificate;

                if (issuer?.Thumbprint == Certificate?.Certificate?.Thumbprint)
                    return true;
            }

            return false;
        }

        private Task _currentRefreshTask = Task.CompletedTask;

        public Task RefreshTask => _currentRefreshTask;

        public void RefreshClusterCertificateTimerCallback(object state)
        {
            RefreshClusterCertificate(state, RaftIdGenerator.NewId());
            
            try
            {
                UpdateCertificateExpirationAlert();
        }
            catch (Exception exception)
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations("Periodic check of the server certificate expiration date failed.", exception);
            }
        }

        public bool RefreshClusterCertificate(object state, string raftRequestId)
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

            var refreshCertificate = new Task(async () => { await DoActualCertificateRefresh(currentCertificate, raftRequestId, forceRenew: forceRenew); });
            if (Interlocked.CompareExchange(ref _currentRefreshTask, currentRefreshTask, refreshCertificate) != currentRefreshTask)
                return false;

            refreshCertificate.Start();

            return true;
        }

        private async Task DoActualCertificateRefresh(CertificateHolder currentCertificate, string raftRequestId, bool forceRenew = false)
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
                        _httpsConnectionMiddleware.SetCertificate(newCertificate.Certificate);
                    ServerCertificateChanged?.Invoke(this, EventArgs.Empty);
                    return;
                }

                if (ServerStore.IsLeader() == false)
                    return;

                byte[] newCertBytes;

                if (Configuration.Core.SetupMode == SetupMode.LetsEncrypt)
                {
                    newCertBytes = await RefreshViaLetsEncrypt(currentCertificate, forceRenew);
                }
                else if (string.IsNullOrEmpty(Configuration.Security.CertificateRenewExec) == false)
                {
                    newCertBytes = RefreshViaExecutable();
                }
                else
                {
                    // The case of the periodic check, if the certificate changed on disk.
                    return;
                }

                // One of the prerequisites for the refresh has failed and it has been logged. Nothing to do anymore.
                if (newCertBytes == null)
                    return;

                await StartCertificateReplicationAsync(newCertBytes, false, raftRequestId);
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

        private byte[] RefreshViaExecutable()
        {
            try
            {
                var certHolder = ServerStore.Secrets.LoadCertificateWithExecutable(Configuration.Security.CertificateRenewExec, Configuration.Security.CertificateRenewExecArguments, ServerStore);

                return certHolder.Certificate.Export(X509ContentType.Pfx); // With the private key
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Unable to refresh certificate with executable.", e);
            }
        }

        protected async Task<byte[]> RefreshViaLetsEncrypt(CertificateHolder currentCertificate, bool forceRenew)
        {
            byte[] newCertBytes;
            if (ClusterCommandsVersionManager.ClusterCommandsVersions.TryGetValue(nameof(ConfirmServerCertificateReplacedCommand), out var commandVersion) == false)
                throw new InvalidOperationException($"Failed to get the command version of '{nameof(ConfirmServerCertificateReplacedCommand)}'.");

            if (ClusterCommandsVersionManager.CurrentClusterMinimalVersion < commandVersion)
                throw new ClusterNodesVersionMismatchException(
                    "It is not possible to refresh/replace the cluster certificate in the current cluster topology. Please make sure that all the cluster nodes have an equal or newer version than the command version." +
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
                        return null;
                    }

                    if (certUpdate.TryGet(nameof(CertificateReplacement.Replaced), out int replaced) == false)
                        replaced = 0;

                    if (nodesInCluster > replaced)
                    {
                        // This is for the case where all nodes confirmed they received the replacement cert but
                        // not all nodes have made the actual change yet.
                        await ServerStore.SendToLeaderAsync(new RecheckStatusOfServerCertificateReplacementCommand());
                    }

                    return null;
                }
            }

            // same certificate, but now we need to see if we need to auto update it
            var (shouldRenew, renewalDate) = CalculateRenewalDate(currentCertificate, forceRenew);
            if (shouldRenew == false)
            {
                // We don't want an alert here, this happens frequently.
                if (Logger.IsOperationsEnabled)
                    Logger.Operations(
                        $"Renew check: still have time left to renew the server certificate with thumbprint `{currentCertificate.Certificate.Thumbprint}`, estimated renewal date: {renewalDate}");
                return null;
            }

            if (ServerStore.LicenseManager.LicenseStatus.CanAutoRenewLetsEncryptCertificate == false && forceRenew == false)
            {
                var msg =
                    "It's time to renew your Let's Encrypt server certificate but automatic renewal is not supported by your license. Go to the certificate page in the studio and trigger the renewal manually.";
                ServerStore.NotificationCenter.Add(AlertRaised.Create(
                    null,
                    CertificateReplacement.CertReplaceAlertTitle,
                    msg,
                    AlertType.Certificates_DeveloperLetsEncryptRenewal,
                    NotificationSeverity.Warning));

                if (Logger.IsOperationsEnabled)
                    Logger.Operations(msg);
                return null;
            }

            try
            {
                newCertBytes = await RenewLetsEncryptCertificate(currentCertificate);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to update certificate from Lets Encrypt", e);
            }

            return newCertBytes;
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

        public async Task StartCertificateReplicationAsync(byte[] certBytes, bool replaceImmediately, string raftRequestId)
        {
            // We assume that at this point, the password was already stripped out of the certificate.

            // the process of updating a new certificate is the same as deleting a database
            // we first send the certificate to all the nodes, then we get acknowledgments
            // about that from them, and we replace only when they are confirmed to have been
            // successful. However, if we have less than 3 days for renewing the cert or if
            // replaceImmediately is true, we'll replace immediately

            try
            {
                X509Certificate2 newCertificate;
                try
                {
                    newCertificate = new X509Certificate2(certBytes, (string)null, X509KeyStorageFlags.Exportable | X509KeyStorageFlags.PersistKeySet | X509KeyStorageFlags.MachineKeySet);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Failed to load (and validate) the new certificate which was received during the refresh process.", e);
                }

                if (Logger.IsOperationsEnabled)
                {
                    var source = string.IsNullOrEmpty(Configuration.Security.CertificateLoadExec) ? "Let's Encrypt" : $"executable ({Configuration.Security.CertificateLoadExec} {Configuration.Security.CertificateLoadExecArguments})";
                    Logger.Operations($"Got new certificate from {source}. Starting certificate replication.");
                }

                // During replacement of a cluster certificate, we must have both the new and the old server certificates registered in the server store.
                // This is needed for trust in the case where a node replaced its own certificate while another node still runs with the old certificate.
                // Since both nodes use different certificates, they will only trust each other if the certs are registered in the server store.
                // When the certificate replacement is finished throughout the cluster, we will delete both these entries.
                await ServerStore.PutValueInClusterAsync(new PutCertificateCommand(newCertificate.Thumbprint,
                    new CertificateDefinition
                    {
                        Certificate = Convert.ToBase64String(Certificate.Certificate.Export(X509ContentType.Cert)),
                        Thumbprint = Certificate.Certificate.Thumbprint,
                        PublicKeyPinningHash = Certificate.Certificate.GetPublicKeyPinningHash(),
                        NotAfter = Certificate.Certificate.NotAfter,
                        Name = "Old Server Certificate - can delete",
                        SecurityClearance = SecurityClearance.ClusterNode
                    }, $"{raftRequestId}/put-old-certificate"));

                var res = await ServerStore.PutValueInClusterAsync(new PutCertificateCommand(newCertificate.Thumbprint,
                    new CertificateDefinition
                    {
                        Certificate = Convert.ToBase64String(newCertificate.Export(X509ContentType.Cert)),
                        Thumbprint = newCertificate.Thumbprint,
                        PublicKeyPinningHash = newCertificate.GetPublicKeyPinningHash(),
                        NotAfter = newCertificate.NotAfter,
                        Name = "Server Certificate",
                        SecurityClearance = SecurityClearance.ClusterNode
                    }, $"{raftRequestId}/put-new-certificate"));

                await ServerStore.Cluster.WaitForIndexNotification(res.Index);

                await ServerStore.SendToLeaderAsync(new InstallUpdatedServerCertificateCommand(Convert.ToBase64String(certBytes), replaceImmediately,
                    $"{raftRequestId}/install-new-certificate"));
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
                    throw new InvalidOperationException($"Your license is associated with the following domains: {string.Join(", ", userDomainsResult.RootDomains)} " +
                                                        $"but the PublicServerUrl configuration setting is: {Configuration.Core.PublicServerUrl.Value.UriValue}. " +
                                                        "There is a mismatch, therefore cannot automatically renew the Lets Encrypt certificate. Please contact support.");

                throw new InvalidOperationException("PublicServerUrl is empty. Cannot automatically renew the Lets Encrypt certificate. Please contact support.");
            }

            if (userDomainsResult.Emails.Contains(Configuration.Security.CertificateLetsEncryptEmail, StringComparer.OrdinalIgnoreCase) == false)
                throw new InvalidOperationException($"Your license is associated with the following emails: {string.Join(", ", userDomainsResult.Emails)} " +
                                                    $"but the Security.Certificate.LetsEncrypt.Email configuration setting is: {Configuration.Security.CertificateLetsEncryptEmail}. " +
                                                    "There is a mismatch, therefore cannot automatically renew the Lets Encrypt certificate. Please contact support.");

            var hosts = SetupManager.GetCertificateAlternativeNames(existing.Certificate).ToArray();

            // cloud: *.free.iftah.ravendb.cloud => we extract the domain free.iftah
            // normal: *.iftah.development.run => we extract the domain iftah

            // remove the root domain
            var substring = hosts[0].Substring(0, hosts[0].Length - usedRootDomain.Length - 1);
            var firstDot = substring.IndexOf('.');
            // remove the *.
            var domain = substring.Substring(firstDot + 1);

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

        private CertificateHolder LoadCertificateAtStartup()
        {
            try
            {
                if (string.IsNullOrEmpty(Configuration.Security.CertificateLoadExec) == false &&
                    (string.IsNullOrEmpty(Configuration.Security.CertificateRenewExec) || string.IsNullOrEmpty(Configuration.Security.CertificateChangeExec)))
                {
                    if (Logger.IsOperationsEnabled)
                        Logger.Operations($"You are using the configuration property '{RavenConfiguration.GetKey(x => x.Security.CertificateLoadExec)}', without specifying '{RavenConfiguration.GetKey(x => x.Security.CertificateRenewExec)}' and '{RavenConfiguration.GetKey(x => x.Security.CertificateChangeExec)}'. This configuration requires you to renew the certificate manually across the entire cluster.");
                }

                return LoadCertificate();
            }
            catch (Exception e)
                {
                throw new InvalidOperationException("Unable to start the server.", e);
                }
        }

        private CertificateHolder LoadCertificate()
        {
            try
            {
                if (string.IsNullOrEmpty(Configuration.Security.CertificateExec) == false)
                {
                    throw new InvalidOperationException($"Invalid certificate configuration. The configuration property '{RavenConfiguration.GetKey(x => x.Security.CertificateExec)}' has been deprecated since RavenDB 4.2, please use '{RavenConfiguration.GetKey(x => x.Security.CertificateLoadExec)}' along with '{RavenConfiguration.GetKey(x => x.Security.CertificateRenewExec)}' and '{RavenConfiguration.GetKey(x => x.Security.CertificateChangeExec)}'.");
                }

                if (string.IsNullOrEmpty(Configuration.Security.CertificatePath) == false)
                    return ServerStore.Secrets.LoadCertificateFromPath(Configuration.Security.CertificatePath, Configuration.Security.CertificatePassword, ServerStore);
                if (string.IsNullOrEmpty(Configuration.Security.CertificateLoadExec) == false)
                    return ServerStore.Secrets.LoadCertificateWithExecutable(Configuration.Security.CertificateLoadExec, Configuration.Security.CertificateLoadExecArguments, ServerStore);

                return null;
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Unable to load the server certificate due to invalid configuration! Admin assistance required.", e);
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

            public string WrongProtocolMessage;

            private AuthenticationStatus _status;

            public AuthenticationStatus StatusForAudit => _status;

            public string IssuerHash;

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

        internal AuthenticateConnection AuthenticateConnectionCertificate(X509Certificate2 certificate, object connectionInfo)
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
                    var cert = ServerStore.Cluster.GetCertificateByThumbprint(ctx, certificate.Thumbprint) ??
                               ServerStore.Cluster.GetLocalStateByThumbprint(ctx, certificate.Thumbprint);

                    if (cert == null)
                    {
                        // If connection will be approved, cert won't be null anymore and will be assigned the relevant permissions
                        MaybeAllowConnectionBasedOnPinningHash(certificate, ctx, ref authenticationStatus, ref cert, connectionInfo);
                    }

                    if (cert != null)
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

        private void MaybeAllowConnectionBasedOnPinningHash(X509Certificate2 certificate, TransactionOperationContext ctx,
            ref AuthenticateConnection authenticationStatus, ref BlittableJsonReaderObject cert, object connectionInfo)
        {
            // The certificate is not explicitly registered in our server, let's see if we have a certificate
            // with the same public key pinning hash.
            var pinningHash = certificate.GetPublicKeyPinningHash();
            var certWithSameHash = ServerStore.Cluster.GetCertificatesByPinningHash(ctx, pinningHash).FirstOrDefault();

            if (certWithSameHash == null)
            {
                authenticationStatus.Status = AuthenticationStatus.UnfamiliarCertificate;
                return;
            }

            string remoteAddress = null;
            switch (connectionInfo)
            {
                case TcpClient tcp:
                    remoteAddress = tcp.Client.RemoteEndPoint.ToString();
                    break;
                case HttpConnectionFeature http:
                    remoteAddress = $"{http.RemoteIpAddress}:{http.RemotePort}";
                    break;
            }

            // Hash is good, let's validate it was signed by a known issuer, otherwise users can use the private key to register a new cert with a different issuer.
            var goodKnownCert = new X509Certificate2(Convert.FromBase64String(certWithSameHash.Certificate));
            if (CertHasKnownIssuer(certificate, goodKnownCert, out var issuerHash) == false)
            {
                if (_authAuditLog.IsInfoEnabled)
                    _authAuditLog.Info($"Connection from {remoteAddress} with certificate '{certificate.Subject} ({certificate.Thumbprint})' which is not registered in the cluster. " +
                                       "Tried to allow the connection implicitly based on the client certificate's Public Key Pinning Hash but the client certificate was signed by an unknown issuer - closing the connection. " +
                                       $"To fix this, the admin can register the pinning hash of the *issuer* certificate: '{issuerHash}' in the '{RavenConfiguration.GetKey(x => x.Security.WellKnownIssuerHashes)}' configuration entry." +
                                       $"Alternatively, the admin can register the actual certificate ({certificate.FriendlyName} '{certificate.Thumbprint}') explicitly in the cluster.");

                authenticationStatus.Status = AuthenticationStatus.UnfamiliarIssuer;
                authenticationStatus.IssuerHash = issuerHash;
                return;
            }

            // Success, we'll add the new certificate with same permissions as the original
            var newCertBytes = certificate.Export(X509ContentType.Cert);

            var newCertDef = new CertificateDefinition()
            {
                Name = certWithSameHash.Name,
                Certificate = Convert.ToBase64String(newCertBytes),
                Permissions = certWithSameHash.Permissions,
                SecurityClearance = certWithSameHash.SecurityClearance,
                Password = certWithSameHash.Password,
                Thumbprint = certificate.Thumbprint,
                PublicKeyPinningHash = pinningHash,
                NotAfter = certificate.NotAfter
            };

            cert = ServerStore.Cluster.GetCertificateByThumbprint(ctx, certificate.Thumbprint) ??
                   ServerStore.Cluster.GetLocalStateByThumbprint(ctx, certificate.Thumbprint);
            if (cert != null)
                return;

            // This command will discard leftover certificates after the new certificate is saved.
            GC.KeepAlive(Task.Run(async () =>
            {
                try
                {
                    await ServerStore.SendToLeaderAsync(new PutCertificateWithSamePinningHashCommand(certificate.Thumbprint, newCertDef, RaftIdGenerator.NewId()))
                        .ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Failed to run command '{nameof(PutCertificateWithSamePinningHashCommand)}'.", e);
                }
            }, ServerStore.ServerShutdown));

            if (_authAuditLog.IsInfoEnabled)
                _authAuditLog.Info(
                    $"Connection from {remoteAddress} with new certificate '{certificate.Subject} ({certificate.Thumbprint})' which is not registered in the cluster. " +
                    "Allowing the connection based on the certificate's Public Key Pinning Hash which is trusted by the cluster. " +
                    $"Registering the new certificate explicitly based on permissions of existing certificate '{certWithSameHash.Thumbprint}'. Security Clearance: {newCertDef.SecurityClearance}, " +
                    $"Permissions:{Environment.NewLine}{string.Join(Environment.NewLine, newCertDef.Permissions.Select(kvp => kvp.Key + ": " + kvp.Value.ToString()))}");

            cert = ctx.ReadObject(newCertDef.ToJson(), "Client/Certificate/Definition");
        }

        private bool CertHasKnownIssuer(X509Certificate2 userCertificate, X509Certificate2 knownCertificate, out string issuerPinningHash)
        {
            issuerPinningHash = null;
            X509Certificate2 issuerCertificate = null;

            var userChain = new X509Chain();
            var knownCertChain = new X509Chain();

            try
            {
                userChain.Build(userCertificate);
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Cannot validate new client certificate '{userCertificate.FriendlyName} {userCertificate.Thumbprint}', failed to build the chain.", e);
                return false;
            }

            try
            {
                issuerCertificate = userChain.ChainElements.Count > 1
                    ? userChain.ChainElements[1].Certificate
                    : userChain.ChainElements[0].Certificate;
                issuerPinningHash = issuerCertificate.GetPublicKeyPinningHash();
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Cannot extract pinning hash from the client certificate's issuer '{issuerCertificate?.FriendlyName} {issuerCertificate?.Thumbprint}'.", e);
                return false;
            }

            var wellKnown = ServerStore.Configuration.Security.WellKnownIssuerHashes;
            if (wellKnown != null && wellKnown.Contains(issuerPinningHash, StringComparer.Ordinal)) // Case sensitive, base64
                return true;

            try
            {
                knownCertChain.Build(knownCertificate);
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Cannot validate new client certificate '{userCertificate.FriendlyName} {userCertificate.Thumbprint}'. Found a known certificate '{knownCertificate.Thumbprint}' with the same hash but failed to build its chain.", e);
                return false;
            }

            for (var i = 0; i < userChain.ChainElements.Count; i++)
            {
                // We walk the chain and compare the user certificate vs one of the existing certificate with same pinning hash
                var currentElementPinningHash = userChain.ChainElements[i].Certificate.GetPublicKeyPinningHash();
                if (currentElementPinningHash != knownCertChain.ChainElements[i].Certificate.GetPublicKeyPinningHash())
                {
                    issuerPinningHash = currentElementPinningHash;
                    return false;
                }
            }

            return true;
        }

        public string WebUrl { get; private set; }

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

            if (this.Configuration.Core.SetupMode == SetupMode.Initial)
                return status;

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
                        var msg = $"Unable to start tcp listener on {ipAddress} on port {port}.{ Environment.NewLine}" +
                        $"Port might be already in use.{ Environment.NewLine}" +
                        $"Try running with an unused TCP port.{Environment.NewLine}" +
                        $"You can change the TCP port using one of the following options:{Environment.NewLine}" +
                        $"1) Change the ServerUrl.Tcp property in setting.json file.{Environment.NewLine}" +
                        $"2) Run the server from the command line with --ServerUrl.Tcp option.{Environment.NewLine}" +
                        $"3) Add RAVEN_ServerUrl_Tcp to the Environment Variables.{Environment.NewLine}" +
                        "For more information go to https://ravendb.net/l/EJS81M/5.0";

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
                var tcpClient = await AcceptTcpClientAsync(listener);
                if (tcpClient == null)
                    return;

                Logger tcpAuditLog = LoggingSource.AuditLog.IsInfoEnabled ? LoggingSource.AuditLog.GetLogger("TcpConnections", "Audit") : null;
                ListenToNewTcpConnection(listener);

                if (ServerStore.Initialized == false)
                    await ServerStore.InitializationCompleted.WaitAsync();

                try
                {
                    tcpClient.NoDelay = true;
                    tcpClient.ReceiveBufferSize = (int)Configuration.Cluster.TcpReceiveBufferSize.GetValue(SizeUnit.Bytes);
                    tcpClient.SendBufferSize = (int)Configuration.Cluster.TcpSendBufferSize.GetValue(SizeUnit.Bytes);
                    tcpClient.LingerState = new LingerOption(true, 5);

                    var sendTimeout = (int)Configuration.Cluster.TcpConnectionTimeout.AsTimeSpan.TotalMilliseconds;

                    DebuggerAttachedTimeout.SendTimeout(ref sendTimeout);
                    tcpClient.ReceiveTimeout = tcpClient.SendTimeout = sendTimeout;

                    Stream stream = tcpClient.GetStream();
                    X509Certificate2 cert;

                    (stream, cert) = await AuthenticateAsServerIfSslNeeded(stream);

                    if (_forTestingPurposes != null && _forTestingPurposes.ThrowExceptionInListenToNewTcpConnection)
                        throw new Exception("Simulated TCP failure.");

                    using (_tcpContextPool.AllocateOperationContext(out JsonOperationContext context))
                    using (context.GetMemoryBuffer(out JsonOperationContext.MemoryBuffer buffer))
                    {
                        var tcp = new TcpConnectionOptions
                        {
                            ContextPool = _tcpContextPool,
                            Stream = stream,
                            TcpClient = tcpClient,
                        };

                        var remoteEndPoint = tcpClient.Client.RemoteEndPoint;

                        try
                        {
                            var header = await NegotiateOperationVersion(stream, buffer, tcpClient, tcpAuditLog, cert, tcp);

                            if (header.Operation == TcpConnectionHeaderMessage.OperationTypes.TestConnection ||
                                header.Operation == TcpConnectionHeaderMessage.OperationTypes.Ping)
                            {
                                tcp = null;
                                return;
                            }

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
                            try
                            {
                                tcp?.Dispose();
                            }
                            catch
                            {
                                // nothing we can do
                            }
                        }
                        finally
                        {
                            if (tcpAuditLog != null)
                                tcpAuditLog.Info($"Closed TCP connection {remoteEndPoint} with certificate '{cert?.Subject} ({cert?.Thumbprint})'.");
                        }
                    }
                }
                catch (Exception e)
                {
                    try
                    {
                        tcpClient.Dispose();
                    }
                    catch
                    {
                        // nothing we can do
                    }

                    if (_tcpLogger.IsInfoEnabled)
                    {
                        _tcpLogger.Info("Failure when processing tcp connection", e);
                    }
                }
            });
        }

        private async Task<TcpConnectionHeaderMessage> NegotiateOperationVersion(
            Stream stream,
            JsonOperationContext.MemoryBuffer buffer,
            TcpClient tcpClient,
            Logger tcpAuditLog,
            X509Certificate2 cert,
            TcpConnectionOptions tcp)
        {
            TcpConnectionHeaderMessage header;
            int count = 0, maxRetries = 100;
            using (_tcpContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                int supported;
                while (true)
                {
                    context.Reset();
                    context.Renew();
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
                                tcpAuditLog.Info(
                                    $"Got connection from {tcpClient.Client.RemoteEndPoint} with certificate '{cert?.Subject} ({cert?.Thumbprint})'. Dropping connection because: {header.Info}");

                            if (Logger.IsInfoEnabled)
                            {
                                Logger.Info($"Got a request to drop TCP connection to {header.DatabaseName ?? "the cluster node"} " +
                                            $"from {tcpClient.Client.RemoteEndPoint} reason: {header.Info}");
                            }

                            return header;
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

                    RespondToTcpConnection(stream, context, $"Not supporting version {header.OperationVersion} for {header.Operation}", TcpConnectionStatus.TcpVersionMismatch,
                        supported);
                }

                bool authSuccessful = TryAuthorize(Configuration, tcp.Stream, header, tcpClient, out var err);
                //At this stage the error is not relevant.
                RespondToTcpConnection(stream, context, null,
                    authSuccessful ? TcpConnectionStatus.Ok : TcpConnectionStatus.AuthorizationFailed,
                    supported);

                tcp.ProtocolVersion = supported;

                if (authSuccessful == false)
                {
                    if (tcpAuditLog != null)
                        tcpAuditLog.Info(
                            $"Got connection from {tcpClient.Client.RemoteEndPoint} with certificate '{cert?.Subject} ({cert?.Thumbprint})'. Rejecting connection because {err} for {header.Operation} on {header.DatabaseName}.");

                    if (Logger.IsInfoEnabled)
                    {
                        Logger.Info(
                            $"New {header.Operation} TCP connection to {header.DatabaseName ?? "the cluster node"} from {tcpClient.Client.RemoteEndPoint}" +
                            $" is not authorized to access {header.DatabaseName ?? "the cluster node"} because {err}");
                    }

                    return header;
                }

                if (Logger.IsInfoEnabled)
                {
                    Logger.Info($"TCP connection from {header.SourceNodeTag ?? tcpClient.Client.RemoteEndPoint.ToString()} " +
                                $"for '{header.Operation}' is accepted with version {supported}");
                }
            }

            if (tcpAuditLog != null)
                tcpAuditLog.Info(
                    $"Got connection from {tcpClient.Client.RemoteEndPoint} with certificate '{cert?.Subject} ({cert?.Thumbprint})'. Accepted for {header.Operation} on {header.DatabaseName}.");
            return header;
        }

        private async Task<TcpClient> AcceptTcpClientAsync(TcpListener listener)
        {
            var backoffSecondsDelay = 0;
            while (true)
            {
                try
                {
                    return await listener.AcceptTcpClientAsync();
                }
                catch (ObjectDisposedException)
                {
                    // shutting down
                    return null;
                }
                catch (Exception e)
                {
                    if (ServerStore.ServerShutdown.IsCancellationRequested)
                        return null;

                    if (backoffSecondsDelay == 0)
                    {
                        if (_tcpLogger.IsInfoEnabled)
                        {
                            _tcpLogger.Info($"Failed to accept new tcp connection, will retry now", e);
                        }

                        backoffSecondsDelay = 1;
                        continue;
                    }

                    if (_tcpLogger.IsOperationsEnabled)
                    {
                        _tcpLogger.Operations($"Failed to accept new tcp connection again, will wait {backoffSecondsDelay} seconds before retrying", e);
                    }

                    try
                    {
                        await Task.Delay(TimeSpan.FromSeconds(backoffSecondsDelay), ServerStore.ServerShutdown);
                    }
                    catch (OperationCanceledException)
                    {
                        // shutting down
                        return null;
                    }

                    if (listener.Server.Connected)
                    {
                        backoffSecondsDelay = Math.Min(backoffSecondsDelay * 2, 60);
                        continue;
                    }

                    var msg = $"The socket on {listener.LocalEndpoint} is no longer connected.";

                    if (_tcpLogger.IsOperationsEnabled)
                    {
                        _tcpLogger.Operations(msg, e);
                    }

                    var alert = AlertRaised.Create(
                        null,
                        msg,
                        $"Unable to accept connections from TCP, because the listening socket on {listener.LocalEndpoint} was disconnected.{Environment.NewLine}" +
                        $"Restarting the server might temporary fix the issue, but further investigation is required.",
                        AlertType.TcpListenerError,
                        NotificationSeverity.Error,
                        key: msg,
                        details: new ExceptionDetails(e)
                    );

                    ServerStore.NotificationCenter.Add(alert);

                    return null;
                }
            }
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
                using (var context = JsonOperationContext.ShortTermSingleUse())
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
        private HttpsConnectionMiddleware _httpsConnectionMiddleware;
        private PoolOfThreads.LongRunningWork _cpuCreditsMonitoring;
        public readonly ServerMetricCacher MetricCacher;

        public (IPAddress[] Addresses, int Port) ListenEndpoints { get; private set; }

        internal void SetCertificate(X509Certificate2 certificate, byte[] rawBytes, string password)
        {
            var certificateHolder = Certificate;
            var newCertHolder = SecretProtection.ValidateCertificateAndCreateCertificateHolder("Auto Update", certificate, rawBytes, password, ServerStore);
            if (Interlocked.CompareExchange(ref Certificate, newCertHolder, certificateHolder) == certificateHolder)
            {
                _httpsConnectionMiddleware.SetCertificate(certificate);
                ServerCertificateChanged?.Invoke(this, EventArgs.Empty);
            }
        }

        private async Task<bool> DispatchServerWideTcpConnection(TcpConnectionOptions tcp, TcpConnectionHeaderMessage header, JsonOperationContext.MemoryBuffer buffer)
        {
            tcp.Operation = header.Operation;
            if (tcp.Operation == TcpConnectionHeaderMessage.OperationTypes.Cluster)
            {
                var tcpClient = tcp.TcpClient.Client;
                ServerStore.ClusterAcceptNewConnection(tcp, header, () => tcpClient.Disconnect(false), tcpClient.RemoteEndPoint);
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

        private async Task<bool> DispatchDatabaseTcpConnection(TcpConnectionOptions tcp, TcpConnectionHeaderMessage header, JsonOperationContext.MemoryBuffer bufferToCopy)
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
                    SubscriptionConnection.SendSubscriptionDocuments(ServerStore, tcp, bufferToCopy);
                    break;
                case TcpConnectionHeaderMessage.OperationTypes.Replication:
                    var documentReplicationLoader = tcp.DocumentDatabase.ReplicationLoader;
                    documentReplicationLoader.AcceptIncomingConnection(tcp, header.Operation, bufferToCopy);
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

                await sslStream.AuthenticateAsServerAsync(new SslServerAuthenticationOptions
                {
                    ServerCertificate = Certificate.Certificate,
                    ClientCertificateRequired = true,
                    CertificateRevocationCheckMode = X509RevocationMode.NoCheck,
                    EncryptionPolicy = EncryptionPolicy.RequireEncryption,
                    EnabledSslProtocols = TcpUtils.SupportedSslProtocols,
                    CipherSuitesPolicy = CipherSuitesPolicy
                });

                return (sslStream, HttpsConnectionMiddleware.ConvertToX509Certificate2(sslStream.RemoteCertificate));
            }

            return (stream, null);
        }

        private bool TryAuthorize(RavenConfiguration configuration, Stream stream, TcpConnectionHeaderMessage header, TcpClient tcpClient, out string msg)
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
            var auth = AuthenticateConnectionCertificate(certificate, tcpClient);

            switch (auth.Status)
            {
                case AuthenticationStatus.Expired:
                    msg = $"The provided client certificate ({certificate.FriendlyName} '{certificate.Thumbprint}') is expired on {certificate.NotAfter}";
                    return false;
                case AuthenticationStatus.NotYetValid:
                    msg = $"The provided client certificate ({certificate.FriendlyName} '{certificate.Thumbprint}') is not yet valid because it starts on {certificate.NotBefore}";
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
                            msg = $"{header.Operation} is a server-wide operation and the certificate ({certificate.FriendlyName} '{certificate.Thumbprint}') is not ClusterAdmin/Operator";
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
                            msg = $"The certificate {certificate.FriendlyName} does not allow access to {header.DatabaseName}";
                            return false;
                        default:
                            throw new InvalidOperationException("Unknown operation " + header.Operation);
                    }
                case AuthenticationStatus.UnfamiliarIssuer:
                    msg = $"The client certificate {certificate.FriendlyName} is not registered in the cluster. Tried to allow the connection implicitly based on the client certificate's Public Key Pinning Hash but the client certificate was signed by an unknown issuer - closing the connection. " +
                          $"To fix this, the admin can register the pinning hash of the *issuer* certificate: '{auth.IssuerHash}' in the '{RavenConfiguration.GetKey(x => x.Security.WellKnownIssuerHashes)}' configuration entry. Alternatively, the admin can register the actual certificate ({certificate.FriendlyName} '{certificate.Thumbprint}') explicitly in the cluster.";
                    return false;
                case AuthenticationStatus.UnfamiliarCertificate:
                    var info = header.AuthorizeInfo;
                    if (info != null && info.AuthorizeAs == TcpConnectionHeaderMessage.AuthorizationInfo.AuthorizeMethod.PullReplication)
                    {
                        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                        using (ctx.OpenReadTransaction())
                        {
                            if (ServerStore.Cluster.TryReadPullReplicationDefinition(header.DatabaseName, info.AuthorizationFor, ctx, out var pullReplication)
                                && pullReplication.CanAccess(certificate.Thumbprint))
                                return true;

                            msg = $"The certificate {certificate.FriendlyName} does not allow access to {header.DatabaseName} for {info.AuthorizationFor}";
                            return false;
                        }
                    }
                    goto default;
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
                ea.Execute(() => _clusterMaintenanceWorker?.Dispose());
                ea.Execute(() => _cpuCreditsMonitoring?.Join(int.MaxValue));
                ea.Execute(() => CpuUsageCalculator.Dispose());

                // this should be last
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
            if(Configuration.Server.DisableLogsStream == false)
                LogStreamPipe = Pipes.OpenLogStreamPipe();
            if(Configuration.Server.DisableAdminChannel == false)
                AdminConsolePipe = Pipes.OpenAdminConsolePipe();
        }

        public enum AuthenticationStatus
        {
            None,
            NoCertificateProvided,
            UnfamiliarCertificate,
            UnfamiliarIssuer,
            Allowed,
            Operator,
            ClusterAdmin,
            Expired,
            NotYetValid
        }

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (_forTestingPurposes != null)
                return _forTestingPurposes;

            return _forTestingPurposes = new TestingStuff();
    }

        internal class TestingStuff
        {
            internal bool ThrowExceptionInListenToNewTcpConnection = false;
}
    }
}
