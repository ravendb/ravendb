using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.System;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Utils;
using Voron;
using StudioConfiguration = Raven.Client.Documents.Operations.Configuration.StudioConfiguration;

namespace Raven.Server.Commercial
{
    public class LicenseManager : IDisposable
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<LicenseManager>("Server");
        private static readonly RSAParameters _rsaParameters;
        private readonly LicenseStorage _licenseStorage = new LicenseStorage();
        private Timer _leaseLicenseTimer;
        private readonly ServerStore _serverStore;
        private readonly LicenseHelper _licenseHelper;

        private readonly SemaphoreSlim _leaseLicenseSemaphore = new SemaphoreSlim(1);
        private readonly SemaphoreSlim _licenseLimitsSemaphore = new SemaphoreSlim(1);
        private readonly bool _skipLeasingErrorsLogging;
        private DateTime? _lastPerformanceHint;
        private bool _eulaAcceptedButHasPendingRestart;

        private readonly SemaphoreSlim _locker = new SemaphoreSlim(1, 1);
        private LicenseSupportInfo _lastKnownSupportInfo;

        public event Action LicenseChanged;

        public event Action OnBeforeInitialize;

        public static readonly OsInfo OsInfo = OsInfoExtensions.GetOsInfo();

        public static readonly BuildNumber BuildInfo = new BuildNumber
        {
            BuildVersion = ServerVersion.Build,
            ProductVersion = ServerVersion.Version,
            CommitHash = ServerVersion.CommitHash,
            FullVersion = ServerVersion.FullVersion
        };

        public LicenseStatus LicenseStatus { get; private set; } = new LicenseStatus();

        internal static bool IgnoreProcessorAffinityChanges = false;

        static LicenseManager()
        {
            string publicKeyString;
            const string publicKeyPath = "Raven.Server.Commercial.RavenDB.public.json";
            using (var stream = typeof(LicenseManager).Assembly.GetManifestResourceStream(publicKeyPath))
            {
                if (stream == null)
                    throw new InvalidOperationException("Could not find public key for the license");
                publicKeyString = new StreamReader(stream).ReadToEnd();
            }

            var rsaPublicParameters = JsonConvert.DeserializeObject<RSAPublicParameters>(publicKeyString);
            _rsaParameters = new RSAParameters
            {
                Modulus = rsaPublicParameters.RsaKeyValue.Modulus,
                Exponent = rsaPublicParameters.RsaKeyValue.Exponent
            };
        }

        public LicenseManager(ServerStore serverStore)
        {
            _serverStore = serverStore;
            _licenseHelper = new LicenseHelper(serverStore);
            _skipLeasingErrorsLogging = serverStore.Configuration.Licensing.SkipLeasingErrorsLogging;
        }

        public bool IsEulaAccepted => _eulaAcceptedButHasPendingRestart || _serverStore.Configuration.Licensing.EulaAccepted;

        public void Initialize(StorageEnvironment environment, TransactionContextPool contextPool)
        {
            try
            {
                OnBeforeInitialize?.Invoke();

                _licenseStorage.Initialize(environment, contextPool);

                var firstServerStartDate = _licenseStorage.GetFirstServerStartDate();
                if (firstServerStartDate == null)
                {
                    firstServerStartDate = SystemTime.UtcNow;
                    _licenseStorage.SetFirstServerStartDate(firstServerStartDate.Value);
                }

                LicenseStatus.FirstServerStartDate = firstServerStartDate.Value;
                _licenseStorage.SetBuildInfo(BuildInfo);

                // on a fresh server we are setting the amount of cores in the default license (3)
                ReloadLicense(firstRun: true);
                ReloadLicenseLimits(firstRun: true);

                Task.Run(async () => await PutMyNodeInfoAsync()).IgnoreUnobservedExceptions();
            }
            catch (Exception e)
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations("Failed to initialize license manager", e);
            }
            finally
            {
                _leaseLicenseTimer = new Timer(state =>
                    AsyncHelpers.RunSync(ExecuteTasks), null,
                    (int)TimeSpan.FromMinutes(1).TotalMilliseconds,
                    (int)TimeSpan.FromHours(24).TotalMilliseconds);
            }
        }

        public async Task PutMyNodeInfoAsync()
        {
            if (_serverStore.IsPassive())
                return;

            if (await _licenseLimitsSemaphore.WaitAsync(0) == false)
                return;

            try
            {
                var nodeInfo = _serverStore.GetNodeInfo();
                var detailsPerNode = new DetailsPerNode
                {
                    UtilizedCores = UpdateLicenseLimitsCommand.NodeInfoUpdate,
                    NumberOfCores = nodeInfo.NumberOfCores,
                    InstalledMemoryInGb = nodeInfo.InstalledMemoryInGb,
                    UsableMemoryInGb = nodeInfo.UsableMemoryInGb,
                    BuildInfo = nodeInfo.BuildInfo,
                    OsInfo = nodeInfo.OsInfo
                };

                await _serverStore.PutNodeLicenseLimitsAsync(_serverStore.NodeTag, detailsPerNode, LicenseStatus.MaxCores);
            }
            catch (Exception e)
            {
                if (Logger.IsOperationsEnabled && _serverStore.IsPassive() == false)
                    Logger.Operations("Failed to put my node info, will try again later", e);
            }
            finally
            {
                _licenseLimitsSemaphore.Release();
            }
        }

        public void ReloadLicense(bool firstRun = false)
        {
            var license = _serverStore.LoadLicense();
            if (license == null)
            {
                // license is not active
                ResetLicense(error: null);

                CreateAgplAlert();

                return;
            }

            try
            {
                SetLicense(GetLicenseStatus(license));

                RemoveAgplAlert();
            }
            catch (Exception e)
            {
                ResetLicense(e.Message);

                if (Logger.IsInfoEnabled)
                    Logger.Info("Could not validate license", e);

                var alert = AlertRaised.Create(
                     null,
                    "License manager initialization error",
                    "Could not initialize the license manager",
                    AlertType.LicenseManager_InitializationError,
                    NotificationSeverity.Warning,
                    details: new ExceptionDetails(e));

                _serverStore.NotificationCenter.Add(alert);
            }

            LicenseChanged?.Invoke();

            if (firstRun == false)
                _licenseHelper.UpdateLocalLicense(license, _rsaParameters);
        }

        private void CreateAgplAlert()
        {
            var alert = AlertRaised.Create(
                null,
                "Your server is running without a license",
                null,
                AlertType.LicenseManager_AGPL3,
                NotificationSeverity.Warning);

            _serverStore.NotificationCenter.Add(alert);
        }

        private void RemoveAgplAlert()
        {
            _serverStore.NotificationCenter.Dismiss(AlertRaised.GetKey(AlertType.LicenseManager_AGPL3, null));
        }

        public void ReloadLicenseLimits(bool firstRun = false)
        {
            try
            {
                using (var process = Process.GetCurrentProcess())
                {
                    var utilizedCores = GetCoresLimitForNode(out var licenseLimits, firstRun == false);
                    var clusterSize = GetClusterSize();
                    var maxWorkingSet = Math.Min(LicenseStatus.MaxMemory / (double)clusterSize, utilizedCores * LicenseStatus.Ratio);

                    SetAffinity(process, utilizedCores, licenseLimits);
                    SetMaxWorkingSet(process, Math.Max(1, maxWorkingSet));
                }

                ValidateLicenseStatus();
            }
            catch (Exception e)
            {
                Logger.Info("Failed to reload license limits", e);
            }
        }

        public int GetCoresLimitForNode(out LicenseLimits licenseLimits, bool shouldPutNodeInfoIfNotExist = true)
        {
            licenseLimits = _serverStore.LoadLicenseLimits();
            if (licenseLimits?.NodeLicenseDetails != null &&
                licenseLimits.NodeLicenseDetails.TryGetValue(_serverStore.NodeTag, out var detailsPerNode))
            {
                return Math.Min(detailsPerNode.UtilizedCores, LicenseStatus.MaxCores);
            }

            // we don't have any license limits for this node, let's put our info to update it
            if(shouldPutNodeInfoIfNotExist)
                Task.Run(async () => await PutMyNodeInfoAsync()).IgnoreUnobservedExceptions();
            return Math.Min(ProcessorInfo.ProcessorCount, LicenseStatus.MaxCores);
        }

        private int GetClusterSize()
        {
            if (_serverStore.IsPassive())
                return 1;

            return _serverStore.GetClusterTopology().AllNodes.Count;
        }

        public async Task ChangeLicenseLimits(string nodeTag, int? maxUtilizedCores, string raftRequestId)
        {
            var licenseLimits = _serverStore.LoadLicenseLimits();

            DetailsPerNode detailsPerNode = null;
            licenseLimits?.NodeLicenseDetails.TryGetValue(nodeTag, out detailsPerNode);

            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var allNodes = _serverStore.GetClusterTopology(context).AllNodes;
                if (allNodes.TryGetValue(nodeTag, out var nodeUrl) == false)
                    throw new ArgumentException($"Node tag `{nodeTag}` isn't part of the cluster");

                if (nodeTag == _serverStore.NodeTag)
                {
                    detailsPerNode ??= new DetailsPerNode();
                    detailsPerNode.NumberOfCores = ProcessorInfo.ProcessorCount;

                    var memoryInfo = _serverStore.Server.MetricCacher.GetValue<MemoryInfoResult>(MetricCacher.Keys.Server.MemoryInfo);
                    detailsPerNode.InstalledMemoryInGb = memoryInfo.InstalledMemory.GetDoubleValue(SizeUnit.Gigabytes);
                    detailsPerNode.UsableMemoryInGb = memoryInfo.TotalPhysicalMemory.GetDoubleValue(SizeUnit.Gigabytes);
                    detailsPerNode.BuildInfo = BuildInfo;
                    detailsPerNode.OsInfo = OsInfo;
                }
                else
                {
                    var nodeInfo = await GetNodeInfo(nodeUrl, context);
                    if (nodeInfo != null)
                    {
                        detailsPerNode ??= new DetailsPerNode();
                        detailsPerNode.NumberOfCores = nodeInfo.NumberOfCores;
                        detailsPerNode.InstalledMemoryInGb = nodeInfo.InstalledMemoryInGb;
                        detailsPerNode.UsableMemoryInGb = nodeInfo.UsableMemoryInGb;
                        detailsPerNode.BuildInfo = nodeInfo.BuildInfo;
                        detailsPerNode.OsInfo = nodeInfo.OsInfo;
                    }
                    else if (detailsPerNode == null)
                    {
                        throw new InvalidOperationException($"Node tag: {nodeTag} with node url: {nodeUrl} cannot be reached");
                    }
                }

                Debug.Assert(detailsPerNode != null);
                Debug.Assert(detailsPerNode.NumberOfCores > 0);

                detailsPerNode.MaxUtilizedCores = maxUtilizedCores;
            }

            await _serverStore.PutNodeLicenseLimitsAsync(nodeTag, detailsPerNode, LicenseStatus.MaxCores, raftRequestId);
        }

        private async Task<NodeInfo> GetNodeInfo(string nodeUrl, TransactionOperationContext ctx)
        {
            using (var requestExecutor = ClusterRequestExecutor.CreateForSingleNode(nodeUrl, _serverStore.Server.Certificate.Certificate))
            {
                var infoCmd = new GetNodeInfoCommand(TimeSpan.FromSeconds(15));

                try
                {
                    await requestExecutor.ExecuteAsync(infoCmd, ctx);
                }
                catch (Exception)
                {
                    return null;
                }

                return infoCmd.Result;
            }
        }

        private int GetUtilizedCores()
        {
            var licenseLimits = _serverStore.LoadLicenseLimits();
            var detailsPerNode = licenseLimits?.NodeLicenseDetails;
            if (detailsPerNode == null)
                return 0;

            return detailsPerNode.Sum(x => x.Value.UtilizedCores);
        }

        public async Task ActivateAsync(License license, string raftRequestId, bool skipGettingUpdatedLicense = false)
        {
            var licenseStatus = GetLicenseStatus(license);
            if (licenseStatus.Expiration.HasValue == false)
                throw new LicenseExpiredException("License doesn't have an expiration date!");

            if (licenseStatus.Expired)
            {
                if (skipGettingUpdatedLicense)
                    throw new LicenseExpiredException($"License already expired on: {licenseStatus.Expiration}");

                try
                {
                    // license expired, we'll try to update it
                    var updatedLicense = await GetUpdatedLicenseInternal(license);
                    if (updatedLicense == null)
                        throw new LicenseExpiredException($"License already expired on: {licenseStatus.Expiration} and we failed to get an updated one from {ApiHttpClient.ApiRavenDbNet}.");

                    await ActivateAsync(updatedLicense, raftRequestId, skipGettingUpdatedLicense: true);
                    return;
                }
                catch (Exception e)
                {
                    if (e is HttpRequestException)
                        throw new LicenseExpiredException($"License already expired on: {licenseStatus.Expiration} and we were unable to get an updated license. Please make sure you that you have access to {ApiHttpClient.ApiRavenDbNet}.", e);

                    throw new LicenseExpiredException($"License already expired on: {licenseStatus.Expiration} and we were unable to get an updated license.", e);
                }
            }

            ThrowIfCannotActivateLicense(licenseStatus);

            try
            {
                await _serverStore.PutLicenseAsync(license, raftRequestId).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                var message = $"Could not save the following license:{Environment.NewLine}" +
                              $"Id: {license.Id}{Environment.NewLine}" +
                              $"Name: {license.Name}{Environment.NewLine}" +
                              $"Keys: [{(license.Keys != null ? string.Join(", ", license.Keys) : "N/A")}]";

                if (Logger.IsInfoEnabled)
                    Logger.Info(message, e);

                throw new InvalidOperationException("Could not save license!", e);
            }
        }

        private void ResetLicense(string error)
        {
            LicenseStatus = new LicenseStatus
            {
                FirstServerStartDate = LicenseStatus.FirstServerStartDate,
                ErrorMessage = error,
            };
        }

        private void SetLicense(LicenseStatus licenseStatus)
        {
            LicenseStatus = new LicenseStatus
            {
                Id = licenseStatus.Id,
                LicensedTo = licenseStatus.LicensedTo,
                ErrorMessage = null,
                Attributes = licenseStatus.Attributes,
                FirstServerStartDate = LicenseStatus.FirstServerStartDate,
            };
        }

        public static LicenseStatus GetLicenseStatus(License license)
        {
            Dictionary<string, object> licenseAttributes;

            try
            {
                licenseAttributes = LicenseValidator.Validate(license, _rsaParameters);
            }
            catch (Exception e)
            {
                var message = $"Could not validate the following license:{Environment.NewLine}" +
                              $"Id: {license.Id}{Environment.NewLine}" +
                              $"Name: {license.Name}{Environment.NewLine}" +
                              $"Keys: [{(license.Keys != null ? string.Join(", ", license.Keys) : "N/A")}]";

                if (Logger.IsInfoEnabled)
                    Logger.Info(message, e);

                throw new InvalidDataException("Could not validate license!", e);
            }

            var licenseStatus = new LicenseStatus
            {
                Id = license.Id,
                Attributes = licenseAttributes,
                LicensedTo = license.Name
            };

            return licenseStatus;
        }

        public async Task TryActivateLicenseAsync(bool throwOnActivationFailure)
        {
            if (LicenseStatus.Type != LicenseType.None)
                return;

            var license = _licenseHelper.TryGetLicenseFromString(throwOnActivationFailure) ??
                          _licenseHelper.TryGetLicenseFromPath(throwOnActivationFailure);
            if (license == null)
                return;

            try
            {
                await ActivateAsync(license, RaftIdGenerator.NewId());
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Failed to activate license", e);

                if (throwOnActivationFailure)
                    throw new LicenseActivationException("Failed to activate license", e);
            }
        }

        public async Task<License> GetUpdatedLicense(
            License currentLicense,
            Func<HttpResponseMessage, Task> onFailure = null,
            Func<LeasedLicense, License> onSuccess = null)
        {
            if (_serverStore.Configuration.Licensing.DisableAutoUpdate)
            {
                if (_skipLeasingErrorsLogging == false && Logger.IsInfoEnabled)
                {
                    // ReSharper disable once MethodHasAsyncOverload
                    Logger.Info("Skipping updating of the license because 'Licensing.DisableAutoLicenceUpdate' was set to true");
                }
                return null;
            }
            var leaseLicenseInfo = GetLeaseLicenseInfo(currentLicense);
            var response = await ApiHttpClient.Instance.PostAsync("/api/v2/license/lease",
                    new StringContent(JsonConvert.SerializeObject(leaseLicenseInfo), Encoding.UTF8, "application/json"))
                .ConfigureAwait(false);

            if (response.IsSuccessStatusCode == false)
            {
                if (onFailure != null)
                {
                    await onFailure(response).ConfigureAwait(false);
                }

                return null;
            }

            var leasedLicenseAsStream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var json = await context.ReadForMemoryAsync(leasedLicenseAsStream, "leased license info");
                var leasedLicense = JsonDeserializationServer.LeasedLicense(json);

                if (onSuccess == null)
                    return leasedLicense.License;

                return onSuccess.Invoke(leasedLicense);
            }
        }

        private LeaseLicenseInfo GetLeaseLicenseInfo(License license)
        {
            return new LeaseLicenseInfo
            {
                License = license,
                BuildInfo = BuildInfo,
                OsInfo = OsInfo,
                ClusterId = _serverStore.GetClusterTopology().TopologyId,
                UtilizedCores = GetUtilizedCores(),
                NodeTag = _serverStore.NodeTag,
                StudioEnvironment = GetStudioEnvironment()
            };
        }

        private StudioConfiguration.StudioEnvironment GetStudioEnvironment()
        {
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var studioConfigurationJson = _serverStore.Cluster.Read(context, Constants.Configuration.StudioId, out long _);
                if (studioConfigurationJson == null)
                    return StudioConfiguration.StudioEnvironment.None;

                var studioConfiguration = JsonDeserializationServer.ServerWideStudioConfiguration(studioConfigurationJson);

                return studioConfiguration.Disabled ?
                    StudioConfiguration.StudioEnvironment.None :
                    studioConfiguration.Environment;
            }
        }

        private async Task<License> GetUpdatedLicenseInternal(License currentLicense)
        {
            return await GetUpdatedLicense(currentLicense,
                    async response =>
                    {
                        var responseString = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                        AddLeaseLicenseError($"status code: {response.StatusCode}, response: {responseString}");
                    },
                    leasedLicense =>
                    {
                        var newLicense = leasedLicense.License;
                        var licenseChanged =
                            newLicense.Name != currentLicense.Name ||
                            newLicense.Id != currentLicense.Id ||
                            newLicense.Keys.All(currentLicense.Keys.Contains) == false;

                        if (string.IsNullOrWhiteSpace(leasedLicense.Message) == false)
                        {
                            var severity =
                                leasedLicense.NotificationSeverity == NotificationSeverity.None
                                    ? NotificationSeverity.Info : leasedLicense.NotificationSeverity;
                            var alert = AlertRaised.Create(
                                null,
                                leasedLicense.Title,
                                leasedLicense.Message,
                                AlertType.LicenseManager_LicenseUpdateMessage,
                                severity);

                            _serverStore.NotificationCenter.Add(alert);
                        }

                        if (string.IsNullOrWhiteSpace(leasedLicense.ErrorMessage) == false)
                        {
                            LicenseStatus.ErrorMessage = leasedLicense.ErrorMessage;
                        }

                        return licenseChanged ? leasedLicense.License : null;
                    })
                .ConfigureAwait(false);
        }

        private async Task ExecuteTasks()
        {
            try
            {
                await LeaseLicense(RaftIdGenerator.NewId(), throwOnError: false);

                await PutMyNodeInfoAsync();

                ReloadLicenseLimits();
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Failed to execute tasks", e);
            }
        }

        public async Task<LicenseRenewalResult> RenewLicense()
        {
            var license = _serverStore.LoadLicense();

            if (license == null)
                throw new InvalidOperationException("License not found");

            var response = await ApiHttpClient.Instance.PostAsync("/api/v2/license/renew",
                    new StringContent(JsonConvert.SerializeObject(license), Encoding.UTF8, "application/json"))
                .ConfigureAwait(false);

            var responseString = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
            if (response.IsSuccessStatusCode == false)
                throw new InvalidOperationException($"Failed to renew license, error: {responseString}");

            return JsonConvert.DeserializeObject<LicenseRenewalResult>(responseString);
        }

        public async Task LeaseLicense(string raftRequestId, bool throwOnError)
        {
            if (await _leaseLicenseSemaphore.WaitAsync(0) == false)
                return;

            try
            {
                var loadedLicense = _serverStore.LoadLicense();
                if (loadedLicense == null)
                    return;

                var updatedLicense = await GetUpdatedLicenseInternal(loadedLicense);
                if (updatedLicense == null)
                    return;

                var licenseStatus = GetLicenseStatus(updatedLicense);

                try
                {
                    // we'll activate the license from the license server
                    await _serverStore.PutLicenseAsync(updatedLicense, raftRequestId).ConfigureAwait(false);
                }
                catch
                {
                    // we want to set the new license status anyway
                    SetLicense(licenseStatus);
                    throw;
                }

                var alert = AlertRaised.Create(
                    null,
                    "License was updated",
                    "Successfully leased license",
                    AlertType.LicenseManager_LeaseLicenseSuccess,
                    NotificationSeverity.Info);

                _serverStore.NotificationCenter.Add(alert);
            }
            catch (Exception e)
            {
                var message = e is HttpRequestException ? "failed to connect to api.ravendb.net" : "see exception details";
                AddLeaseLicenseError(message, e);

                if (throwOnError)
                    throw;
            }
            finally
            {
                _leaseLicenseSemaphore.Release();
            }
        }

        private void ValidateLicenseStatus()
        {
            var licenseLimits = _serverStore.LoadLicenseLimits();
            if (licenseLimits == null)
                return;

            var utilizedCores = licenseLimits.NodeLicenseDetails.Sum(x => x.Value.UtilizedCores);
            string errorMessage = null;
            if (utilizedCores > LicenseStatus.MaxCores)
            {
                errorMessage = $"The number of utilized cores is {utilizedCores}, " +
                              $"while the license limit is {LicenseStatus.MaxCores} cores";
            }
            else if (licenseLimits.NodeLicenseDetails.Count > LicenseStatus.MaxClusterSize)
            {
                errorMessage = $"The cluster size is {licenseLimits.NodeLicenseDetails.Count}, " +
                              $"while the license limit is {LicenseStatus.MaxClusterSize}";
            }

            if (errorMessage != null)
                LicenseStatus.ErrorMessage = errorMessage;
        }

        private void AddLeaseLicenseError(string errorMessage, Exception exception = null)
        {
            if (_skipLeasingErrorsLogging)
                return;

            if (LicenseStatus.Expired == false &&
                LicenseStatus.Expiration != null &&
                LicenseStatus.Expiration.Value.Subtract(DateTime.UtcNow).TotalDays > 3 &&
                (exception == null || exception is HttpRequestException))
            {
                // ignore the error if the license isn't expired yet
                // and we don't have access to api.ravendb.net from this machine
                return;
            }

            const string title = "Failed to lease license";
            if (Logger.IsInfoEnabled)
            {
                Logger.Info($"{title}, {errorMessage}", exception);
            }

            var alert = AlertRaised.Create(
                null,
                title,
                "Could not lease license",
                AlertType.LicenseManager_LeaseLicenseError,
                NotificationSeverity.Warning,
                details: new ExceptionDetails(
                    new InvalidOperationException(errorMessage, exception)));

            _serverStore.NotificationCenter.Add(alert);
        }

        private void SetAffinity(Process process, int cores, LicenseLimits licenseLimits)
        {
            if (cores > ProcessorInfo.ProcessorCount)
                cores = ProcessorInfo.ProcessorCount;

            try
            {
                if (ShouldIgnoreProcessorAffinityChanges(cores, licenseLimits))
                    return;

                AffinityHelper.SetProcessAffinity(process, cores, _serverStore.Configuration.Server.ProcessAffinityMask, out var currentlyAssignedCores);

                if (cores == ProcessorInfo.ProcessorCount)
                {
                    _serverStore.NotificationCenter.Dismiss(PerformanceHint.GetKey(PerformanceHintType.UnusedCapacity, nameof(LicenseManager)));
                    _lastPerformanceHint = null;
                    return;
                }

                if (currentlyAssignedCores == cores &&
                    _lastPerformanceHint != null &&
                    _lastPerformanceHint.Value.AddDays(7) > DateTime.UtcNow)
                {
                    return;
                }

                _lastPerformanceHint = DateTime.UtcNow;

                _serverStore.NotificationCenter.Add(PerformanceHint.Create(
                    null,
                    "Your database can be faster - not all cores are used",
                    $"Your server is currently using only {cores} core{Pluralize(cores)} " +
                    $"out of the {Environment.ProcessorCount} that it has available",
                    PerformanceHintType.UnusedCapacity,
                    NotificationSeverity.Info,
                    nameof(LicenseManager)));
            }
            catch (PlatformNotSupportedException)
            {
                // nothing to do
            }
            catch (NotSupportedException)
            {
                // nothing to do
            }
            catch (Exception e)
            {
                Logger.Info($"Failed to set affinity for {cores} cores, error code: {Marshal.GetLastWin32Error()}", e);
            }
        }

        private bool ShouldIgnoreProcessorAffinityChanges(int cores, LicenseLimits licenseLimits)
        {
            if (IgnoreProcessorAffinityChanges)
            {
                if (ProcessorInfo.ProcessorCount != cores)
                {
                    var basicMessage = "Ignore request for setting processor affinity. " +
                                       $"Requested cores: {cores}. " +
                                       $"Number of cores on the machine: {ProcessorInfo.ProcessorCount}. ";
                    var licenseMessage = string.Empty;

                    if (licenseLimits != null)
                    {
                        licenseMessage = $"License limits: {string.Join(", ", licenseLimits.NodeLicenseDetails.Select(x => $"{x.Key}: {x.Value.UtilizedCores}/{x.Value.NumberOfCores}"))}. " +
                                         $"Total utilized cores: {licenseLimits.TotalUtilizedCores}. " +
                                         $"Max licensed cores: {LicenseStatus.MaxCores}";
                    }
                    Console.WriteLine(basicMessage + " " + licenseMessage);
                }

                return true;
            }

            return false;
        }

        private static void SetMaxWorkingSet(Process process, double ramInGb)
        {
            try
            {
                Extensions.MemoryExtensions.SetWorkingSet(process, ramInGb, Logger);
            }
            catch (Exception e)
            {
                Logger.Info($"Failed to set max working set to {ramInGb}GB, error code: {Marshal.GetLastWin32Error()}", e);
            }
        }

        public void Dispose()
        {
            _leaseLicenseTimer?.Dispose();
        }

        private void ThrowIfCannotActivateLicense(LicenseStatus newLicenseStatus)
        {
            var clusterSize = GetClusterSize();
            var maxClusterSize = newLicenseStatus.MaxClusterSize;
            if (clusterSize > maxClusterSize)
            {
                var message = "Cannot activate license because the maximum allowed cluster size is: " +
                              $"{maxClusterSize} while the current cluster size is: {clusterSize}";
                throw GenerateLicenseLimit(LimitType.ClusterSize, message);
            }

            var maxCores = newLicenseStatus.MaxCores;
            if (clusterSize > maxCores)
            {
                var message = "Cannot activate license because the cores limit is: " +
                              $"{maxCores} while the current cluster size is: {clusterSize}!";
                throw GenerateLicenseLimit(LimitType.Cores, message);
            }

            if (_serverStore.Configuration.Monitoring.Snmp.Enabled &&
                newLicenseStatus.HasSnmpMonitoring == false)
            {
                const string message = "SNMP Monitoring is currently enabled. " +
                                       "The provided license cannot be activated as it doesn't contain this feature. " +
                                       "In order to use this license please disable SNMP Monitoring in the server configuration";
                throw GenerateLicenseLimit(LimitType.Snmp, message);
            }

            var encryptedDatabasesCount = 0;
            var externalReplicationCount = 0;
            var delayedExternalReplicationCount = 0;
            var pullReplicationAsHubCount = 0;
            var documentsCompressionCount = 0;
            var pullReplicationAsSinkCount = 0;
            var timeSeriesRollupsAndRetentionCount = 0;
            var ravenEtlCount = 0;
            var sqlEtlCount = 0;
            var snapshotBackupsCount = 0;
            var cloudBackupsCount = 0;
            var encryptedBackupsCount = 0;
            var dynamicNodesDistributionCount = 0;
            var additionalAssembliesFromNuGetCount = 0;

            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                foreach (var databaseRecord in _serverStore.Cluster.GetAllDatabases(context))
                {
                    if (databaseRecord.Encrypted)
                        encryptedDatabasesCount++;

                    if (databaseRecord.Topology != null && databaseRecord.Topology.DynamicNodesDistribution)
                        dynamicNodesDistributionCount++;

                    if (databaseRecord.ExternalReplications != null &&
                        databaseRecord.ExternalReplications.Count > 0)
                        externalReplicationCount++;

                    if (databaseRecord.ExternalReplications != null &&
                        databaseRecord.ExternalReplications.Count(x => x.DelayReplicationFor != TimeSpan.Zero) > 0)
                        delayedExternalReplicationCount++;

                    if (databaseRecord.HubPullReplications != null &&
                        databaseRecord.HubPullReplications.Count > 0)
                        pullReplicationAsHubCount++;

                    if (HasDocumentsCompression(databaseRecord.DocumentsCompression))
                        documentsCompressionCount++;

                    if (databaseRecord.SinkPullReplications != null &&
                        databaseRecord.SinkPullReplications.Count > 0)
                        pullReplicationAsSinkCount++;

                    if (HasTimeSeriesRollupsAndRetention(databaseRecord.TimeSeries))
                        timeSeriesRollupsAndRetentionCount++;

                    if (HasRavenEtl(databaseRecord.RavenEtls,
                        databaseRecord.RavenConnectionStrings))
                        ravenEtlCount++;

                    if (HasAdditionalAssembliesFromNuGet(databaseRecord.Indexes))
                        additionalAssembliesFromNuGetCount++;

                    if (databaseRecord.SqlEtls != null &&
                        databaseRecord.SqlEtls.Count > 0)
                        sqlEtlCount++;

                    var backupTypes = GetBackupTypes(databaseRecord.PeriodicBackups);
                    if (backupTypes.HasSnapshotBackup)
                        snapshotBackupsCount++;

                    if (backupTypes.HasCloudBackup)
                        cloudBackupsCount++;

                    if (backupTypes.HasEncryptedBackup)
                        encryptedBackupsCount++;
                }
            }

            if (encryptedDatabasesCount > 0 && newLicenseStatus.HasEncryption == false)
            {
                var message = GenerateDetails(encryptedDatabasesCount, "encryption");
                throw GenerateLicenseLimit(LimitType.Encryption, message);
            }

            if (externalReplicationCount > 0 && newLicenseStatus.HasExternalReplication == false)
            {
                var message = GenerateDetails(externalReplicationCount, "external replication");
                throw GenerateLicenseLimit(LimitType.ExternalReplication, message);
            }

            if (delayedExternalReplicationCount > 0 && newLicenseStatus.HasDelayedExternalReplication == false)
            {
                var message = GenerateDetails(externalReplicationCount, "delayed external replication");
                throw GenerateLicenseLimit(LimitType.DelayedExternalReplication, message);
            }

            if (pullReplicationAsHubCount > 0 && newLicenseStatus.HasPullReplicationAsHub == false)
            {
                var message = GenerateDetails(pullReplicationAsHubCount, "pull replication as hub");
                throw GenerateLicenseLimit(LimitType.PullReplicationAsHub, message);
            }

            if (pullReplicationAsSinkCount > 0 && newLicenseStatus.HasPullReplicationAsSink == false)
            {
                var message = GenerateDetails(pullReplicationAsSinkCount, "pull replication as sink");
                throw GenerateLicenseLimit(LimitType.PullReplicationAsSink, message);
            }

            if (timeSeriesRollupsAndRetentionCount > 0 && newLicenseStatus.HasTimeSeriesRollupsAndRetention == false)
            {
                var message = GenerateDetails(timeSeriesRollupsAndRetentionCount, "time series rollups and retention");
                throw GenerateLicenseLimit(LimitType.TimeSeriesRollupsAndRetention, message);
            }

            if (additionalAssembliesFromNuGetCount > 0 && newLicenseStatus.HasAdditionalAssembliesFromNuGet == false)
            {
                var message = GenerateDetails(additionalAssembliesFromNuGetCount, "additional assemblies from NuGet");
                throw GenerateLicenseLimit(LimitType.AdditionalAssembliesFromNuGet, message);
            }

            if (ravenEtlCount > 0 && newLicenseStatus.HasRavenEtl == false)
            {
                var message = GenerateDetails(ravenEtlCount, "Raven ETL");
                throw GenerateLicenseLimit(LimitType.RavenEtl, message);
            }

            if (sqlEtlCount > 0 && newLicenseStatus.HasSqlEtl == false)
            {
                var message = GenerateDetails(sqlEtlCount, "SQL ETL");
                throw GenerateLicenseLimit(LimitType.SqlEtl, message);
            }

            if (snapshotBackupsCount > 0 && newLicenseStatus.HasSnapshotBackups == false)
            {
                var message = GenerateDetails(snapshotBackupsCount, "snapshot backups");
                throw GenerateLicenseLimit(LimitType.SnapshotBackup, message);
            }

            if (cloudBackupsCount > 0 && newLicenseStatus.HasCloudBackups == false)
            {
                var message = GenerateDetails(cloudBackupsCount, "cloud backups");
                throw GenerateLicenseLimit(LimitType.CloudBackup, message);
            }

            if (encryptedBackupsCount > 0 && newLicenseStatus.HasEncryptedBackups == false)
            {
                var message = GenerateDetails(cloudBackupsCount, "encrypted backups");
                throw GenerateLicenseLimit(LimitType.EncryptedBackup, message);
            }

            if (dynamicNodesDistributionCount > 0 && newLicenseStatus.HasDynamicNodesDistribution == false)
            {
                var message = GenerateDetails(dynamicNodesDistributionCount, "dynamic database distribution");
                throw GenerateLicenseLimit(LimitType.DynamicNodeDistribution, message);
            }

            if (documentsCompressionCount > 0 && newLicenseStatus.HasDocumentsCompression == false)
            {
                var message = GenerateDetails(documentsCompressionCount, "documents compression");
                throw GenerateLicenseLimit(LimitType.DocumentsCompression, message);
            }
        }

        private static string GenerateDetails(int count, string feature)
        {
            return $"Cannot activate license because there {(count == 1 ? "is" : "are")} " +
                   $"{count} database{Pluralize(count)} that use{(count == 1 ? "s" : string.Empty)} {feature} " +
                   $"while the new license doesn't include the usage of {feature}";
        }

        private static string Pluralize(int count)
        {
            return count == 1 ? string.Empty : "s";
        }

        private static bool HasRavenEtl(List<RavenEtlConfiguration> ravenEtls,
            Dictionary<string, RavenConnectionString> ravenConnectionStrings)
        {
            if (ravenEtls == null)
                return false;

            foreach (var ravenEtl in ravenEtls)
            {
                if (ravenConnectionStrings.TryGetValue(ravenEtl.ConnectionStringName, out var _) == false)
                    continue;

                return true;
            }

            return false;
        }

        private static bool HasDocumentsCompression(DocumentsCompressionConfiguration documentsCompression)
        {
            return documentsCompression?.CompressRevisions == true ||
                   documentsCompression?.Collections?.Length > 0;
        }

        private static bool HasTimeSeriesRollupsAndRetention(TimeSeriesConfiguration configuration)
        {
            if (configuration?.Collections == null)
                return false;

            return configuration.Collections.Any(x => x.Value != null && x.Value.Disabled == false);
        }

        private static bool HasAdditionalAssembliesFromNuGet(Dictionary<string, IndexDefinition> indexes)
        {
            if (indexes == null || indexes.Count == 0)
                return false;

            foreach (var kvp in indexes)
            {
                if (HasAdditionalAssembliesFromNuGet(kvp.Value))
                    return true;
            }

            return false;
        }

        private static bool HasAdditionalAssembliesFromNuGet(IndexDefinition indexDefinition)
        {
            if (indexDefinition == null)
                return false;

            var additionalAssemblies = indexDefinition.AdditionalAssemblies;
            if (additionalAssemblies == null || additionalAssemblies.Count == 0)
                return false;

            foreach (var additionalAssembly in additionalAssemblies)
            {
                if (string.IsNullOrEmpty(additionalAssembly.PackageName))
                    continue;

                return true;
            }

            return false;
        }

        private static (bool HasSnapshotBackup, bool HasCloudBackup, bool HasEncryptedBackup) GetBackupTypes(
            IEnumerable<PeriodicBackupConfiguration> periodicBackups)
        {
            var hasSnapshotBackup = false;
            var hasCloudBackup = false;
            var hasEncryptedBackup = false;
            foreach (var configuration in periodicBackups)
            {
                hasSnapshotBackup |= configuration.BackupType == BackupType.Snapshot;
                hasCloudBackup |= configuration.HasCloudBackup();
                hasEncryptedBackup |= HasEncryptedBackup(configuration);

                if (hasSnapshotBackup && hasCloudBackup && hasEncryptedBackup)
                    return (true, true, true);
            }

            return (hasSnapshotBackup, hasCloudBackup, hasEncryptedBackup);
        }

        public void AssertCanAddNode()
        {
            if (IsValid(out var licenseLimit) == false)
                throw licenseLimit;

            var allNodesCount = _serverStore.GetClusterTopology().AllNodes.Count;
            if (LicenseStatus.MaxCores <= allNodesCount)
            {
                var message = $"Cannot add the node to the cluster because the number of licensed cores is {LicenseStatus.MaxCores} " +
                              $"while the number of nodes is {allNodesCount}. This will bring the number of utilized cores over the limit";
                throw GenerateLicenseLimit(LimitType.Cores, message);
            }

            if (LicenseStatus.DistributedCluster == false)
            {
                var message = $"Your current license ({LicenseStatus.Type}) does not allow adding nodes to the cluster";
                throw GenerateLicenseLimit(LimitType.ForbiddenHost, message);
            }

            var maxClusterSize = LicenseStatus.MaxClusterSize;
            var clusterSize = GetClusterSize();
            if (++clusterSize > maxClusterSize)
            {
                var message = $"Your current license allows up to {maxClusterSize} nodes in a cluster";
                throw GenerateLicenseLimit(LimitType.ClusterSize, message);
            }
        }

        public void AssertCanAddAdditionalAssembliesFromNuGet(IndexDefinition indexDefinition)
        {
            if (IsValid(out var licenseLimit) == false)
                throw licenseLimit;

            if (LicenseStatus.HasAdditionalAssembliesFromNuGet)
                return;

            if (HasAdditionalAssembliesFromNuGet(indexDefinition) == false)
                return;

            const string details = "Your current license doesn't include the additional assemblies from NuGet feature";
            throw GenerateLicenseLimit(LimitType.AdditionalAssembliesFromNuGet, details);
        }

        public void AssertCanAddPeriodicBackup(Client.Documents.Operations.Backups.BackupConfiguration configuration)
        {
            if (IsValid(out var licenseLimit) == false)
                throw licenseLimit;

            if (configuration.BackupType == BackupType.Snapshot &&
                LicenseStatus.HasSnapshotBackups == false)
            {
                const string details = "Your current license doesn't include the snapshot backups feature";
                throw GenerateLicenseLimit(LimitType.SnapshotBackup, details);
            }

            if (configuration.HasCloudBackup() && LicenseStatus.HasCloudBackups == false)
            {
                const string details = "Your current license doesn't include the backup to cloud or ftp feature!";
                throw GenerateLicenseLimit(LimitType.CloudBackup, details);
            }

            if (HasEncryptedBackup(configuration) && LicenseStatus.HasEncryptedBackups == false)
            {
                const string details = "Your current license doesn't include the encrypted backup feature!";
                throw GenerateLicenseLimit(LimitType.CloudBackup, details);
            }
        }

        public static bool HasEncryptedBackup(Client.Documents.Operations.Backups.BackupConfiguration configuration)
        {
            if (configuration.BackupEncryptionSettings == null)
                return false;

            if (configuration.BackupEncryptionSettings.EncryptionMode == EncryptionMode.None)
                return false;

            return true;
        }

        public void AssertCanAddExternalReplication(TimeSpan delayReplicationFor)
        {
            if (IsValid(out var licenseLimit) == false)
                throw licenseLimit;

            if (LicenseStatus.HasExternalReplication == false)
            {
                var details = $"Your current license ({LicenseStatus.Type}) does not allow adding external replication";
                throw GenerateLicenseLimit(LimitType.ExternalReplication, details);
            }

            AssertCanDelayReplication(delayReplicationFor);
        }

        public void AssertCanDelayReplication(TimeSpan delayReplicationFor)
        {
            if (IsValid(out var licenseLimit) == false)
                throw licenseLimit;

            if (LicenseStatus.HasDelayedExternalReplication)
                return;

            if (delayReplicationFor.Ticks == 0)
                return;

            const string message = "Your current license doesn't include the delayed replication feature";
            throw GenerateLicenseLimit(LimitType.DelayedExternalReplication, message, addNotification: true);
        }

        public void AssertCanUseDocumentsCompression(DocumentsCompressionConfiguration documentsCompression)
        {
            var hasDocumentsCompression = HasDocumentsCompression(documentsCompression);

            if (IsValid(out var licenseLimit) == false)
                throw licenseLimit;

            if (LicenseStatus.HasDocumentsCompression)
                return;

            if (hasDocumentsCompression == false)
                return;

            var details = $"Your current license ({LicenseStatus.Type}) does not allow documents compression";
            throw GenerateLicenseLimit(LimitType.DocumentsCompression, details);
        }

        public void AssertCanAddPullReplicationAsHub()
        {
            if (IsValid(out var licenseLimit) == false)
                throw licenseLimit;

            if (LicenseStatus.HasPullReplicationAsHub)
                return;

            var details = $"Your current license ({LicenseStatus.Type}) does not allow adding pull replication as hub";
            throw GenerateLicenseLimit(LimitType.PullReplicationAsHub, details);
        }

        public void AssertCanAddPullReplicationAsSink()
        {
            if (IsValid(out var licenseLimit) == false)
                throw licenseLimit;

            if (LicenseStatus.HasPullReplicationAsSink)
                return;

            var details = $"Your current license ({LicenseStatus.Type}) does not allow adding pull replication as sink";
            throw GenerateLicenseLimit(LimitType.PullReplicationAsSink, details);
        }

        public void AssertCanAddTimeSeriesRollupsAndRetention(TimeSeriesConfiguration configuration)
        {
            if (IsValid(out var licenseLimit) == false)
                throw licenseLimit;

            if (LicenseStatus.HasTimeSeriesRollupsAndRetention)
                return;

            if (HasTimeSeriesRollupsAndRetention(configuration) == false)
                return;

            var details = $"Your current license ({LicenseStatus.Type}) does not allow adding time series rollups and retention";
            throw GenerateLicenseLimit(LimitType.TimeSeriesRollupsAndRetention, details);
        }

        public void AssertCanAddRavenEtl()
        {
            if (IsValid(out var licenseLimit) == false)
                throw licenseLimit;

            if (LicenseStatus.HasRavenEtl)
                return;

            const string message = "Your current license doesn't include the RavenDB ETL feature";
            throw GenerateLicenseLimit(LimitType.RavenEtl, message);
        }

        public void AssertCanAddSqlEtl()
        {
            if (IsValid(out var licenseLimit) == false)
                throw licenseLimit;

            if (LicenseStatus.HasSqlEtl != false)
                return;

            const string message = "Your current license doesn't include the SQL ETL feature";
            throw GenerateLicenseLimit(LimitType.SqlEtl, message);
        }

        public void AssertCanUseMonitoringEndpoints()
        {
            if (IsValid(out var licenseLimit) == false)
                throw licenseLimit;

            if (LicenseStatus.HasMonitoringEndpoints)
                return;

            const string details = "Your current license doesn't include the monitoring endpoints feature";
            throw GenerateLicenseLimit(LimitType.MonitoringEndpoints, details, addNotification: false);
        }

        public void AssertCanAddReadOnlyCertificates(CertificateDefinition certificate)
        {
            if (certificate.Permissions.Count == 0 || 
                certificate.Permissions.All(x => x.Value == DatabaseAccess.Read) == false)
                return;

            if (IsValid(out var licenseLimit) == false)
                throw licenseLimit;

            if (LicenseStatus.HasReadOnlyCertificates)
                return;

            const string details = "Your current license doesn't include the read-only certificates feature";
            throw GenerateLicenseLimit(LimitType.ReadOnlyCertificates, details);
        }

        public bool CanUseSnmpMonitoring(bool withNotification)
        {
            if (IsValid(out _) == false)
                return false;

            var value = LicenseStatus.HasSnmpMonitoring;
            if (withNotification == false)
                return value;

            if (value)
            {
                DismissLicenseLimit(LimitType.Snmp);
                return true;
            }

            const string details = "Your current license doesn't include the SNMP monitoring feature";
            GenerateLicenseLimit(LimitType.Snmp, details, addNotification: true);
            return false;
        }

        public bool CanDynamicallyDistributeNodes(bool withNotification, out LicenseLimitException licenseLimit)
        {
            if (IsValid(out licenseLimit) == false)
                return false;

            var value = LicenseStatus.HasDynamicNodesDistribution;
            if (withNotification == false)
                return value;

            if (value)
            {
                DismissLicenseLimit(LimitType.DynamicNodeDistribution);
                return true;
            }

            const string message = "Your current license doesn't include the dynamic database distribution feature";
            GenerateLicenseLimit(LimitType.DynamicNodeDistribution, message, addNotification: true);
            return false;
        }

        public bool HasHighlyAvailableTasks()
        {
            return LicenseStatus.HasHighlyAvailableTasks;
        }

        public static AlertRaised CreateHighlyAvailableTasksAlert(DatabaseTopology databaseTopology, IDatabaseTask databaseTask, string lastResponsibleNode)
        {
            var taskName = databaseTask.GetTaskName();
            var taskType = GetTaskType(databaseTask);
            var nodeState = GetNodeState(databaseTopology, lastResponsibleNode);
            var message = $"Cannot redistribute the {taskType} task: '{taskName}'";
            var alert = AlertRaised.Create(
                null,
                $@"You've reached a license limit ({EnumHelper.GetDescription(LimitType.HighlyAvailableTasks)})",
                message,
                AlertType.LicenseManager_HighlyAvailableTasks,
                NotificationSeverity.Warning,
                key: message,
                details: new MessageDetails
                {
                    Message = $"The {taskType} task: '{taskName}' will not be redistributed " +
                              $"to a healthy node because the current license doesn't include the highly available tasks feature." + Environment.NewLine +
                              $"Task's last responsible node '{lastResponsibleNode}', is currently {nodeState} and will continue to execute the {GetTaskType(databaseTask, lower: true)} task." + Environment.NewLine +
                              $"You can choose a different mentor node that will execute this task " +
                              $"(current mentor node state: {GetMentorNodeState(databaseTask, databaseTopology, nodeState)}). " + Environment.NewLine + Environment.NewLine +
                              $"Upgrading the license will allow RavenDB to manage that automatically."
                });
            return alert;
        }

        private static string GetTaskType(IDatabaseTask databaseTask, bool lower = false)
        {
            switch (databaseTask)
            {
                case PeriodicBackupConfiguration _:
                    return lower == false ? "Backup" : "backup";

                case SubscriptionState _:
                    return lower == false ? "Subscription" : "subscription";

                case RavenEtlConfiguration _:
                    return "Raven ETL";

                case SqlEtlConfiguration _:
                    return "SQL ETL";

                case ExternalReplication _:
                    return lower == false ? "External Replication" : "external replication";

                default:
                    return string.Empty;
            }
        }

        private static string GetMentorNodeState(IDatabaseTask databaseTask, DatabaseTopology databaseTopology, string nodeState)
        {
            var mentorNode = databaseTask.GetMentorNode();
            return mentorNode == null ? "wasn't set" : $"'{mentorNode}' is {nodeState}";
        }

        private static string GetNodeState(DatabaseTopology databaseTopology, string nodeTag)
        {
            if (databaseTopology.Promotables.Contains(nodeTag))
                return "in a 'promotable' state";

            if (databaseTopology.Rehabs.Contains(nodeTag))
                return "in a 'rehab' state";

            return "not a part of the cluster";
        }

        public void AssertCanCreateEncryptedDatabase()
        {
            if (IsValid(out var licenseLimit) == false)
                throw licenseLimit;

            if (LicenseStatus.HasEncryption)
                return;

            const string message = "Your current license doesn't include the encryption feature";
            throw GenerateLicenseLimit(LimitType.Encryption, message);
        }

        private void DismissLicenseLimit(LimitType limitType)
        {
            LicenseLimitWarning.DismissLicenseLimitNotification(_serverStore.NotificationCenter, limitType);
        }

        private LicenseLimitException GenerateLicenseLimit(
            LimitType limitType,
            string message,
            bool addNotification = false)
        {
            var licenseLimit = new LicenseLimitException(limitType, message);
            if (addNotification)
                LicenseLimitWarning.AddLicenseLimitNotification(_serverStore.NotificationCenter, licenseLimit);

            return licenseLimit;
        }

        private bool IsValid(out LicenseLimitException licenseLimit)
        {
            if (LicenseStatus.Type != LicenseType.Invalid)
            {
                licenseLimit = null;
                return true;
            }

            const string message = "Cannot perform operation while the license is in invalid state!";
            licenseLimit = GenerateLicenseLimit(LimitType.InvalidLicense, message);
            return false;
        }

        public async Task<LicenseSupportInfo> GetLicenseSupportInfo()
        {
            var license = _serverStore.LoadLicense();
            if (license == null)
            {
                throw new InvalidOperationException("License doesn't exist");
            }

            if (_serverStore.Configuration.Licensing.DisableLicenseSupportCheck)
            {
                if (_skipLeasingErrorsLogging == false && Logger.IsInfoEnabled)
                {
                    Logger.Info("Skipping checking the license support options because 'Licensing.DisableLicenseSupportMode' is set to true");
                }
                return GetDefaultLicenseSupportInfo();
            }

            var leaseLicenseInfo = GetLeaseLicenseInfo(license);
            const int timeoutInSec = 5;
            try
            {
                using (var cts = new CancellationTokenSource(timeoutInSec * 1000))
                {
                    var response = await ApiHttpClient.Instance.PostAsync("/api/v2/license/support",
                            new StringContent(JsonConvert.SerializeObject(leaseLicenseInfo), Encoding.UTF8, "application/json"), cts.Token)
                        .ConfigureAwait(false);

                    if (response.IsSuccessStatusCode == false)
                    {
                        var responseString = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

                        var message = $"Couldn't get license support info, response: {responseString}, status code: {response.StatusCode}";
                        if (_skipLeasingErrorsLogging == false && Logger.IsInfoEnabled)
                            Logger.Info(message);

                        return GetDefaultLicenseSupportInfo();
                    }

                    var licenseSupportStream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);
                    using (var context = JsonOperationContext.ShortTermSingleUse())
                    {
                        var json = await context.ReadForMemoryAsync(licenseSupportStream, "license support info");
                        return _lastKnownSupportInfo = JsonDeserializationServer.LicenseSupportInfo(json);
                    }
                }
            }
            catch (Exception e)
            {
                if (e is HttpRequestException == false && e is OperationCanceledException == false)
                    throw;

                // couldn't reach api.ravendb.net
                if (_skipLeasingErrorsLogging == false && Logger.IsInfoEnabled)
                {
                    var message = @"Couldn't reach api.ravendb.net to get the support info";
                    if (e is TaskCanceledException)
                        message += $", the request was aborted after {timeoutInSec} seconds";
                    Logger.Info(message);
                }

                return GetDefaultLicenseSupportInfo();
            }
        }

        private LicenseSupportInfo GetDefaultLicenseSupportInfo()
        {
            if (_lastKnownSupportInfo != null)
                return _lastKnownSupportInfo;

            return _lastKnownSupportInfo = new LicenseSupportInfo
            {
                Status = Status.NoSupport
            };
        }

        public async Task AcceptEulaAsync()
        {
            if (_eulaAcceptedButHasPendingRestart)
                return;

            await _locker.WaitAsync();

            try
            {
                if (_eulaAcceptedButHasPendingRestart)
                    return;

                var settingsPath = _serverStore.Configuration.ConfigPath;

                using (_serverStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    BlittableJsonReaderObject settingsJson;

                    await using (var fs = SafeFileStream.Create(settingsPath, FileMode.Open, FileAccess.Read))
                    {
                        settingsJson = await context.ReadForMemoryAsync(fs, "settings-json");
                        settingsJson.Modifications = new DynamicJsonValue(settingsJson);
                        settingsJson.Modifications[RavenConfiguration.GetKey(x => x.Licensing.EulaAccepted)] = true;
                    }

                    var modifiedJsonObj = context.ReadObject(settingsJson, "modified-settings-json");

                    var indentedJson = SetupManager.IndentJsonString(modifiedJsonObj.ToString());
                    SetupManager.WriteSettingsJsonLocally(settingsPath, indentedJson);
                }
                _eulaAcceptedButHasPendingRestart = true;
            }
            finally
            {
                _locker.Release();
            }
        }
    }
}
