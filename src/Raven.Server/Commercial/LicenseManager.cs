using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Http;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.ETL;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.PeriodicBackup;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Platform.Posix;
using Sparrow.Utils;
using Voron;
using Voron.Platform.Posix;
using Size = Sparrow.Size;

namespace Raven.Server.Commercial
{
    public class LicenseManager : IDisposable
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<LicenseManager>("Server");
        private readonly LicenseStorage _licenseStorage = new LicenseStorage();
        private readonly LicenseStatus _licenseStatus = new LicenseStatus();
        private readonly BuildNumber _buildInfo;
        private Timer _leaseLicenseTimer;
        private bool _disableCalculatingLicenseLimits;
        private RSAParameters? _rsaParameters;
        private readonly ServerStore _serverStore;
        private readonly SemaphoreSlim _leaseLicenseSemaphore = new SemaphoreSlim(1);
        private readonly SemaphoreSlim _licenseLimitsSemaphore = new SemaphoreSlim(1);
        private readonly bool _skipLeasingErrorsLogging;

        public LicenseManager(ServerStore serverStore)
        {
            _serverStore = serverStore;
            _skipLeasingErrorsLogging = serverStore.Configuration.Licensing.SkipLeasingErrorsLogging;

            _buildInfo = new BuildNumber
            {
                BuildVersion = ServerVersion.Build,
                ProductVersion = ServerVersion.Version,
                CommitHash = ServerVersion.CommitHash,
                FullVersion = ServerVersion.FullVersion
            };

            _serverStore.LicenseChanged += (_, e) =>
            {
                ReloadLicense();
                ReloadLicenseLimits();
            };
            _serverStore.LicenseLimitsChanged += (_, e) => ReloadLicenseLimits();
        }

        private RSAParameters RSAParameters
        {
            get
            {
                if (_rsaParameters != null)
                    return _rsaParameters.Value;

                string publicKeyString;
                const string publicKeyPath = "Raven.Server.Commercial.RavenDB.public.json";
                using (var stream = typeof(LicenseManager).GetTypeInfo().Assembly.GetManifestResourceStream(publicKeyPath))
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
                return _rsaParameters.Value;
            }
        }

        public void Initialize(StorageEnvironment environment, TransactionContextPool contextPool)
        {
            try
            {
                _licenseStorage.Initialize(environment, contextPool);

                var firstServerStartDate = _licenseStorage.GetFirstServerStartDate();
                if (firstServerStartDate == null)
                {
                    firstServerStartDate = SystemTime.UtcNow;
                    _licenseStorage.SetFirstServerStartDate(firstServerStartDate.Value);
                }

                _licenseStatus.FirstServerStartDate = firstServerStartDate.Value;

                ReloadLicense();
                ReloadLicenseLimits();
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

        public LicenseStatus GetLicenseStatus()
        {
            return _licenseStatus;
        }

        private void ReloadLicense()
        {
            var license = _serverStore.LoadLicense();
            if (license == null)
            {
                // license is not active
                _licenseStatus.Attributes = null;
                _licenseStatus.Error = false;
                _licenseStatus.Message = null;
                _licenseStatus.Id = null;
                return;
            }

            try
            {
                _licenseStatus.Attributes = LicenseValidator.Validate(license, RSAParameters);
                _licenseStatus.Error = false;
                _licenseStatus.Message = null;
                _licenseStatus.Id = license.Id;
            }
            catch (Exception e)
            {
                _licenseStatus.Attributes = null;
                _licenseStatus.Error = true;
                _licenseStatus.Message = e.Message;
                _licenseStatus.Id = null;

                if (Logger.IsInfoEnabled)
                    Logger.Info("Could not validate license", e);

                var alert = AlertRaised.Create(
                    "License manager initialization error",
                    "Could not initialize the license manager",
                    AlertType.LicenseManager_InitializationError,
                    NotificationSeverity.Warning,
                    details: new ExceptionDetails(e));

                _serverStore.NotificationCenter.Add(alert);
            }
        }

        public void ReloadLicenseLimits()
        {
            try
            {
                var licenseLimits = _serverStore.LoadLicenseLimits();

                if (licenseLimits?.NodeLicenseDetails != null &&
                    licenseLimits.NodeLicenseDetails.TryGetValue(_serverStore.NodeTag, out var detailsPerNode))
                {
                    var cores = Math.Min(detailsPerNode.UtilizedCores, _licenseStatus.MaxCores);

                    var process = Process.GetCurrentProcess();
                    SetAffinity(process, cores);

                    var ratio = (int)Math.Ceiling(_licenseStatus.MaxMemory / (double)_licenseStatus.MaxCores);
                    var clusterSize = GetClusterSize();
                    var maxWorkingSet = Math.Min(_licenseStatus.MaxMemory / (double)clusterSize, cores * ratio);
                    SetMaxWorkingSet(process, Math.Max(1, maxWorkingSet));
                }
            }
            catch (Exception e)
            {
                Logger.Info("Failed to reload license limits", e);
            }

            CalculateLicenseLimits();
        }

        private int GetClusterSize()
        {
            if (_serverStore.IsPassive())
                return 1;

            return _serverStore.GetClusterTopology().AllNodes.Count;
        }

        public async Task<LicenseLimit> ChangeLicenseLimits(string nodeTag, int newAssignedCores)
        {
            if (_serverStore.IsLeader() == false)
                throw new InvalidOperationException("Only the leader is allowed to change the license limits");

            if (_licenseLimitsSemaphore.Wait(1000 * 10) == false)
                throw new TimeoutException("License limit change is already in progress. " +
                                           "Please try again later.");

            try
            {
                var licenseLimits = _serverStore.LoadLicenseLimits() ?? new LicenseLimits();
                var detailsPerNode = licenseLimits.NodeLicenseDetails ??
                                     (licenseLimits.NodeLicenseDetails = new Dictionary<string, DetailsPerNode>());

                var oldAssignedCores = 0;
                var numberOfCores = -1;
                double installedMemoryInGb = -1;
                double usableMemoryInGb = -1;
                if (detailsPerNode.TryGetValue(nodeTag, out var nodeDetails))
                {
                    installedMemoryInGb = nodeDetails.InstalledMemoryInGb;
                    usableMemoryInGb = nodeDetails.UsableMemoryInGb;
                    oldAssignedCores = nodeDetails.UtilizedCores;
                }

                var utilizedCores = detailsPerNode.Sum(x => x.Value.UtilizedCores) - oldAssignedCores + newAssignedCores;
                var maxCores = _licenseStatus.MaxCores;
                if (utilizedCores > maxCores)
                {
                    return new LicenseLimit
                    {
                        Type = LimitType.Cores,
                        Message = $"Cannot change the license limit for node {nodeTag} " +
                                  $"from {oldAssignedCores} core{Pluralize(oldAssignedCores)} " +
                                  $"to {newAssignedCores} core{Pluralize(newAssignedCores)} " +
                                  $"because the utilized number of cores in the cluster will be {utilizedCores} " +
                                  $"while the maximum allowed cores according to the license is {maxCores}."
                    };
                }

                using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var allNodes = _serverStore.GetClusterTopology(context).AllNodes;
                    if (allNodes.TryGetValue(nodeTag, out var nodeUrl) == false)
                        throw new ArgumentException($"Node tag: {nodeTag} isn't part of the cluster");

                    var nodeInfo = await GetNodeInfo(nodeUrl, context);
                    if (nodeInfo != null)
                    {
                        numberOfCores = nodeInfo.NumberOfCores;
                        installedMemoryInGb = nodeInfo.InstalledMemoryInGb;
                        usableMemoryInGb = nodeInfo.UsableMemoryInGb;
                    }

                    if (numberOfCores != -1 && numberOfCores < newAssignedCores)
                        throw new ArgumentException($"The new assigned cores count: {newAssignedCores} " +
                                                    $"is larger than the number of cores in the node: {numberOfCores}");

                    detailsPerNode[nodeTag] = new DetailsPerNode
                    {
                        UtilizedCores = newAssignedCores,
                        NumberOfCores = numberOfCores,
                        InstalledMemoryInGb = installedMemoryInGb,
                        UsableMemoryInGb = usableMemoryInGb
                    };
                }

                await _serverStore.PutLicenseLimitsAsync(licenseLimits);
                return null;
            }
            finally
            {
                _licenseLimitsSemaphore.Release();
            }
        }

        public void CalculateLicenseLimits(
            string assignedNodeTag = null,
            int? assignedCores = null,
            NodeInfo nodeInfo = null,
            bool forceFetchingNodeInfo = false)
        {
            if (_serverStore.IsLeader() == false)
                return;

            if (_disableCalculatingLicenseLimits)
                return;

            if (_licenseLimitsSemaphore.Wait(0) == false)
                return;

            try
            {
                var licenseLimits = _serverStore.LoadLicenseLimits() ?? new LicenseLimits();
                var detailsPerNode = licenseLimits.NodeLicenseDetails ??
                    (licenseLimits.NodeLicenseDetails = new Dictionary<string, DetailsPerNode>());

                using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var hasChanges = false;
                    if (assignedNodeTag != null && assignedCores != null && nodeInfo != null)
                    {
                        hasChanges = true;
                        detailsPerNode[assignedNodeTag] = new DetailsPerNode
                        {
                            UtilizedCores = assignedCores.Value,
                            NumberOfCores = nodeInfo.NumberOfCores,
                            InstalledMemoryInGb = nodeInfo.InstalledMemoryInGb,
                            UsableMemoryInGb = nodeInfo.UsableMemoryInGb
                        };
                    }

                    var allNodes = _serverStore.GetClusterTopology(context).AllNodes;
                    var allNodeTags = allNodes.Keys;
                    if (allNodeTags.Count == detailsPerNode.Count &&
                        allNodeTags.All(detailsPerNode.Keys.Contains) &&
                        hasChanges == false)
                    {
                        return;
                    }

                    var nodesToRemove = detailsPerNode.Keys.Except(allNodeTags).ToList();
                    foreach (var nodeToRemove in nodesToRemove)
                    {
                        detailsPerNode.Remove(nodeToRemove);
                    }

                    var missingNodesAssignment = new List<string>();
                    foreach (var nodeTag in allNodeTags)
                    {
                        if (detailsPerNode.TryGetValue(nodeTag, out var _) == false)
                            missingNodesAssignment.Add(nodeTag);
                    }

                    if (missingNodesAssignment.Count > 0)
                    {
                        var utilizedCores = detailsPerNode.Sum(x => x.Value.UtilizedCores);
                        var availableCores = _licenseStatus.MaxCores - utilizedCores;
                        var coresPerNode = Math.Max(1, availableCores / missingNodesAssignment.Count);

                        foreach (var nodeTag in missingNodesAssignment)
                        {
                            int numberOfCores;
                            double installedMemory;
                            double usableMemoryInGb;
                            if (nodeTag == _serverStore.NodeTag)
                            {
                                numberOfCores = ProcessorInfo.ProcessorCount;
                                var memoryInfo = MemoryInformation.GetMemoryInfoInGb();
                                installedMemory = memoryInfo.InstalledMemory;
                                usableMemoryInGb = memoryInfo.UsableMemory;
                            }
                            else
                            {
                                if (nodeTag != assignedNodeTag)
                                    nodeInfo = null;

                                if (nodeInfo == null && forceFetchingNodeInfo)
                                {
                                    nodeInfo = AsyncHelpers.RunSync(() => GetNodeInfo(allNodes[nodeTag], context));
                                }

                                numberOfCores = nodeInfo?.NumberOfCores ?? -1;
                                installedMemory = nodeInfo?.InstalledMemoryInGb ?? -1;
                                usableMemoryInGb = nodeInfo?.UsableMemoryInGb ?? -1;
                            }

                            if (numberOfCores != -1)
                                coresPerNode = Math.Min(coresPerNode, numberOfCores);

                            detailsPerNode[nodeTag] = new DetailsPerNode
                            {
                                UtilizedCores = coresPerNode,
                                NumberOfCores = numberOfCores,
                                InstalledMemoryInGb = installedMemory,
                                UsableMemoryInGb = usableMemoryInGb
                            };
                        }
                    }

                    CheckLicenseStatus(detailsPerNode);
                }

                _serverStore.PutLicenseLimits(licenseLimits);
            }
            catch (Exception e)
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations("Failed to set license limits", e);
            }
            finally
            {
                _licenseLimitsSemaphore.Release();
            }
        }

        private void CheckLicenseStatus(Dictionary<string, DetailsPerNode> detailsPerNode)
        {
            var utilizedCores = detailsPerNode.Sum(x => x.Value.UtilizedCores);
            if (utilizedCores <= _licenseStatus.MaxCores)
                return;

            var details = $"The number of utilized cores is: {utilizedCores}," +
                          $"while the license limit is: {_licenseStatus.MaxCores} cores";
            GenerateLicenseLimit(LimitType.Cores, details, addNotification: true);

            _licenseStatus.Error = true;
            _licenseStatus.Message = details;
        }

        private int GetUtilizedCores()
        {
            var licenseLimits = _serverStore.LoadLicenseLimits();
            var detailsPerNode = licenseLimits?.NodeLicenseDetails;
            if (detailsPerNode == null)
                return 0;

            return detailsPerNode.Sum(x => x.Value.UtilizedCores);
        }

        public int GetCoresToAssign(int numberOfCores)
        {
            var utilizedCores = GetUtilizedCores();
            if (utilizedCores == 0)
                return Math.Min(numberOfCores, _licenseStatus.MaxCores);

            var availableCores = Math.Max(1, _licenseStatus.MaxCores - utilizedCores);
            return Math.Min(availableCores, numberOfCores);
        }

        private bool CanAssignCores(int? assignedCores, out string details)
        {
            details = null;

            if (assignedCores.HasValue == false)
                return true;

            var licenseLimits = _serverStore.LoadLicenseLimits();
            if (licenseLimits?.NodeLicenseDetails == null ||
                licenseLimits.NodeLicenseDetails.Count == 0)
                return true;

            var coresInUse = licenseLimits.NodeLicenseDetails.Sum(x => x.Value.UtilizedCores);
            if (coresInUse + assignedCores.Value > _licenseStatus.MaxCores)
            {
                details = $"Can't assign {assignedCores} core{Pluralize(assignedCores.Value)} " +
                          $"to the node, max allowed cores on license: {_licenseStatus.MaxCores}, " +
                          $"number of utilized cores: {coresInUse}";
                return false;
            }

            return true;
        }

        public async Task<LicenseLimit> Activate(License license, bool skipLeaseLicense, bool ensureNotPassive = true)
        {
            Dictionary<string, object> licenseAttributes;

            try
            {
                licenseAttributes = LicenseValidator.Validate(license, RSAParameters);
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

            var newLicenseStatus = new LicenseStatus
            {
                Attributes = licenseAttributes
            };

            var licenseExpiration = newLicenseStatus.Expiration;
            if (licenseExpiration.HasValue == false)
                throw new LicenseExpiredException("License doesn't have an expiration date!");

            if (licenseExpiration < DateTime.UtcNow)
            {
                if (skipLeaseLicense == false)
                {
                    // license expired, we'll try to update it
                    license = await GetUpdatedLicense(license);
                    if (license != null)
                    {
                        return await Activate(license, skipLeaseLicense: true, ensureNotPassive: ensureNotPassive);
                    }
                }

                throw new LicenseExpiredException($"License already expired on: {licenseExpiration}");
            }

            if (CanActivateLicense(newLicenseStatus, out var licenseLimit) == false)
                return licenseLimit;

            using (DisableCalculatingCoresCount())
            {
                if (ensureNotPassive)
                    _serverStore.EnsureNotPassive();

                try
                {

                    await _serverStore.PutLicenseAsync(license);

                    _licenseStatus.Attributes = licenseAttributes;
                    _licenseStatus.Error = false;
                    _licenseStatus.Message = null;
                    _licenseStatus.Id = license.Id;
                }
                catch (Exception e)
                {
                    var message = $"Could not save the following license:{Environment.NewLine}" +
                                  $"Id: {license.Id}{Environment.NewLine}" +
                                  $"Name: {license.Name}{Environment.NewLine}" +
                                  $"Keys: [{(license.Keys != null ? string.Join(", ", license.Keys) : "N/A")}]";

                    if (Logger.IsInfoEnabled)
                        Logger.Info(message, e);

                    throw new InvalidDataException("Could not save license!", e);
                }
            }

            CalculateLicenseLimits(forceFetchingNodeInfo: true);
            return null;
        }

        private IDisposable DisableCalculatingCoresCount()
        {
            _disableCalculatingLicenseLimits = true;
            return new DisposableAction(() =>
            {
                _disableCalculatingLicenseLimits = false;
            });
        }

        public void TryActivateLicense()
        {
            if (_licenseStatus.Type != LicenseType.None)
                return;

            if (TryGetLicenseFromString(out var license) == false)
                TryGetLicenseFromPath(out license);

            if (license == null)
                return;

            try
            {
                AsyncHelpers.RunSync(() => Activate(license, skipLeaseLicense: false, ensureNotPassive: false));
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Failed to activate license", e);
            }
        }

        private bool TryGetLicenseFromPath(out License license)
        {
            license = null;

            var path = _serverStore.Configuration.Licensing.LicensePath;

            try
            {
                using (var stream = File.Open(path.FullPath, FileMode.Open, FileAccess.Read))
                {
                    license = DeserializeLicense(stream);
                    return true;
                }
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failed to read license from '{path.FullPath}' path", e);
            }

            return false;
        }

        private bool TryGetLicenseFromString(out License license)
        {
            license = null;

            var licenseString = _serverStore.Configuration.Licensing.License;
            if (string.IsNullOrWhiteSpace(licenseString))
                return false;

            try
            {
                using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(licenseString)))
                {
                    license = DeserializeLicense(stream);
                    return true;
                }
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failed to read license from '{RavenConfiguration.GetKey(x => x.Licensing.License)}' configuration", e);
            }

            return false;
        }

        private static License DeserializeLicense(Stream stream)
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var json = context.Read(stream, "license/json");
                return JsonDeserializationServer.License(json);
            }
        }

        public async Task DeactivateLicense()
        {
            var license = _serverStore.LoadLicense();
            if (license == null)
                return;

            await _serverStore.DeactivateLicense(license);
        }

        private async Task<License> GetUpdatedLicense(License license)
        {
            var leaseLicenseInfo = new LeaseLicenseInfo
            {
                License = license,
                BuildInfo = _buildInfo,
                ClusterId = _serverStore.GetClusterTopology().TopologyId,
                UtilizedCores = GetUtilizedCores()
            };

            var response = await ApiHttpClient.Instance.PostAsync("/api/v2/license/lease",
                    new StringContent(JsonConvert.SerializeObject(leaseLicenseInfo), Encoding.UTF8, "application/json"))
                .ConfigureAwait(false);

            if (response.IsSuccessStatusCode == false)
            {
                await HandleLeaseLicenseFailure(response).ConfigureAwait(false);
                return null;
            }

            var leasedLicenseAsStream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var json = context.Read(leasedLicenseAsStream, "leased license info");
                var leasedLicense = JsonDeserializationServer.LeasedLicense(json);

                var newLicense = leasedLicense.License;
                var licenseChanged =
                    newLicense.Name != license.Name ||
                    newLicense.Id != license.Id ||
                    newLicense.Keys.All(license.Keys.Contains) == false;

                if (string.IsNullOrWhiteSpace(leasedLicense.Message) == false)
                {
                    var severity =
                        leasedLicense.NotificationSeverity == NotificationSeverity.None ?
                        NotificationSeverity.Info : leasedLicense.NotificationSeverity;
                    var alert = AlertRaised.Create(
                        leasedLicense.Title,
                        leasedLicense.Message,
                        AlertType.LicenseManager_LicenseUpdateMessage,
                        severity);

                    _serverStore.NotificationCenter.Add(alert);
                }

                return licenseChanged ? leasedLicense.License : null;
            }
        }

        private async Task ExecuteTasks()
        {
            try
            {
                await LeaseLicense();

                await UpdateNodesInfo();

                ReloadLicenseLimits();
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Failed to execute tasks", e);
            }
        }

        private async Task LeaseLicense()
        {
            if (_serverStore.IsLeader() == false)
                return;

            if (_leaseLicenseSemaphore.Wait(0) == false)
                return;

            try
            {
                var loadedLicense = _serverStore.LoadLicense();
                if (loadedLicense == null)
                    return;

                var updatedLicense = await GetUpdatedLicense(loadedLicense);
                if (updatedLicense == null)
                    return;

                await Activate(updatedLicense, skipLeaseLicense: true);

                var alert = AlertRaised.Create(
                    "License was updated",
                    "Successfully leased license",
                    AlertType.LicenseManager_LeaseLicenseSuccess,
                    NotificationSeverity.Info);

                _serverStore.NotificationCenter.Add(alert);
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Failed to lease license", e);

                var alert = AlertRaised.Create(
                    "Failed to lease license",
                    "Could not lease license",
                    AlertType.LicenseManager_LeaseLicenseError,
                    NotificationSeverity.Warning,
                    details: new ExceptionDetails(e));

                _serverStore.NotificationCenter.Add(alert);
            }
            finally
            {
                _leaseLicenseSemaphore.Release();
            }
        }

        private async Task UpdateNodesInfo()
        {
            if (_serverStore.IsLeader() == false)
                return;

            if (_licenseLimitsSemaphore.Wait(0) == false)
                return;

            try
            {
                using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var licenseLimits = _serverStore.LoadLicenseLimits();
                    if (licenseLimits?.NodeLicenseDetails == null)
                        return;

                    var allNodes = _serverStore.GetClusterTopology(context).AllNodes;
                    var hasChanges = false;
                    foreach (var nodeDetails in licenseLimits.NodeLicenseDetails)
                    {
                        var nodeTag = nodeDetails.Key;
                        if (allNodes.TryGetValue(nodeDetails.Key, out var url) == false)
                            continue;

                        int numberOfCores;
                        double installedMemory;
                        double usableMemoryInGb;
                        if (nodeTag == _serverStore.NodeTag)
                        {
                            numberOfCores = ProcessorInfo.ProcessorCount;
                            var memoryInfo = MemoryInformation.GetMemoryInfoInGb();
                            installedMemory = memoryInfo.InstalledMemory;
                            usableMemoryInGb = memoryInfo.UsableMemory;
                        }
                        else
                        {
                            var nodeInfo = await GetNodeInfo(url, context);
                            if (nodeInfo == null)
                                continue;

                            numberOfCores = nodeInfo.NumberOfCores;
                            installedMemory = nodeInfo.InstalledMemoryInGb;
                            usableMemoryInGb = nodeInfo.UsableMemoryInGb;
                        }

                        if (nodeDetails.Value.NumberOfCores == numberOfCores &&
                            nodeDetails.Value.UsableMemoryInGb.Equals(usableMemoryInGb) &&
                            nodeDetails.Value.InstalledMemoryInGb.Equals(installedMemory))
                            continue;

                        hasChanges = true;
                        nodeDetails.Value.NumberOfCores = numberOfCores;
                        nodeDetails.Value.InstalledMemoryInGb = installedMemory;
                        nodeDetails.Value.UsableMemoryInGb = usableMemoryInGb;
                    }

                    if (hasChanges)
                        await _serverStore.PutLicenseLimitsAsync(licenseLimits);
                }
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Error updating nodes info", e);
            }
            finally
            {
                _licenseLimitsSemaphore.Release();
            }
        }

        private async Task<NodeInfo> GetNodeInfo(string nodeUrl, TransactionOperationContext ctx)
        {
            using (var requestExecutor = ClusterRequestExecutor.CreateForSingleNode(nodeUrl, _serverStore.Server.ClusterCertificateHolder.Certificate))
            {
                var infoCmd = new GetNodeInfoCommand();

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

        private async Task HandleLeaseLicenseFailure(HttpResponseMessage response)
        {
            if (Logger.IsInfoEnabled == false || _skipLeasingErrorsLogging)
                return;

            var responseString = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

            var alert = AlertRaised.Create(
                "Lease license failure",
                "Could not lease license",
                AlertType.LicenseManager_LeaseLicenseError,
                NotificationSeverity.Warning,
                details: new ExceptionDetails(
                    new InvalidOperationException($"Status code: {response.StatusCode}, response: {responseString}")));

            _serverStore.NotificationCenter.Add(alert);
        }

        private void SetAffinity(Process process, int cores)
        {
            if (ProcessorInfo.ProcessorCount < cores)
                return;

            try
            {
                var bitMask = 1L;
                var processAffinityMask = _serverStore.Configuration.Server.ProcessAffinityMask;
                if (processAffinityMask == null)
                {
                    for (var i = 0; i < cores; i++)
                    {
                        bitMask |= 1L << i;
                    }
                }
                else if (NumberOfSetBits(processAffinityMask.Value) > cores)
                {
                    var affinityMask = processAffinityMask.Value;
                    var bitNumber = 0;
                    while (cores > 0)
                    {
                        if ((affinityMask & 1) != 0)
                        {
                            bitMask |= 1L << bitNumber;
                            cores--;
                        }
                        affinityMask = affinityMask >> 1;
                        bitNumber++;
                    }
                }
                else
                {
                    bitMask = processAffinityMask.Value;
                }

                process.ProcessorAffinity = new IntPtr(bitMask);
            }
            catch (Exception e)
            {
                Logger.Info($"Failed to set affinity for {cores} cores, error code: {Marshal.GetLastWin32Error()}", e);
            }
        }

        //https://stackoverflow.com/questions/2709430/count-number-of-bits-in-a-64-bit-long-big-integer
        private static long NumberOfSetBits(long i)
        {
            i = i - ((i >> 1) & 0x5555555555555555);
            i = (i & 0x3333333333333333) + ((i >> 2) & 0x3333333333333333);
            return (((i + (i >> 4)) & 0xF0F0F0F0F0F0F0F) * 0x101010101010101) >> 56;
        }

        [return: MarshalAs(UnmanagedType.Bool)]
        [DllImport("kernel32.dll", EntryPoint = "SetProcessWorkingSetSize", SetLastError = true, CallingConvention = CallingConvention.StdCall)]
        internal static extern bool SetProcessWorkingSetSizeEx(IntPtr pProcess,
            long dwMinimumWorkingSetSize, long dwMaximumWorkingSetSize, QuotaLimit flags);

        [Flags]
        internal enum QuotaLimit
        {
            QUOTA_LIMITS_HARDWS_MIN_DISABLE = 0x00000002,
            QUOTA_LIMITS_HARDWS_MIN_ENABLE = 0x00000001,
            QUOTA_LIMITS_HARDWS_MAX_DISABLE = 0x00000008,
            QUOTA_LIMITS_HARDWS_MAX_ENABLE = 0x00000004
        }

        private static void SetMaxWorkingSet(Process process, double ramInGb)
        {
            try
            {
                var memoryInfo = MemoryInformation.GetMemoryInfoInGb();
                if (memoryInfo.UsableMemory <= ramInGb)
                    return;

                var maxWorkingSetInBytes = (long)Size.ConvertToBytes(ramInGb, SizeUnit.Gigabytes);
                var minWorkingSetInBytes = process.MinWorkingSet.ToInt64();
                if (minWorkingSetInBytes > maxWorkingSetInBytes)
                {
                    minWorkingSetInBytes = maxWorkingSetInBytes;
                }

                if (PlatformDetails.RunningOnPosix == false)
                {
                    // windows
                    const QuotaLimit flags = QuotaLimit.QUOTA_LIMITS_HARDWS_MAX_ENABLE;
                    var result = SetProcessWorkingSetSizeEx(process.Handle, minWorkingSetInBytes, maxWorkingSetInBytes, flags);
                    if (result == false)
                    {
                        Logger.Info($"Failed to set max working set to {ramInGb}, error code: {Marshal.GetLastWin32Error()}");
                    }
                    return;
                }

                if (PlatformDetails.RunningOnMacOsx)
                {
                    // macOS
                    process.MinWorkingSet = new IntPtr(minWorkingSetInBytes);
                    process.MaxWorkingSet = new IntPtr(maxWorkingSetInBytes);
                    return;
                }

                const string groupName = "ravendb";
                var basePath = $"/sys/fs/cgroup/memory/{groupName}";
                var fd = Syscall.open(basePath, 0, 0);
                if (fd == -1)
                {
                    if (Syscall.mkdir(basePath, (ushort)FilePermissions.S_IRWXU) == -1)
                    {
                        Logger.Info($"Failed to create directory path: {basePath}, error code: {Marshal.GetLastWin32Error()}");
                        return;
                    }
                }

                Syscall.close(fd);

                var str = maxWorkingSetInBytes.ToString();
                if (WriteValue($"{basePath}/memory.limit_in_bytes", str) == false)
                    return;

                WriteValue($"{basePath}/cgroup.procs", str);
            }
            catch (Exception e)
            {
                Logger.Info($"Failed to set max working set to {ramInGb}GB, error code: {Marshal.GetLastWin32Error()}", e);
            }
        }

        private static unsafe bool WriteValue(string path, string str)
        {
            var fd = Syscall.open(path, OpenFlags.O_WRONLY, FilePermissions.S_IWUSR);
            if (fd == -1)
            {
                Logger.Info($"Failed to open path: {path}");
                return false;
            }

            fixed (char* x = str)
            {
                var length = str.Length;
                while (length > 0)
                {
                    var written = Syscall.write(fd, x, (ulong)length);
                    if (written <= 0)
                    {
                        // -1 or 0 is error when not regular file, 
                        // and this is a case of non-regular file
                        Logger.Info($"Failed to write to path: {path}, value: {str}");
                        Syscall.close(fd);
                        return false;
                    }
                    length -= (int)written;
                }

                if (Syscall.close(fd) == -1)
                {
                    Logger.Info($"Failed to close: {path}");
                    return false;
                }
            }

            return true;
        }

        public void Dispose()
        {
            _leaseLicenseTimer?.Dispose();
        }

        private bool CanActivateLicense(LicenseStatus newLicenseStatus, out LicenseLimit licenseLimit)
        {
            if (newLicenseStatus.Type < LicenseType.Trial &&
                (int)newLicenseStatus.Type < (int)_licenseStatus.Type)
            {
                var details = $"Cannot downgrade license from {_licenseStatus.Type.ToString()} " +
                              $"to {newLicenseStatus.Type.ToString()}!";
                licenseLimit = GenerateLicenseLimit(LimitType.Downgrade, details);
                return false;
            }

            var clusterSize = GetClusterSize();
            var maxClusterSize = newLicenseStatus.MaxClusterSize;
            if (clusterSize > maxClusterSize)
            {
                var details = "Cannot activate license because the maximum allowed cluster size is: " +
                              $"{maxClusterSize} while the current cluster size is: {clusterSize}";
                licenseLimit = GenerateLicenseLimit(LimitType.ClusterSize, details);
                return false;
            }

            var maxCores = newLicenseStatus.MaxCores;
            if (clusterSize > maxCores)
            {
                var details = "Cannot activate license because the cores limit is: " +
                              $"{maxCores} while the current cluster size is: {clusterSize}!";
                licenseLimit = GenerateLicenseLimit(LimitType.Cores, details);
                return false;
            }

            var encryptedDatabasesCount = 0;
            var externalReplicationCount = 0;
            var ravenEtlCount = 0;
            var sqlEtlCount = 0;
            var snapshotBackupsCount = 0;
            var cloudBackupsCount = 0;
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                foreach (var databaseRecord in _serverStore.Cluster.ReadAllDatabases(context))
                {
                    //TODO: check monitoring existance

                    if (databaseRecord.Encrypted)
                        encryptedDatabasesCount++;

                    if (databaseRecord.ExternalReplication != null &&
                        databaseRecord.ExternalReplication.Count > 0)
                        externalReplicationCount++;

                    if (HasRavenEtl(databaseRecord.RavenEtls,
                        databaseRecord.RavenConnectionStrings))
                        ravenEtlCount++;

                    if (databaseRecord.SqlEtls != null &&
                        databaseRecord.SqlEtls.Count > 0)
                        sqlEtlCount++;

                    var backupTypes = GetBackupTypes(databaseRecord.PeriodicBackups);
                    if (backupTypes.HasSnapshotBackup)
                        snapshotBackupsCount++;
                    if (backupTypes.HasCloudBackup)
                        cloudBackupsCount++;
                }
            }

            if (encryptedDatabasesCount > 0 &&
                newLicenseStatus.HasEncryption == false)
            {
                var details = GenerateDetails(encryptedDatabasesCount, "encryption");
                licenseLimit = GenerateLicenseLimit(LimitType.Encryption, details);
                return false;
            }

            if (externalReplicationCount > 0 &&
                newLicenseStatus.HasExternalReplication == false)
            {
                var details = GenerateDetails(externalReplicationCount, "external replication");
                licenseLimit = GenerateLicenseLimit(LimitType.ExternalReplication, details);
                return false;
            }

            if (ravenEtlCount > 0 &&
                newLicenseStatus.HasRavenEtl == false)
            {
                var details = GenerateDetails(ravenEtlCount, "Raven ETL");
                licenseLimit = GenerateLicenseLimit(LimitType.RavenEtl, details);
                return false;
            }

            if (sqlEtlCount > 0 &&
                newLicenseStatus.HasSqlEtl == false)
            {
                var details = GenerateDetails(sqlEtlCount, "SQL ETL");
                licenseLimit = GenerateLicenseLimit(LimitType.SqlEtl, details);
                return false;
            }

            if (snapshotBackupsCount > 0 &&
                newLicenseStatus.HasSnapshotBackups == false)
            {
                var details = GenerateDetails(cloudBackupsCount, "snapshot backups");
                licenseLimit = GenerateLicenseLimit(LimitType.SnapshotBackup, details);
                return false;
            }

            if (cloudBackupsCount > 0 &&
                newLicenseStatus.HasCloudBackups == false)
            {
                var details = GenerateDetails(cloudBackupsCount, "cloud backups");
                licenseLimit = GenerateLicenseLimit(LimitType.CloudBackup, details);
                return false;
            }

            licenseLimit = null;
            return true;
        }

        private static string GenerateDetails(int count, string feature)
        {
            return $"Cannot activate license because there {Pluralize(count)} " +
                   $"{count} database{(count == 1 ? string.Empty : "s")} that use {feature} " +
                   $"while the current license doesn't include the usage of {feature}";
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

        private static (bool HasSnapshotBackup, bool HasCloudBackup) GetBackupTypes(
            IEnumerable<PeriodicBackupConfiguration> periodicBackups)
        {
            var hasSnapshotBackup = false;
            var hasCloudBackup = false;
            foreach (var configuration in periodicBackups)
            {
                hasSnapshotBackup |= configuration.BackupType == BackupType.Snapshot;
                hasCloudBackup |= configuration.HasCloudBackup();

                if (hasSnapshotBackup && hasCloudBackup)
                    return (true, true);
            }

            return (hasSnapshotBackup, hasCloudBackup);
        }

        public bool CanAddNode(string nodeUrl, int? assignedCores, out LicenseLimit licenseLimit)
        {
            if (IsValid(out licenseLimit) == false)
                return false;

            if (CanAssignCores(assignedCores, out var coresDetails) == false)
            {
                licenseLimit = GenerateLicenseLimit(LimitType.Cores, coresDetails);
                return false;
            }

            if (_licenseStatus.DistributedCluster == false)
            {
                var details = $"Your current license ({_licenseStatus.Type}) does not allow adding nodes to the cluster";
                licenseLimit = GenerateLicenseLimit(LimitType.ForbiddenHost, details);
                return false;
            }

            var maxClusterSize = _licenseStatus.MaxClusterSize;
            var clusterSize = GetClusterSize();
            if (++clusterSize > maxClusterSize)
            {
                var details = $"Your current license allows up to {maxClusterSize} nodes in a cluster";
                licenseLimit = GenerateLicenseLimit(LimitType.ClusterSize, details);
                return false;
            }

            licenseLimit = null;
            return true;
        }

        public bool CanAddPeriodicBackup(BlittableJsonReaderObject readerObject, out LicenseLimit licenseLimit)
        {
            if (IsValid(out licenseLimit) == false)
                return false;

            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var backupBlittable = readerObject.Clone(context);
                var configuration = JsonDeserializationCluster.PeriodicBackupConfiguration(backupBlittable);
                if (configuration.BackupType == BackupType.Snapshot &&
                    _licenseStatus.HasSnapshotBackups == false)
                {
                    const string details = "Your current license doesn't include the snapshot backups feature";
                    licenseLimit = GenerateLicenseLimit(LimitType.SnapshotBackup, details);
                    return false;
                }

                var hasCloudBackup = configuration.HasCloudBackup();
                if (hasCloudBackup && _licenseStatus.HasCloudBackups == false)
                {
                    const string details = "Your current license doesn't include the backup to cloud or ftp feature!";
                    licenseLimit = GenerateLicenseLimit(LimitType.CloudBackup, details);
                    return false;
                }

                licenseLimit = null;
                return true;
            }
        }

        public bool CanAddExternalReplication(out LicenseLimit licenseLimit)
        {
            if (IsValid(out licenseLimit) == false)
                return false;

            if (_licenseStatus.HasExternalReplication == false)
            {
                var details = $"Your current license ({_licenseStatus.Type}) does not allow adding external replication";
                licenseLimit = GenerateLicenseLimit(LimitType.ExternalReplication, details);
                return false;
            }

            licenseLimit = null;
            return true;
        }

        public bool CanAddRavenEtl(out LicenseLimit licenseLimit)
        {
            if (IsValid(out licenseLimit) == false)
                return false;

            if (_licenseStatus.HasRavenEtl == false)
            {
                const string details = "Your current license doesn't include the RavenDB ETL feature";
                licenseLimit = GenerateLicenseLimit(LimitType.RavenEtl, details);
                return false;
            }

            licenseLimit = null;
            return true;
        }

        public bool CanAddSqlEtl(out LicenseLimit licenseLimit)
        {
            if (IsValid(out licenseLimit) == false)
                return false;

            if (_licenseStatus.HasSqlEtl == false)
            {
                const string details = "Your current license doesn't include the SQL ETL feature";
                licenseLimit = GenerateLicenseLimit(LimitType.SqlEtl, details);
                return false;
            }

            licenseLimit = null;
            return true;
        }

        public bool CanDynamicallyDistributeNodes()
        {
            if (IsValid(out var _) == false)
                return false;

            if (_licenseStatus.HasDynamicNodesDistribution)
                return true;

            const string details = "Your current license doesn't include " +
                                   "the dynamic nodes distribution feature";
            GenerateLicenseLimit(LimitType.DynamicNodeDistribution, details, addNotification: true);
            return false;
        }

        public bool CanCreateEncryptedDatabase(out LicenseLimit licenseLimit)
        {
            if (IsValid(out licenseLimit) == false)
                return false;

            if (_licenseStatus.HasEncryption == false)
            {
                const string details = "Your current license doesn't include the encryption feature";
                licenseLimit = GenerateLicenseLimit(LimitType.Encryption, details);
                return false;
            }

            licenseLimit = null;
            return true;
        }

        private LicenseLimit GenerateLicenseLimit(
            LimitType limitType,
            string message,
            bool addNotification = false)
        {
            var licenseLimit = new LicenseLimit
            {
                Type = limitType,
                Message = message
            };

            if (addNotification)
                LicenseLimitWarning.AddLicenseLimitNotification(_serverStore, licenseLimit);

            return licenseLimit;
        }

        private bool IsValid(out LicenseLimit licenseLimit)
        {
            if (_licenseStatus.Type == LicenseType.Invalid)
            {
                const string details = "Cannot perform operation while the license is in invalid state!";
                licenseLimit = GenerateLicenseLimit(LimitType.InvalidLicense, details);
                return false;
            }

            licenseLimit = null;
            return true;
        }
    }
}
