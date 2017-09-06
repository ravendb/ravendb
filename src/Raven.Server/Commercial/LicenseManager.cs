using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.ETL;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.PeriodicBackup;
using Raven.Client.Util;
using Raven.Server.Config.Settings;
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
        private Timer _updateNodesInfoTimer;
        private RSAParameters? _rsaParameters;
        private readonly ServerStore _serverStore;
        private readonly SemaphoreSlim _leaseLicenseSemaphore = new SemaphoreSlim(1);
        private readonly SemaphoreSlim _licenseLimitsSemaphore = new SemaphoreSlim(1);
        private readonly bool _skipLoggingLeaseLicenseErrors;

        public LicenseManager(ServerStore serverStore)
        {
            _serverStore = serverStore;
            _skipLoggingLeaseLicenseErrors = serverStore.Configuration.Licensing.SkipLoggingErrors;

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
                    AsyncHelpers.RunSync(LeaseLicense), null,
                    (int)TimeSpan.FromMinutes(1).TotalMilliseconds,
                    (int)TimeSpan.FromHours(24).TotalMilliseconds);

                _updateNodesInfoTimer = new Timer(state =>
                    AsyncHelpers.RunSync(UpdateNodesInfo), null,
                    (int)TimeSpan.FromSeconds(30).TotalMilliseconds,
                    (int)TimeSpan.FromHours(24).TotalMilliseconds);
            }
        }

        public LicenseStatus GetLicenseStatus()
        {
            return _licenseStatus;
        }

        private void ReloadLicense()
        {
            var licnese = _serverStore.LoadLicense();
            if (licnese == null)
            {
                // license is not active
                _licenseStatus.Attributes = null;
                _licenseStatus.Error = false;
                _licenseStatus.Message = null;
                return;
            }

            try
            {
                _licenseStatus.Attributes = LicenseValidator.Validate(licnese, RSAParameters);
                _licenseStatus.Error = false;
                _licenseStatus.Message = null;
            }
            catch (Exception e)
            {
                _licenseStatus.Attributes = null;
                _licenseStatus.Error = true;
                _licenseStatus.Message = e.Message;

                if (Logger.IsInfoEnabled && _skipLoggingLeaseLicenseErrors == false)
                    Logger.Info("Could not validate license", e);

                var alert = AlertRaised.Create(
                    "License manager initialization error",
                    "Could not intitalize the license manager",
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

            RecalculateLicenseLimits();
        }

        private int GetClusterSize()
        {
            if (_serverStore.IsPassive())
                return 1;

            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                return _serverStore.GetClusterTopology(context).AllNodes.Count;
            }
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
                double usableMemoryInGb = -1;
                double installedMemoryInGb = -1;
                if (detailsPerNode.TryGetValue(nodeTag, out var nodeDetails))
                {
                    installedMemoryInGb = nodeDetails.InstalledMemoryInGb;
                    oldAssignedCores = nodeDetails.UtilizedCores;
                }

                var utilizedCores = detailsPerNode.Sum(x => x.Value.UtilizedCores) - oldAssignedCores + newAssignedCores;
                var maxCores = _licenseStatus.MaxCores;
                if (utilizedCores > maxCores)
                {
                    return new LicenseLimit
                    {
                        Type = LimitType.Cores,
                        Details = $"Cannot change the license limit for node: {nodeTag} " +
                                  $"from: {oldAssignedCores} core{Pluralize(oldAssignedCores)} " +
                                  $"to {newAssignedCores} core{Pluralize(newAssignedCores)}" +
                                  $"because the utilized number of cores in the cluster will be: {utilizedCores} " +
                                  $"while the maximum allowed cores acoording to the license is: {maxCores}."
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

                    if (numberOfCores < newAssignedCores)
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

        private static string Pluralize(int count)
        {
            return count == 1 ? string.Empty : "s";
        }

        public void RecalculateLicenseLimits(
            string assignedNodeTag = null,
            int? assignedCores = null,
            NodeInfo nodeInfo = null)
        {
            if (_serverStore.IsLeader() == false)
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
                    if (assignedNodeTag != null && assignedCores != null && nodeInfo != null)
                    {
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
                    var utilizedCores = detailsPerNode.Sum(x => x.Value.UtilizedCores);
                    if (allNodeTags.Count == detailsPerNode.Count &&
                        allNodeTags.SequenceEqual(detailsPerNode.Keys) &&
                        utilizedCores <= _licenseStatus.MaxCores)
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
                        var availableCores = _licenseStatus.MaxCores - utilizedCores;
                        var coresPerNode = Math.Max(1, availableCores / missingNodesAssignment.Count);

                        foreach (var nodeTag in missingNodesAssignment)
                        {
                            var numberOfCores = -1;
                            double installedMemory = -1;
                            double usableMemoryInGb = -1;
                            if (nodeTag == _serverStore.NodeTag)
                            {
                                numberOfCores = ProcessorInfo.ProcessorCount;
                                var memoryInfo = MemoryInformation.GetMemoryInfoInGb();
                                installedMemory = memoryInfo.InstalledMemory;
                                usableMemoryInGb = memoryInfo.UsableMemory;
                            }
                            else if (nodeTag == assignedNodeTag && nodeInfo != null)
                            {
                                numberOfCores = nodeInfo.NumberOfCores;
                                installedMemory = nodeInfo.InstalledMemoryInGb;
                                usableMemoryInGb = nodeInfo.UsableMemoryInGb;
                            }
                                
                            detailsPerNode[nodeTag] = new DetailsPerNode
                            {
                                UtilizedCores = coresPerNode,
                                NumberOfCores = numberOfCores,
                                InstalledMemoryInGb = installedMemory,
                                UsableMemoryInGb = usableMemoryInGb
                            };
                        }
                    }

                    utilizedCores = detailsPerNode.Sum(x => x.Value.UtilizedCores);
                    if (utilizedCores > _licenseStatus.MaxCores)
                    {
                        // need to redistribute the number of cores
                        var coresPerNode = Math.Max(1, _licenseStatus.MaxCores / allNodeTags.Count);
                        foreach (var nodeTag in allNodeTags)
                        {
                            detailsPerNode[nodeTag] = new DetailsPerNode
                            {
                                UtilizedCores = coresPerNode,
                                NumberOfCores = detailsPerNode[nodeTag].NumberOfCores,
                                InstalledMemoryInGb = detailsPerNode[nodeTag].InstalledMemoryInGb,
                                UsableMemoryInGb = detailsPerNode[nodeTag].UsableMemoryInGb
                            };
                        }
                    }
                }

                _serverStore.PutLicenseLimits(licenseLimits);
            }
            catch (Exception e)
            {
                if(Logger.IsOperationsEnabled)
                    Logger.Operations("Failed to set license limits", e);
            }
            finally
            {
                _licenseLimitsSemaphore.Release();
            }
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

        public async Task<LicenseLimit> Activate(License license, bool skipLeaseLicense)
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
                throw new LicenseExpiredException("License doesn't have an expirtaion date!");

            if (licenseExpiration < DateTime.UtcNow)
            {
                if (skipLeaseLicense == false)
                {
                    // license expired, we'll try to update it
                    license = await GetUpdatedLicense(license);
                    if (license != null)
                    {
                        return await Activate(license, skipLeaseLicense: true);
                    }
                }

                throw new LicenseExpiredException($"License already expired on: {licenseExpiration}");
            }

            if (CanActivateLicense(newLicenseStatus, out var licenseLimit) == false)
                return licenseLimit;

            _serverStore.EnsureNotPassive(skipActivateLicense: true);

            try
            {
                await _serverStore.PutLicenseAsync(license);

                _licenseStatus.Attributes = licenseAttributes;
                _licenseStatus.Error = false;
                _licenseStatus.Message = null;

                return null;
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

        public void TryActivateLicense()
        {
            if (_licenseStatus.Type != LicenseType.None)
                return;

            var licensePath = _serverStore.Configuration.Licensing.LicensePath;
            var license = GetLicenseFromPath(licensePath);
            if (license == null)
                return;

            AsyncHelpers.RunSync(() => Activate(license, skipLeaseLicense: false));
        }

        private static License GetLicenseFromPath(string licensePath)
        {
            try
            {
                var licenseFullPath = new PathSetting(licensePath).ToFullPath();
                using (var context = JsonOperationContext.ShortTermSingleUse())
                using (var fileStream = File.Open(licenseFullPath, FileMode.Open, FileAccess.Read))
                {
                    var json = context.Read(fileStream, "license activation from license path");
                    return JsonDeserializationServer.License(json);
                }
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failed to get license from path: {licensePath}", e);

                return null;
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
                BuildInfo = _buildInfo
            };

            var response = await ApiHttpClient.Instance.PostAsync("/api/v2/license/lease",
                    new StringContent(JsonConvert.SerializeObject(leaseLicenseInfo), Encoding.UTF8, "application/json"))
                .ConfigureAwait(false);

            if (response.IsSuccessStatusCode == false)
            {
                await HandleLeaseLicenseFailure(response).ConfigureAwait(false);
                return null;
            }

            var licenseAsStream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var json = context.Read(licenseAsStream, "leased license info");
                var leasedLicense = JsonDeserializationServer.LeasedLicense(json);

                var newLicense = leasedLicense.License;
                var licenseChanged =
                    newLicense.Name != license.Name ||
                    newLicense.Id != license.Id ||
                    newLicense.Keys.SequenceEqual(license.Keys) == false;

                var hasMessage = string.IsNullOrWhiteSpace(leasedLicense.Message) == false;
                if (hasMessage || licenseChanged)
                {
                    var severity =
                        leasedLicense.NotificationSeverity == NotificationSeverity.None ? 
                        NotificationSeverity.Info : leasedLicense.NotificationSeverity;
                    var alert = AlertRaised.Create(
                        "License was updated",
                        hasMessage ? leasedLicense.Message : "license was updated",
                        AlertType.LicenseManager_LicenseUpdateMessage,
                        severity);

                    _serverStore.NotificationCenter.Add(alert);
                }

                return licenseChanged ? leasedLicense.License : null;
            }
        }

        private async Task LeaseLicense()
        {
            if (_serverStore.IsLeader() == false)
            {
                // only the leader is in charge of updating the license
                return;
            }

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
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Error leasing license", e);

                var alert = AlertRaised.Create(
                    "Error leasing license",
                    "Could not lease license",
                    AlertType.LicenseManager_LeaseLicenseError,
                    NotificationSeverity.Warning,
                    details: new ExceptionDetails(e));

                _serverStore.NotificationCenter.Add(alert);
            }
            finally
            {
                _leaseLicenseSemaphore.Release();
                ReloadLicenseLimits();
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
            using (var requestExecutor = ClusterRequestExecutor.CreateForSingleNode(nodeUrl, _serverStore.RavenServer.ClusterCertificateHolder.Certificate))
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
            var responseString = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
            if (response.StatusCode == HttpStatusCode.ExpectationFailed)
            {
                // TODO: handle lease canceled on the server
                // the license was canceled
                _licenseStatus.Attributes = null;
                _licenseStatus.Error = true;
                _licenseStatus.Message = responseString;
            }

            var alert = AlertRaised.Create(
                "Lease license failure",
                "Could not lease license",
                AlertType.LicenseManager_LeaseLicenseError,
                NotificationSeverity.Warning,
                details: new ExceptionDetails(
                    new InvalidOperationException($"Status code: {response.StatusCode}, response: {responseString}")));

            _serverStore.NotificationCenter.Add(alert);
        }

        private static void SetAffinity(Process process, int cores)
        {
            if (Environment.ProcessorCount < cores)
                return;

            try
            {
                var bitMask = 1;
                for (var i = 1; i <= cores; i++)
                {
                    bitMask |= 1 << (i - 1);
                }

                process.ProcessorAffinity = new IntPtr(bitMask);
            }
            catch (Exception e)
            {
                Logger.Info($"Failed to set affinity for {cores} cores, error code: {Marshal.GetLastWin32Error()}", e);
            }
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
            _updateNodesInfoTimer?.Dispose();
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
            var maxClusterSize = _licenseStatus.MaxClusterSize;
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

                    if (HasValidExternalReplication(newLicenseStatus,
                        databaseRecord.ExternalReplication))
                        externalReplicationCount++;

                    if (HasInvalidRavenEtl(newLicenseStatus, databaseRecord.RavenEtls,
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
                var details = "Cannot activate license because there are " +
                              $"{encryptedDatabasesCount} encrypted database{Pluralize(encryptedDatabasesCount)}: " +
                              "while the current license doesn't include the usage of encryption";
                licenseLimit = GenerateLicenseLimit(LimitType.Encryption, details);
                return false;
            }

            if (externalReplicationCount > 0 &&
                newLicenseStatus.HasExternalReplication == false)
            {
                var details = "Cannot activate license because there are " +
                              $"{externalReplicationCount} database{Pluralize(externalReplicationCount)} " +
                              "that use external replication " +
                              "while the current license doesn't include the usage of external replication";
                licenseLimit = GenerateLicenseLimit(LimitType.ExternalReplication, details);
                return false;
            }

            if (ravenEtlCount > 0 &&
                newLicenseStatus.HasRavenEtl == false)
            {
                var details = "Cannot activate license because there are " +
                              $"{ravenEtlCount} database{Pluralize(ravenEtlCount)} that use Raven ETL " +
                              "while the current license doesn't include the usage of Raven ETL!";
                licenseLimit = GenerateLicenseLimit(LimitType.RavenEtl, details);
                return false;
            }

            if (sqlEtlCount > 0 &&
                newLicenseStatus.HasSqlEtl == false)
            {
                var details = "Cannot activate license because there are " +
                              $"{sqlEtlCount} database{Pluralize(sqlEtlCount)} that use SQL ETL " +
                              "while the current license doesn't include the usage of SQL ETL";
                licenseLimit = GenerateLicenseLimit(LimitType.SqlEtl, details);
                return false;
            }

            if (snapshotBackupsCount > 0 &&
                newLicenseStatus.HasSnapshotBackups == false)
            {
                var details = "Cannot activate license because there are " +
                              $"{snapshotBackupsCount} database{Pluralize(snapshotBackupsCount)} that use snapshot bakcups " +
                              "while the current license doesn't include the usage of snapshot backups";
                licenseLimit = GenerateLicenseLimit(LimitType.SnapshotBackup, details);
                return false;
            }

            if (cloudBackupsCount > 0 &&
                newLicenseStatus.HasCloudBackups == false)
            {
                var details = "Cannot activate license because there are " +
                              $"{cloudBackupsCount} database{Pluralize(cloudBackupsCount)} that use cloud backups " +
                              "while the current license doesn't include the usage of cloud backups";
                licenseLimit = GenerateLicenseLimit(LimitType.CloudBackup, details);
                return false;
            }

            licenseLimit = null;
            return true;
        }

        private static bool HasValidExternalReplication(
            LicenseStatus licenseStatus,
            List<ExternalReplication> externalReplications)
        {
            if (externalReplications == null)
                return false;

            var hasInvalidExternalReplication = false;

            foreach (var externalReplication in externalReplications)
            {
                hasInvalidExternalReplication |= 
                    IsValidExternalReplication(licenseStatus, externalReplication.Url) == false;
            }

            return hasInvalidExternalReplication;
        }

        private static bool HasInvalidRavenEtl(
            LicenseStatus licenseStatus,
            List<RavenEtlConfiguration> ravenEtls,
            Dictionary<string, RavenConnectionString> ravenConnectionStrings)
        {
            if (ravenEtls == null)
                return false;

            var hasInvalidRavenEtl = false;

            foreach (var ravenEtl in ravenEtls)
            {
                if (ravenConnectionStrings.TryGetValue(ravenEtl.ConnectionStringName, out var value) == false)
                    continue;

                hasInvalidRavenEtl |= IsValidRavenEtl(licenseStatus, value.Url) == false;
            }

            return hasInvalidRavenEtl;
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

            if (_licenseStatus.DistributedCluster == false &&
                IsValidLocalUrl(nodeUrl) == false)
            {
                var details = $"Your current license ({_licenseStatus.Type}) allows adding nodes " +
                        "that run locally (localhost, 127.*.*.* or [::1])";
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
                    const string details = "Your current license doesn't include the backup to cloud feature!";
                    //TODO
                    licenseLimit = GenerateLicenseLimit(LimitType.CloudBackup, details);
                    return false;
                }

                licenseLimit = null;
                return true;
            } 
        }

        public bool CanAddExternalReplication(ExternalReplication watcher, out LicenseLimit licenseLimit)
        {
            if (IsValid(out licenseLimit) == false)
                return false;

            if (IsValidExternalReplication(_licenseStatus, watcher.Url) == false)
            {
                var details = $"Your current license ({_licenseStatus.Type}) allows adding external replication " +
                               "destinations that run locally (localhost, 127.*.*.* or [::1])";
                licenseLimit = GenerateLicenseLimit(LimitType.ExternalReplication, details);
                return false;
            }

            licenseLimit = null;
            return true;
        }

        private static bool IsValidExternalReplication(LicenseStatus licenseStatus, string url)
        {
            return licenseStatus.HasExternalReplication || IsValidLocalUrl(url);
        }

        public bool CanAddRavenEtl(string url, out LicenseLimit licenseLimit)
        {
            if (IsValid(out licenseLimit) == false)
                return false;

            if (IsValidRavenEtl(_licenseStatus, url) == false)
            {
                var details = $"Your current license ({_licenseStatus.Type}) allows adding Raven ETL " +
                              "destinations that run locally (localhost, 127.*.*.* or [::1])";
                licenseLimit = GenerateLicenseLimit(LimitType.RavenEtl, details);
                return false;
            }

            licenseLimit = null;
            return true;
        }

        private static bool IsValidRavenEtl(LicenseStatus licenseStatus, string url)
        {
            return licenseStatus.HasRavenEtl || IsValidLocalUrl(url);
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
            string details,
            bool addNotification = false)
        {
            var licenseLimit = new LicenseLimit
            {
                Type = limitType,
                Details = details
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

        private static bool IsValidLocalUrl(string url)
        {
            if (string.IsNullOrWhiteSpace(url))
                return false;

            var uri = new Uri(url);
            if (IPAddress.TryParse(uri.Host, out var ipAddress) == false)
            {
                return uri.Host == "localhost" || uri.Host == "localhost.fiddler";
            }

            switch (ipAddress.AddressFamily)
            {
                case AddressFamily.InterNetwork:
                    // ipv4
                    if (uri.Host.StartsWith("127"))
                        return true;
                    break;
                case AddressFamily.InterNetworkV6:
                    if (uri.Host == "[::1]")
                        return true;
                    // ipv6
                    break;
            }

            return false;
        }
    }
}
