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
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.ETL;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.PeriodicBackup;
using Raven.Client.Util;
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
        private RSAParameters? _rsaParameters;
        private readonly ServerStore _serverStore;
        private readonly SemaphoreSlim _leaseLicenseSemaphore = new SemaphoreSlim(1);
        private readonly SemaphoreSlim _recalculateLicenseLimitsSemsphore = new SemaphoreSlim(1);

        public LicenseManager(ServerStore serverStore)
        {
            _serverStore = serverStore;
            
            _buildInfo = new BuildNumber
            {
                BuildVersion = ServerVersion.Build,
                ProductVersion = ServerVersion.Version,
                CommitHash = ServerVersion.CommitHash,
                FullVersion = ServerVersion.FullVersion
            };

            _serverStore.LicenseChanged += async(_, e) =>
            {
                ReloadLicense();
                await ReloadLicenseLimits();
            };
            _serverStore.LicenseLimitsChanged += async(_, e) => await ReloadLicenseLimits();
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
                Task.Run(async() => await ReloadLicenseLimits());
            }
            finally
            {
                _leaseLicenseTimer = new Timer(state =>
                    AsyncHelpers.RunSync(LeaseLicense), null, 0, (int)TimeSpan.FromHours(24).TotalMilliseconds);
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

                if (Logger.IsInfoEnabled)
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

        public async Task ReloadLicenseLimits()
        {
            try
            {
                var licenseLimits = _serverStore.LoadLicenseLimits();
                if (licenseLimits?.CoresByNode != null && 
                    licenseLimits.CoresByNode.TryGetValue(_serverStore.NodeTag, out var cores))
                {
                    cores = Math.Min(cores, _licenseStatus.MaxCores);

                    var process = Process.GetCurrentProcess();
                    SetAffinity(process, cores);

                    var ratio = (int)Math.Ceiling(_licenseStatus.MaxRamInGb / (double)_licenseStatus.MaxCores);
                    var clusterSize = GetClusterSize();
                    var maxWorkingSet = Math.Min(_licenseStatus.MaxRamInGb / (double)clusterSize, cores * ratio);
                    SetMaxWorkingSet(process, Math.Max(1, maxWorkingSet));
                }
            }
            catch (Exception e)
            {
                Logger.Info("Failed to reload license limits", e);
            }

            await RecalculateLicenseLimitsIfNeeded();
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

        public async Task RecalculateLicenseLimitsIfNeeded(
            string assignedNodeTag = null, int? assignedCores = null)
        {
            if (_serverStore.IsLeader() == false)
                return;

            try
            {
                _recalculateLicenseLimitsSemsphore.Wait();

                var licenseLimits = _serverStore.LoadLicenseLimits() ?? new LicenseLimits();
                var coresByNode = licenseLimits.CoresByNode ?? 
                    (licenseLimits.CoresByNode = new Dictionary<string, int>());

                using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    if (assignedNodeTag != null && assignedCores != null)
                    {
                        coresByNode[assignedNodeTag] = assignedCores.Value;
                    }

                    var allNodeTags = _serverStore.GetClusterTopology(context).AllNodes.Keys;
                    var coresInUse = coresByNode.Sum(x => x.Value);
                    if (allNodeTags.Count == coresByNode.Count &&
                        coresInUse <= _licenseStatus.MaxCores)
                    {
                        return;
                    }

                    var nodesToRemove = coresByNode.Keys.Except(allNodeTags).ToList();
                    foreach (var nodeToRemove in nodesToRemove)
                    {
                        coresByNode.Remove(nodeToRemove);
                    }

                    var missingNodesAssignment = new List<string>();
                    foreach (var nodeTag in allNodeTags)
                    {
                        if (coresByNode.TryGetValue(nodeTag, out var _) == false)
                            missingNodesAssignment.Add(nodeTag);
                    }

                    if (missingNodesAssignment.Count > 0)
                    {
                        var availableCores = _licenseStatus.MaxCores - coresInUse;
                        var coresPerNode = Math.Max(1, availableCores / missingNodesAssignment.Count);
                        foreach (var nodeTag in missingNodesAssignment)
                        {
                            coresByNode[nodeTag] = coresPerNode;
                        }
                    }

                    coresInUse = coresByNode.Sum(x => x.Value);
                    if (coresInUse > _licenseStatus.MaxCores)
                    {
                        // need to redistribute the number of cores
                        var coresPerNode = Math.Max(1, _licenseStatus.MaxCores / allNodeTags.Count);
                        foreach (var nodeTag in allNodeTags)
                        {
                            coresByNode[nodeTag] = coresPerNode;
                        }
                    }
                }

                await _serverStore.PutLicenseLimits(licenseLimits);
            }
            catch (Exception e)
            {
                Logger.Info("Failed to set license limits", e);
            }
            finally
            {
                _recalculateLicenseLimitsSemsphore.Release();
            }
        }

        public void VerifyNewAssignedCoresValue(int? newAssignedCores)
        {
            if (newAssignedCores.HasValue == false)
                return;

            var licenseLimits = _serverStore.LoadLicenseLimits();
            if (licenseLimits?.CoresByNode == null || 
                licenseLimits.CoresByNode.Count == 0)
                return;

            var coresInUse = licenseLimits.CoresByNode.Sum(x => x.Value);
            if (coresInUse + newAssignedCores.Value > _licenseStatus.MaxCores)
            {
                throw new ArgumentException($"Can't assign {newAssignedCores} core(s) to the node, " +
                                            $"max allowed cores: {_licenseStatus.MaxCores}, " +
                                            $"number of in use cores: {coresInUse}");
            }
        }

        public async Task RegisterForFreeLicense(UserRegistrationInfo userInfo)
        {
            userInfo.BuildInfo = _buildInfo;

            var response = await ApiHttpClient.Instance.PostAsync("api/v1/license/register",
                    new StringContent(JsonConvert.SerializeObject(userInfo), Encoding.UTF8, "application/json"))
                .ConfigureAwait(false);
            
            var responseString = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
            if (response.IsSuccessStatusCode == false)
            {
                throw new InvalidOperationException("Registration failed with status code: " + response.StatusCode +
                                                    Environment.NewLine + responseString);
            }
        }

        public async Task Activate(License license, bool skipLeaseLicense)
        {
            var licenseAttributes = LicenseValidator.Validate(license, RSAParameters);
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
                        await Activate(license, skipLeaseLicense: true);
                        return;
                    }
                }

                throw new LicenseExpiredException($"License already expired on: {licenseExpiration}");
            }

            if (CanActivateLicense(newLicenseStatus, out var licenseLimit) == false)
            {
                //TODO: use the license limit
                return;
            }

            _serverStore.EnsureNotPassive();

            try
            {
                await _serverStore.PutLicense(license);

                _licenseStatus.Attributes = licenseAttributes;
                _licenseStatus.Error = false;
                _licenseStatus.Message = null;
            }
            catch (Exception e)
            {
                _licenseStatus.Attributes = null;
                _licenseStatus.Error = true;
                _licenseStatus.Message = e.Message;

                var message = $"Could not validate the following license:{Environment.NewLine}" +
                              $"Id: {license.Id}{Environment.NewLine}" +
                              $"Name: {license.Name}{Environment.NewLine}" +
                              $"Keys: [{(license.Keys != null ? string.Join(", ", license.Keys) : "N/A")}]";

                if (Logger.IsInfoEnabled)
                    Logger.Info(message, e);

                throw new InvalidDataException("Could not validate license!", e);
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
                await ReloadLicenseLimits();
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

        private static void SetMaxWorkingSet(Process process, double ramInGB)
        {
            try
            {
                var memoryinformation = MemoryInformation.GetMemoryInfo();
                var totalMemory = memoryinformation.TotalPhysicalMemory.GetValue(SizeUnit.Gigabytes);
                if (totalMemory <= ramInGB)
                    return;

                var maxWorkingSetInBytes = (long)Size.ConvertToBytes(ramInGB, SizeUnit.Gigabytes);
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
                        Logger.Info($"Failed to set max working set to {ramInGB}, error code: {Marshal.GetLastWin32Error()}");
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
                Logger.Info($"Failed to set max working set to {ramInGB}GB, error code: {Marshal.GetLastWin32Error()}", e);
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
                var details = $"Cannot downgrade license from {_licenseStatus.Type.ToString()}" +
                              $"to {newLicenseStatus.Type.ToString()}!";
                licenseLimit = GenerateLicenseLimit(LimitType.Downgrade, details);
                return false;
            }

            var clusterSize = GetClusterSize();
            var maxClusterSize = _licenseStatus.MaxClusterSize;
            if (clusterSize > maxClusterSize)
            {
                var details = "Cannot activate licnese because the maximum allowed cluster size is: " +
                              $"{maxClusterSize} while the current cluster size is: {clusterSize}!";
                licenseLimit = GenerateLicenseLimit(LimitType.ClusterSize, details);
                return false;
            }

            var maxCores = newLicenseStatus.MaxCores;
            if (clusterSize > maxCores)
            {
                var details = "Cannot activate licnese because the cores limit is: " +
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
                var details = "Cannot activate licnese because there are " +
                              $"{encryptedDatabasesCount} encrypted database(s): " +
                              $"while the current license doesn't permit the usage of encryption!";
                licenseLimit = GenerateLicenseLimit(LimitType.Encryption, details);
                return false;
            }

            if (externalReplicationCount > 0 &&
                newLicenseStatus.HasExternalReplication == false)
            {
                var details = "Cannot activate licnese because there are " +
                              $"{externalReplicationCount} database(s) that use external replication " +
                              $"while the current license doesn't permit the usage of external replication!";
                licenseLimit = GenerateLicenseLimit(LimitType.ExternalReplication, details);
                return false;
            }

            if (ravenEtlCount > 0 &&
                newLicenseStatus.HasRavenEtl == false)
            {
                var details = "Cannot activate licnese because there are " +
                              $"{ravenEtlCount} database(s) that use Raven ETL " +
                              $"while the current license doesn't permit the usage of Raven ETL!";
                licenseLimit = GenerateLicenseLimit(LimitType.RavenEtl, details);
                return false;
            }

            if (sqlEtlCount > 0 &&
                newLicenseStatus.HasSqlEtl == false)
            {
                var details = "Cannot activate licnese because there are " +
                              $"{sqlEtlCount} database(s) that use SQL ETL " +
                              $"while the current license doesn't permit the usage of SQL ETL!";
                licenseLimit = GenerateLicenseLimit(LimitType.SqlEtl, details);
                return false;
            }

            if (snapshotBackupsCount > 0 &&
                newLicenseStatus.HasSnapshotBackups == false)
            {
                var details = "Cannot activate licnese because there are " +
                              $"{snapshotBackupsCount} database(s) that use snapshot bakcups " +
                              $"while the current license doesn't permit the usage of snapshot backups!";
                licenseLimit = GenerateLicenseLimit(LimitType.SnapshotBackup, details);
                return false;
            }

            if (cloudBackupsCount > 0 &&
                newLicenseStatus.HasCloudBackups == false)
            {
                var details = "Cannot activate licnese because there are " +
                              $"{cloudBackupsCount} database(s) that use cloud backups " +
                              $"while the current license doesn't permit the usage of cloud backups!";
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

        public bool CanAddNode(string nodeUrl, out LicenseLimit licenseLimit)
        {
            if (IsValid(out var invalidLicenseLimit) == false)
            {
                licenseLimit = invalidLicenseLimit;
                return false;
            }

            if (_licenseStatus.HasGlobalCluster == false &&
                IsValidLocalUrl(nodeUrl) == false)
            {
                var details = $"When running {GetLicenseString()}, " +
                              "only local nodes (which run on localhost, 127.*.*.* or [::1]) can be added to the cluster!";
                licenseLimit = GenerateLicenseLimit(LimitType.ForbiddenHost, details);
                return false;
            }

            var maxClusterSize = _licenseStatus.MaxClusterSize;
            var clusterSize = GetClusterSize();
            if (++clusterSize > maxClusterSize)
            {
                var details = $"You've reached the maximum number of allowed nodes in a cluster: {maxClusterSize}";
                licenseLimit = GenerateLicenseLimit(LimitType.ClusterSize, details);
                return false;
            }

            licenseLimit = null;
            return true;
        }

        public bool CanAddPeriodicBackup(BlittableJsonReaderObject readerObject, out LicenseLimit licenseLimit)
        {
            if (IsValid(out var invalidLicenseLimit) == false)
            {
                licenseLimit = invalidLicenseLimit;
                return false;
            }

            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var backupBlittable = readerObject.Clone(context);
                var configuration = JsonDeserializationCluster.PeriodicBackupConfiguration(backupBlittable);
                if (configuration.BackupType == BackupType.Snapshot && 
                    _licenseStatus.HasSnapshotBackups == false)
                {
                    const string details = "The license doesn't allow the creation of snapshot backups!";
                    licenseLimit = GenerateLicenseLimit(LimitType.SnapshotBackup, details);
                    return false;
                }

                var hasCloudBackup = configuration.HasCloudBackup();
                if (hasCloudBackup && _licenseStatus.HasCloudBackups == false)
                {
                    const string details = "The license doesn't allow uploading backups to cloud accounts!";
                    licenseLimit = GenerateLicenseLimit(LimitType.CloudBackup, details);
                    return false;
                }

                licenseLimit = null;
                return true;
            } 
        }

        public bool CanAddExternalReplication(ExternalReplication watcher, out LicenseLimit licenseLimit)
        {
            if (IsValid(out licenseLimit))
                return false;

            if (IsValidExternalReplication(_licenseStatus, watcher.Url) == false)
            {
                var details = $"When running {GetLicenseString()}, " +
                                       "only local URLs (which run on localhost, 127.*.*.* or [::1]) can be used for external replication!";
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
            if (IsValid(out var invalidLicenseLimit) == false)
            {
                licenseLimit = invalidLicenseLimit;
                return false;
            }

            if (IsValidRavenEtl(_licenseStatus, url) == false)
            {
                var details = $"When running {GetLicenseString()}, " +
                              "only local URLs (which run on localhost, 127.*.*.* or [::1]) can be used for Raven ETL!";
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
            if (IsValid(out var invalidLicenseLimit) == false)
            {
                licenseLimit = invalidLicenseLimit;
                return false;
            }

            if (_licenseStatus.HasSqlEtl == false)
            {
                const string details = "The license doesn't allow the usage of SQL ETL!";
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

            var result = _licenseStatus.Type == LicenseType.None;
            if (result == false)
            {
                const string details = "The license doesn't allow the usage of dynamic nodes distribution!";
                GenerateLicenseLimit(LimitType.DynamicNodeDistribution, details);
            }

            return result;
        }

        public bool CanCreateEncryptedDatabase(out LicenseLimit licenseLimit)
        {
            if (IsValid(out var invalidLicenseLimit) == false)
            {
                licenseLimit = invalidLicenseLimit;
                return false;
            }

            if (_licenseStatus.HasEncryption == false)
            {
                const string details = "The license doesn't allow the usage of encryption!";
                licenseLimit = GenerateLicenseLimit(LimitType.Encryption, details);
                return false;
            }

            licenseLimit = null;
            return true;
        }

        private LicenseLimit GenerateLicenseLimit(
            LimitType limitType, 
            string details, 
            bool addNotification = true)
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
                const string details = "Cannot perform operation while license is an invalid state!";
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

        private string GetLicenseString()
        {
            return _licenseStatus.Type == LicenseType.None ?
                "without a license" : $"using the {_licenseStatus.Type.ToString()} license";
        }
    }
}
