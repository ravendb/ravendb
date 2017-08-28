using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Rachis;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;
using Voron;

namespace Raven.Server.Commercial
{
    public class LicenseManager : IDisposable
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<LicenseManager>("Server");
        private readonly LicenseStorage _licenseStorage = new LicenseStorage();
        private readonly LicenseStatus _licenseStatus = new LicenseStatus();
        private readonly BuildNumber _buildInfo;
        private Timer _leaseLicenseTimer;
        private long _lastSavedLicenseIndex;
        private RSAParameters? _rsaParameters;
        private readonly ServerStore _serverStore;
        private readonly SemaphoreSlim _semaphoreSlim = new SemaphoreSlim(1);

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

                var license = _serverStore.LoadLicense();
                if (license == null)
                    return;

                _leaseLicenseTimer = new Timer(state =>
                    AsyncHelpers.RunSync(LeaseLicense), null, 0, (int)TimeSpan.FromHours(24).TotalMilliseconds);

                _licenseStatus.Attributes = LicenseValidator.Validate(license, RSAParameters);
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

        public LicenseStatus GetLicenseStatus()
        {
            var lastLicenseIndex = _serverStore.LastLicenseIndex;
            if (lastLicenseIndex > _lastSavedLicenseIndex)
            {
                _lastSavedLicenseIndex = lastLicenseIndex;
                ReloadLicense();
            }

            return _licenseStatus;
        }

        private void ReloadLicense()
        {
            var license = _serverStore.LoadLicense();
            if (license == null)
            {
                _licenseStatus.Attributes = null;
                _licenseStatus.Error = false;
                _licenseStatus.Message = null;
                return;
            }

            try
            {
                _licenseStatus.Attributes = LicenseValidator.Validate(license, RSAParameters);
                _licenseStatus.Error = false;
                _licenseStatus.Message = null;
            }
            catch (Exception e)
            {
                _licenseStatus.Attributes = null;
                _licenseStatus.Error = true;
                _licenseStatus.Message = e.Message;
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
            if (_serverStore.CurrentState == RachisConsensus.State.Passive)
                throw new InvalidOperationException(
                    "Couldn't activate license because this node is passive" +
                    "Passive nodes aren't members of a cluster and require admin action (such as creating a db) " +
                    "to indicate that this node should create its own cluster");

            try
            {
                _licenseStatus.Attributes = LicenseValidator.Validate(license, RSAParameters);
                var licenseExpiration = _licenseStatus.Expiration;

                if (licenseExpiration.HasValue == false)
                {
                    _licenseStatus.Attributes = null;
                    throw new LicenseExpiredException(@"License doesn't have an expirtaion date!");
                }

                if (licenseExpiration < DateTime.UtcNow)
                {
                    _licenseStatus.Attributes = null;
                    throw new LicenseExpiredException($"License already expired on: {licenseExpiration}");
                }

                _licenseStatus.Error = false;
                _licenseStatus.Message = null;

                _lastSavedLicenseIndex = await _serverStore.PutLicense(license);
                if (skipLeaseLicense == false)
#pragma warning disable 4014
                    Task.Run(LeaseLicense);
#pragma warning restore 4014
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

        private async Task LeaseLicense()
        {
            if (_serverStore.CurrentState != RachisConsensus.State.Leader)
            {
                // only the leader is in charge of updating the license
                return;
            }

            if (_semaphoreSlim.Wait(0) == false)
                return;

            try
            {
                var license = _serverStore.LoadLicense();
                if (license == null)
                    return;

                var leaseLicenseInfo = new LeaseLicenseInfo
                {
                    License = license,
                    BuildInfo = _buildInfo
                };

                var response = await ApiHttpClient.Instance.PostAsync("/api/v1/license/lease",
                        new StringContent(JsonConvert.SerializeObject(leaseLicenseInfo), Encoding.UTF8, "application/json"))
                    .ConfigureAwait(false);

                if (response.IsSuccessStatusCode == false)
                {
                    await HandleLeaseLicenseFailure(response).ConfigureAwait(false);
                    return;
                }

                var licenseAsStream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var json = context.Read(licenseAsStream, "leased license");
                    var newLicense = JsonDeserializationServer.License(json);
                    if (newLicense.Name == license.Name && newLicense.Id == license.Id &&
                        newLicense.Keys.SequenceEqual(license.Keys))
                        return;

                    await Activate(newLicense, skipLeaseLicense: true);
                }

                var message = "License was updated";
                if (_licenseStatus.Attributes.TryGetValue("expiration", out object expiration) && expiration is DateTime)
                {
                    var expirationDate = ((DateTime)expiration).ToString("yyyy-MMM-dd", CultureInfo.CurrentUICulture);
                    message += $", new expiration date is {expirationDate}";
                }

                var alert = AlertRaised.Create(
                    "License updated",
                    message,
                    AlertType.LicenseManager_LicenseUpdated,
                    NotificationSeverity.Info);

                _serverStore.NotificationCenter.Add(alert);
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
                _semaphoreSlim.Release();
            }
        }

        private async Task HandleLeaseLicenseFailure(HttpResponseMessage response)
        {
            var responseString = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
            if (response.StatusCode == HttpStatusCode.ExpectationFailed)
            {
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

        public void Dispose()
        {
            _leaseLicenseTimer?.Dispose();
        }
    }
}
