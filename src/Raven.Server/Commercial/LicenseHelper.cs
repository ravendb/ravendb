using System;
using System.IO;
using System.Security;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Runtime.Internal.Util;
using Newtonsoft.Json;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.Properties;
using Raven.Server.Commercial.LetsEncrypt;
using Raven.Server.Config;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server.Json.Sync;
using Voron;
using Logger = Sparrow.Logging.Logger;

namespace Raven.Server.Commercial
{
    public sealed class LicenseHelper
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<LicenseHelper>("Server");
        public static readonly string LicenseStringConfigurationName = RavenConfiguration.GetKey(x => x.Licensing.License);

        private readonly ServerStore _serverStore;
        private readonly SemaphoreSlim _sm = new SemaphoreSlim(1, 1);

        public LicenseHelper(ServerStore serverStore)
        {
            _serverStore = serverStore;
        }

        public void UpdateLocalLicense(License newLicense, RSAParameters rsaParameters)
        {
            if (_sm.Wait(0) == false)
                return;

            try
            {
                if (TryGetLicenseFromString(throwOnFailure: false) != null)
                {
                    UpdateLicenseString(newLicense, rsaParameters);
                    return;
                }

                var licenseFromPath = TryGetLicenseFromPath(throwOnFailure: false);
                if (licenseFromPath != null)
                {
                    UpdateLicenseFromPath(licenseFromPath, newLicense, rsaParameters);
                }
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Failed to update the license locally", e);
            }
            finally
            {
                _sm.Release();
            }
        }

        public License TryGetLicenseFromString(bool throwOnFailure)
        {
            var licenseString = _serverStore.Configuration.Licensing.License;
            if (string.IsNullOrWhiteSpace(licenseString))
                return null;

            try
            {
                return DeserializeLicense(licenseString);
            }
            catch (Exception e)
            {
                var msg = $"Failed to read license from '{LicenseStringConfigurationName}' configuration.";

                if (Logger.IsInfoEnabled)
                    Logger.Info(msg, e);

                if (throwOnFailure)
                    throw new LicenseActivationException(msg, e);
            }

            return null;
        }

        public License TryGetLicenseFromPath(bool throwOnFailure)
        {
            var path = _serverStore.Configuration.Licensing.LicensePath;
            if (File.Exists(path.FullPath) == false)
                return null;

            try
            {
                using (var stream = File.Open(path.FullPath, FileMode.Open, FileAccess.Read, FileShare.Read))
                {
                    return DeserializeLicense(stream);
                }
            }
            catch (Exception e)
            {
                var msg = $"Failed to read license from '{path.FullPath}' path.";

                if (Logger.IsInfoEnabled)
                    Logger.Info(msg, e);

                if (throwOnFailure)
                    throw new LicenseActivationException(msg, e);
            }

            return null;
        }

        private static License DeserializeLicense(Stream stream)
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var json = context.Sync.ReadForMemory(stream, "license/json");
                return JsonDeserializationServer.License(json);
            }
        }

        private static License DeserializeLicense(string licenseString)
        {
            using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(licenseString)))
            {
                return DeserializeLicense(stream);
            }
        }

        internal static bool TryDeserializeLicense(string licenseString, out License license)
        {
            try
            {
                license = DeserializeLicense(licenseString);
                return true;
            }
            catch
            {
                license = null;
                return false;
            }
        }

        private void UpdateLicenseString(License newLicense, RSAParameters rsaParameters)
        {
            // try updating the license string in settings.json (if it exists)
            if (TryUpdatingSettingsJson(newLicense, rsaParameters))
                return;

            if (RavenConfiguration.EnvironmentVariableLicenseString != null)
            {
                // update the environment variable string value
                UpdateEnvironmentVariableLicenseString(newLicense, rsaParameters);
            }
        }

        private static void UpdateEnvironmentVariableLicenseString(License newLicense, RSAParameters rsaParameters)
        {
            var preferredTarget = EnvironmentVariableTarget.Machine;
            var licenseString = GetLicenseString();
            if (licenseString == null)
            {
                preferredTarget = EnvironmentVariableTarget.User;
                licenseString = GetLicenseString();
            }

            if (licenseString == null)
                return;

            if (TryDeserializeLicense(licenseString, out var oldLicense) == false)
                return;

            if (ValidateLicense(newLicense, rsaParameters, oldLicense) == false)
                return;

            try
            {
                var newLicenseString = JsonConvert.SerializeObject(newLicense);
                Environment.SetEnvironmentVariable(RavenConfiguration.EnvironmentVariableLicenseString, newLicenseString, preferredTarget);
            }
            catch (SecurityException)
            {
                // expected
            }

            string GetLicenseString()
            {
                try
                {
                    return Environment.GetEnvironmentVariable(RavenConfiguration.EnvironmentVariableLicenseString, preferredTarget);
                }
                catch (SecurityException)
                {
                    return null;
                }
            }
        }

        private static bool ValidateLicense(License oldLicense, RSAParameters rsaParameters, License newLicense)
        {
            try
            {
                LicenseValidator.Validate(oldLicense, rsaParameters);
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failed to validate license: `{oldLicense}`", e);

                return true;
            }

            if (oldLicense.Id != newLicense.Id)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Can't update license because the new license ID is: {newLicense.Id} " +
                                $"while old license ID is: {oldLicense.Id}");

                return false;
            }

            return true;
        }

        // returns true when this setting exists in settings.json
        private bool TryUpdatingSettingsJson(License newLicense, RSAParameters rsaParameters)
        {
            if (File.Exists(_serverStore.Configuration.ConfigPath) == false)
                return false;

            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                BlittableJsonReaderObject settingsJson;
                using (var fs = new FileStream(_serverStore.Configuration.ConfigPath, FileMode.Open, FileAccess.Read))
                {
                    settingsJson = context.Sync.ReadForMemory(fs, "settings-json");
                }

                // validate that we have the license property
                if (settingsJson.TryGet(LicenseStringConfigurationName, out string licenseString) == false)
                    return false;

                if (TryDeserializeLicense(licenseString, out var oldLicense) == false)
                    return true;

                if (ValidateLicense(oldLicense, rsaParameters, newLicense) == false)
                    return true;

                settingsJson.Modifications = new DynamicJsonValue(settingsJson)
                {
                    [LicenseStringConfigurationName] = JsonConvert.SerializeObject(newLicense)
                };

                var modifiedJsonObj = context.ReadObject(settingsJson, "modified-settings-json");

                var indentedJson = JsonStringHelper.Indent(modifiedJsonObj.ToString());
                SettingsZipFileHelper.WriteSettingsJsonLocally(_serverStore.Configuration.ConfigPath, indentedJson);

                return true;
            }
        }

        private void UpdateLicenseFromPath(License oldLicense, License newLicense, RSAParameters rsaParameters)
        {
            if (ValidateLicense(oldLicense, rsaParameters, newLicense) == false)
                return;

            var licenseString = JsonConvert.SerializeObject(newLicense, Formatting.Indented);
            File.WriteAllText(_serverStore.Configuration.Licensing.LicensePath.FullPath, licenseString);
        }

        internal static bool TryValidateLicenseExpirationDate(License license, out DateTime expirationDate)
        {
            expirationDate = DateTime.MinValue;

            if (license == null)
                return false;

            var licenseStatus = LicenseManager.GetLicenseStatus(license);
            if (licenseStatus.Expiration.HasValue)
                expirationDate = licenseStatus.Expiration.Value;

            return expirationDate >= RavenVersionAttribute.Instance.ReleaseDate;
        }

        internal static void ValidateLicenseVersionOrThrow(License license, ServerStore serverStore, TransactionContextPool contextPool, bool usingApi = false)
        {
            var licenseStatus = LicenseManager.GetLicenseStatus(license);
            if (licenseStatus.Version.Major >= 6 || licenseStatus.IsCloud)
                return;

            if (usingApi && serverStore.Server.Configuration.Licensing.DisableAutoUpdateFromApi == false)
            {
                var licenseFromApi = AsyncHelpers.RunSync(() => GetLicenseFromApi(license, contextPool, serverStore.ServerShutdown));
                if (licenseFromApi != null)
                {
                    licenseStatus = LicenseManager.GetLicenseStatus(licenseFromApi);
                    if (licenseStatus.Version.Major >= 6)
                    {
                        serverStore.LicenseManager.OnBeforeInitialize += () =>
                            AsyncHelpers.RunSync(() =>
                                serverStore.LicenseManager.TryActivateLicenseAsync(throwOnActivationFailure: serverStore.Server.ThrowOnLicenseActivationFailure));
                        return;
                    }
                }
            }

            var msg =
                $"Your license ('{licenseStatus.Id}') version '{licenseStatus.Version}' doesn't allow you to upgrade to server version '{RavenVersionAttribute.Instance.FullVersion}'. " +
                $"Please proceed to the https://ravendb.net/l/8O2YU1 website to perform the license upgrade first. ";

            if (usingApi)
            {
                if (serverStore.Server.Configuration.Licensing.DisableAutoUpdateFromApi == false)
                    msg += $"After the upgrade, if your server has access to {ApiHttpClient.ApiRavenDbNet}, your license will be automatically updated. ";
                else
                    msg += $"Please note that automatic license updates from {ApiHttpClient.ApiRavenDbNet} are disabled due to the '{RavenConfiguration.GetKey(x => x.Licensing.DisableAutoUpdateFromApi)}' option being set to 'true'. ";
            }

            msg += $"To update your license, you have the following options:{Environment.NewLine}" +
                   $"1. Update the license via the configuration option '{RavenConfiguration.GetKey(x => x.Licensing.LicensePath)}' or '{RavenConfiguration.GetKey(x => x.Licensing.License)}'.{Environment.NewLine}" +
                   $"2. Downgrade to the previous version of RavenDB, apply the new license, and then continue the update procedure.";

            if (usingApi == false)
                throw new LicenseLimitException(msg);

            if (serverStore.Server.Configuration.Licensing.DisableAutoUpdateFromApi == false)
            {
                msg += $"{Environment.NewLine}3. Ensure your server has access to {ApiHttpClient.ApiRavenDbNet} for automatic license updates.";
            }
            else
            {
                msg += $"{Environment.NewLine}3. If you want to enable automatic license updates:{Environment.NewLine}" +
                       $"   a. Set the '{RavenConfiguration.GetKey(x => x.Licensing.DisableAutoUpdateFromApi)}' option to 'false' in your server configuration.{Environment.NewLine}" +
                       $"   b. Ensure your server has access to {ApiHttpClient.ApiRavenDbNet}.{Environment.NewLine}" +
                       $"   c. Start the server again with the updated configuration.";
            }

            throw new LicenseLimitException(msg);
        }

        private static async Task<License> GetLicenseFromApi(License license, TransactionContextPool contextPool, CancellationToken token)
        {
            try
            {
                var response = await LicenseManager.GetUpdatedLicenseResponseMessage(license, contextPool, token)
                    .ConfigureAwait(false);
                var leasedLicense = await LicenseManager.ConvertResponseToLeasedLicense(response, token)
                    .ConfigureAwait(false);
                return leasedLicense.License;
            }
            catch
            {
                return null;
            }
        }

        public static bool TryValidateAndHandleLicense(ServerStore serverStore, string licenseJson, Guid? inStorageLicenseId, LicenseVerificationErrorBuilder verificationErrorBuilder, TransactionContextPool contextPool)
        {
            if (TryDeserializeLicense(licenseJson, out var deserializedLicense) == false)
            {
                verificationErrorBuilder.AppendDeserializationErrorMessage(licenseJson);
            }
            else
            {
                if (TryValidateLicenseExpirationDate(deserializedLicense, out var deserializedLicenseExpirationDate))
                {
                    ValidateLicenseVersionOrThrow(deserializedLicense, serverStore, contextPool);
                    serverStore.LicenseManager.OnBeforeInitialize += () => serverStore.LicenseManager.TryActivateLicenseAsync(throwOnActivationFailure: serverStore.Server.ThrowOnLicenseActivationFailure).Wait(serverStore.ServerShutdown);
                    return true;
                }

                verificationErrorBuilder.AppendConfigurationLicenseExpiredMessage(inStorageLicenseId, deserializedLicense.Id, deserializedLicenseExpirationDate);
            }

            return false;
        }

        public class LicenseVerificationErrorBuilder
        {
            private readonly RavenConfiguration _configuration;
            private readonly StorageEnvironment _storageEnvironment;
            private readonly TransactionContextPool _contextPool;
            private readonly StringBuilder _errorBuilder = new();
            private bool _isInStorageLicenseExpired;
            private string _configurationKeyInAction;

            public LicenseVerificationErrorBuilder(RavenConfiguration configuration, StorageEnvironment storageEnvironment, TransactionContextPool contextPool)
            {
                _configuration = configuration;
                _storageEnvironment = storageEnvironment;
                _contextPool = contextPool;
            }

            public LicenseVerificationErrorBuilder()
            {
            }

            public void AppendInStorageLicenseExpiredMessage(DateTime expirationDate)
            {
                _isInStorageLicenseExpired = true;

                _errorBuilder.AppendLine("The RavenDB server cannot start due to an expired license. Please review the details below to resolve the issue:");
                _errorBuilder.AppendLine($"- License Expiration Date: {FormattedDateTime(expirationDate)}");
                _errorBuilder.AppendLine($"- Server Version Release Date: {FormattedDateTime(RavenVersionAttribute.Instance.ReleaseDate)}");
            }

            public void AppendLicenseMissingMessage()
            {
                _errorBuilder.AppendLine("The RavenDB server cannot start due to a missing license. Please review the details below to resolve the issue:");
            }

            public void AppendConfigurationKeyUsageAttempt(string configurationKey)
            {
                _configurationKeyInAction = configurationKey;

                _errorBuilder.AppendLine();
                _errorBuilder.AppendLine($"We attempted to obtain a valid license using the configuration key '{configurationKey}', but this process was not successful for the following reason:");
            }

            public void AppendFileReadErrorMessage(Exception e)
            {
                _errorBuilder.AppendLine("- An error occurred while trying to read the license from the file:");
                _errorBuilder.AppendLine($"  {e.Message}");
            }

            public void AppendResolutionSuggestions()
            {
                AppendGeneralSuggestions();

                // We can suggest a downgrade only if in-storage license is expired
                if (_isInStorageLicenseExpired)
                {
                    // Getting build number from the license storage, just in case the license is expired and we have ability to downgrade
                    var licenseStorage = new LicenseStorage();
                    licenseStorage.Initialize(_storageEnvironment, _contextPool);
                    var buildInfo = licenseStorage.GetBuildInfo();

                    if (buildInfo != null)
                        _errorBuilder.AppendLine($"- As a temporary measure, consider downgrading to the last working build ({buildInfo.FullVersion}).");
                }

                AppendSuggestionToDisableThrowOnInvalidOrMissingLicenseOption(_configuration.Licensing.ThrowOnInvalidOrMissingLicense, _isInStorageLicenseExpired);
            }

            public void AppendGeneralSuggestions()
            {
                _errorBuilder.AppendLine();
                _errorBuilder.AppendLine("To resolve this issue, you may consider the following options:");
                _errorBuilder.AppendLine("- Ensure your license key is correctly embedded in 'settings.json', set as an environment variable, or included in your 'ServerOptions' if using an embedded server or Raven.TestDriver.");
                _errorBuilder.AppendLine("- Alternatively, check the 'License.Path' in your configuration to ensure it points to a valid 'license.json' file.");
            }

            public void AppendConfigurationLicenseExpiredMessage(Guid? inStorageLicenseId, Guid deserializedLicenseId, DateTime deserializedLicenseExpirationDate)
            {
                _errorBuilder.AppendLine(deserializedLicenseId == inStorageLicenseId
                    ? "- The license obtained matches the in-storage license but is also expired."
                    : $"- The license '{deserializedLicenseId}' obtained from '{_configurationKeyInAction}' has an expiration date of '{FormattedDateTime(deserializedLicenseExpirationDate)}' and is also expired.");
            }

            public void AppendDeserializationErrorMessage(string licenseContent)
            {
                if (string.IsNullOrWhiteSpace(licenseContent))
                    _errorBuilder.AppendLine("- The license is not provided in the configuration or environment variable.");
                else
                    _errorBuilder.AppendLine($"- Could not parse the license content: '{licenseContent}'.");
            }

            public void AppendSuggestionToDisableThrowOnInvalidOrMissingLicenseOption(bool throwOnInvalidOrMissingLicenseOptionEnabled, bool isInStorageLicenseExpired)
            {
                if (throwOnInvalidOrMissingLicenseOptionEnabled && isInStorageLicenseExpired == false)
                    _errorBuilder.AppendLine($"- Configure the '{RavenConfiguration.GetKey(x => x.Licensing.ThrowOnInvalidOrMissingLicense)}' option by setting it to 'False' to disable this strict licensing requirement for server startup.");
            }

            public override string ToString() => _errorBuilder.ToString();

            private static string FormattedDateTime(DateTime dateTime) => dateTime.ToString("dd MMMM yyyy");
        }
    }
}
