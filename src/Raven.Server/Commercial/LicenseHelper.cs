using System;
using System.IO;
using System.Security;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using Newtonsoft.Json;
using Raven.Client.Exceptions.Commercial;
using Raven.Server.Config;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Commercial
{
    public class LicenseHelper
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
                if (RavenConfiguration.EnvironmentVariableLicenseString != null)
                {
                    // update the environment variable string value
                    UpdateEnvironmentVariableLicenseString(newLicense, rsaParameters);
                    return;
                }

                // update the license string in settings.json
                if (TryUpdatingSettingsJson(newLicense, rsaParameters))
                    return;

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
                var json = context.Read(stream, "license/json");
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
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failed to access the environment variable: `{RavenConfiguration.EnvironmentVariableLicenseString}`");
                return;
            }

            var oldLicense = DeserializeLicense(licenseString);
            if (ValidateLicense(newLicense, rsaParameters, oldLicense) == false)
                return;

            var newLicenseString = JsonConvert.SerializeObject(newLicense);
            Environment.SetEnvironmentVariable(RavenConfiguration.EnvironmentVariableLicenseString, newLicenseString, preferredTarget);

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

        private bool TryUpdatingSettingsJson(License newLicense, RSAParameters rsaParameters)
        {
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                BlittableJsonReaderObject settingsJson;
                using (var fs = new FileStream(_serverStore.Configuration.ConfigPath, FileMode.Open, FileAccess.Read))
                {
                    settingsJson = context.ReadForMemory(fs, "settings-json");
                }

                if (settingsJson.TryGet(LicenseStringConfigurationName, out string licenseString) == false)
                {
                    // validate the we have the license property
                    return false;
                }

                var oldLicense = DeserializeLicense(licenseString);
                if (ValidateLicense(oldLicense, rsaParameters, newLicense) == false)
                    return false;

                settingsJson.Modifications = new DynamicJsonValue(settingsJson)
                {
                    [LicenseStringConfigurationName] = JsonConvert.SerializeObject(newLicense)
                };

                var modifiedJsonObj = context.ReadObject(settingsJson, "modified-settings-json");

                var indentedJson = SetupManager.IndentJsonString(modifiedJsonObj.ToString());
                SetupManager.WriteSettingsJsonLocally(_serverStore.Configuration.ConfigPath, indentedJson);

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
    }
}
