using System;
using System.IO;
using System.Net.Http;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Server.Alerts;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.BackgroundTasks;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Commercial
{
    public static class LicenseManager
    {
        private const string ApiRavenDbNet = "https://api.ravendb.net";

        private static readonly LicenseStatus LicenseStatus = new LicenseStatus();
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<LatestVersionCheck>("Server");
        private static readonly HttpClient HttpClient = new HttpClient
        {
            BaseAddress = new Uri(ApiRavenDbNet)
        };

        //private Timer _leaseLicenseTimer = new Timer((state) =>
        //        AsyncHelpers.RunSync(LeaseLicense), null, 0, (int)TimeSpan.FromHours(24).TotalMilliseconds);

        private static readonly object LeaseLicenseLock = new object();

        private static RSAParameters? _rsaParameters;
        
        private static RSAParameters RSAParameters
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

        public static void Initialize(ServerStore serverStore)
        {
            var firstServerStartDate = serverStore.LicenseStorage.GetFirstServerStartDate();
            if (firstServerStartDate == null)
            {
                firstServerStartDate = SystemTime.UtcNow;
                serverStore.LicenseStorage.SetFirstServerStartDate(firstServerStartDate.Value);
            }

            LicenseStatus.FirstServerStartDate = firstServerStartDate.Value;

            var license = serverStore.LicenseStorage.LoadLicense();
            if (license == null)
                return;

            try
            {
                LicenseStatus.Attributes = LicenseValidator.Validate(license, RSAParameters);
                LicenseStatus.Error = false;
                LicenseStatus.Message = null;
            }
            catch (Exception e)
            {
                LicenseStatus.Attributes = null;
                LicenseStatus.Error = true;
                LicenseStatus.Message = e.Message;

                if (Logger.IsInfoEnabled)
                    Logger.Info("Could not validate license. License details", e);

                throw new InvalidDataException("Could not validate license!");
            }
        }

        public static LicenseStatus GetLicenseStatus()
        {
            return LicenseStatus;
        }

        public static async Task RegisterForFreeLicense(UserRegistrationInfo userInfo)
        {
            var response = await HttpClient.PostAsync("api/v1/license/register",
                    new StringContent(JsonConvert.SerializeObject(userInfo), Encoding.UTF8, "application/json"))
                .ConfigureAwait(false);

            var responseString = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
            if (response.IsSuccessStatusCode == false)
            {
                throw new InvalidOperationException("Registration failed with status code: " + response.StatusCode +
                                                    Environment.NewLine + responseString);
            }
        }

        public static void Activate(License license)
        {
            try
            {
                LicenseStatus.Attributes = LicenseValidator.Validate(license, RSAParameters);
                LicenseStatus.Error = false;
                LicenseStatus.Message = null;
            }
            catch (Exception e)
            {
                LicenseStatus.Attributes = null;
                LicenseStatus.Error = true;
                LicenseStatus.Message = e.Message;

                var message = $"Could not validate the following license:{Environment.NewLine}" +
                              $"Id: {license.Id}{Environment.NewLine}" +
                              $"Name: {license.Name}{Environment.NewLine}" +
                              $"Keys: [{string.Join(", ", license.Keys)}]";

                if (Logger.IsInfoEnabled)
                    Logger.Info(message, e);

                throw new InvalidDataException("Could not validate license!", e);
            }
        }

        private static void LeaseLicense()
        {
            var lockTaken = false;
            try
            {
                Monitor.TryEnter(LeaseLicenseLock, ref lockTaken);
                if (lockTaken == false)
                    return;

                //TODO: implement this (grisha)
                /*using (var content = new StreamContent(stream))
                {
                    var response = await _httpClient.PostAsync("/api/license/lease", content).ConfigureAwait(false);
                    if (response.IsSuccessStatusCode == false)
                        throw new InvalidOperationException("Import failed with status code: " + response.StatusCode +
                                                            Environment.NewLine +
                                                            await response.Content.ReadAsStringAsync()
                        );

                    if (response.IsSuccessStatusCode)
                    {
                        var x = await response.Content.ReadAsStringAsync();
                    }
                }*/
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Error getting latest version info.", e);
            }
            finally
            {
                if (lockTaken)
                    Monitor.Exit(LeaseLicenseLock);
            }
        }

        public class InitializationErrorAlertContent : IAlertContent
        {
            public InitializationErrorAlertContent(Exception e)
            {
                Exception = e;
            }

            public Exception Exception { get; set; }
            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue(GetType())
                {
                    [nameof(Exception)] = Exception.ToString()
                };
            }

            public static string FormatMessage()
            {
                return $@"
            <h3>License manager initialization error!</h3>
            <p>Could not intitalize the license manager</p>";
            }
        }
    }
}
