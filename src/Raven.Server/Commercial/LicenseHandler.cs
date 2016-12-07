using System;
using System.IO;
using System.Net.Http;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Imports.Newtonsoft.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.BackgroundTasks;
using Sparrow.Logging;

namespace Raven.Server.Commercial
{
    public class LicenseHandler : IDisposable
    {
        private const string ApiRavenDbNet = "http://api.ravendb.net"; //TODO: change to https
        private static readonly LicenseStatus LicenseStatus = new LicenseStatus();
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<LatestVersionCheck>(null);
        private ServerStore _serverStore;
        private DateTime FirstServerStartDate;
        private static HttpClient _httpClient;
        //private Timer _leaseLicenseTimer;
        private readonly object _leaseLicenseLock = new object();

        private static RSAParameters? _rsaParameters;
        
        private static RSAParameters RSAParameters
        {
            get
            {
                if (_rsaParameters != null)
                    return _rsaParameters.Value;

                string publicKeyString;
                const string publicKeyPath = "Raven.Server.Commercial.RavenDB.public.json";
                using (var stream = typeof(LicenseHandler).GetTypeInfo().Assembly.GetManifestResourceStream(publicKeyPath))
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

        public LicenseHandler(ServerStore serverStore)
        {
            _serverStore = serverStore;
            _httpClient = new HttpClient
            {
                BaseAddress = new Uri(ApiRavenDbNet)
            };

            /*_leaseLicenseTimer = new Timer((state) =>
                AsyncHelpers.RunSync(LeaseLicense), null, 0, (int)TimeSpan.FromHours(24).TotalMilliseconds);*/
        }

        public void Initialize()
        {
            var firstServerStartDate = _serverStore.LicenseStorage.GetFirstServerStartDate();
            if (firstServerStartDate == null)
            {
                firstServerStartDate = DateTime.UtcNow;
                _serverStore.LicenseStorage.SetFirstServerStartDate(firstServerStartDate.Value);
            }

            FirstServerStartDate = firstServerStartDate.Value;

            var license = _serverStore.LicenseStorage.LoadLicense();
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
                    Logger.Info("Could not validate license", e);

                throw new InvalidDataException("Could not validate license!");
            }
        }

        public static LicenseStatus GetLicenseStatus()
        {
            return LicenseStatus;
        }

        public static async Task Register(RegisteredUserInfo registeredUserInfo)
        {
            var response = await _httpClient.PostAsync("api/v1/license/register",
                    new StringContent(JsonConvert.SerializeObject(registeredUserInfo), Encoding.UTF8, "application/json"))
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

                if (Logger.IsInfoEnabled)
                    Logger.Info("Could not validate license", e);

                throw new InvalidDataException("Could not validate license!");
            }
        }

        private void LeaseLicense()
        {
            var lockTaken = false;
            try
            {
                Monitor.TryEnter(_leaseLicenseLock, ref lockTaken);
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
                    Monitor.Exit(_leaseLicenseLock);
            }
        }

        public void Dispose()
        {
            //_leaseLicenseTimer.Dispose();
        }
    }
}
