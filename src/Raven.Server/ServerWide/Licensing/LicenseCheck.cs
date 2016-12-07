using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Imports.Newtonsoft.Json;
using Raven.Server.ServerWide.BackgroundTasks;
using Sparrow.Logging;

namespace Raven.Server.ServerWide.Licensing
{
    public class LicenseCheck : IDisposable
    {
        private const string ApiRavenDbNet = "https://api.ravendb.net";
        private static readonly LicenseStatus LicenseStatus = new LicenseStatus();
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<LatestVersionCheck>(null);
        private static HttpClient _httpClient;
        //private Timer _leaseLicenseTimer;
        private readonly ServerStore _serverStore;
        private readonly object _leaseLicenseLock = new object();

        private static RSAParameters? _rsaParameters;

        private static RSAParameters RSAParameters
        {
            get
            {
                if (_rsaParameters != null)
                    return _rsaParameters.Value;

                string publicKeyString;
                const string publicKeyPath = "Raven.Server.ServerWide.Licensing.RavenDB.public.json";
                using (var stream = typeof(LicenseCheck).GetTypeInfo().Assembly.GetManifestResourceStream(publicKeyPath))
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

        public LicenseCheck(ServerStore serverStore)
        {
            _serverStore = serverStore;

            _httpClient = new HttpClient
            {
                BaseAddress = new Uri(ApiRavenDbNet)
            };

            /*_leaseLicenseTimer = new Timer((state) =>
                AsyncHelpers.RunSync(LeaseLicense), null, 0, (int)TimeSpan.FromHours(24).TotalMilliseconds);*/
            /*AsyncHelpers.RunSync(() => Register(new RegisteredUserInfo
            {
                Name = "Grisha",
                Email = "Test@test.com"
            }));*/
            Activate(new License()
            {
                Id = Guid.Parse("629a858a-de08-44ff-9107-eba8b3688e66"),
                Name = "Hibernating Rhinos",
                Keys = new List<string>()
                {
                    "YTFKQgFDA0QMRQYGB0gACSoLLC0uL1AAMTITdDFK",
                    "6vQZDJ6ig0WONk9m7jZPazmKGtOtMtdFCunXHXRI5g8iORRfs7PvzVfXWepXEChMSf3kTjTj74lq922ugNaBouorNVirQdNoGmtlZv1h7YXQFyGDLSaZbHhUEmD6pneExKQVJVg6U1QYd+wuxHuP6SoMm1eWKa5c02zfk2clEiQ="
                }
            });
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
                                                    Environment.NewLine + responseString
                );
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
