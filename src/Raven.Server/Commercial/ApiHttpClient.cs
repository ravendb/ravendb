using System;
using System.Globalization;
using System.Net.Http;
using System.Runtime.InteropServices;
using Raven.Server.ServerWide;

namespace Raven.Server.Commercial
{
    public static class ApiHttpClient
    {
        public static string ApiRavenDbNet 
        {
            get
            {
                var envValue = Environment.GetEnvironmentVariable("RAVEN_API_ENV");
                
                if (string.IsNullOrWhiteSpace(envValue) == false)
                {
                    return $"https://{envValue}.api.ravendb.net";
                }
                
                return "https://api.ravendb.net";
            }
        }

        public static HttpClient Instance;

        static ApiHttpClient()
        {
            Instance = new HttpClient
            {
                BaseAddress = new Uri(ApiRavenDbNet)
            };

            var userAgent = $"RavenDB/{ServerVersion.Version} (" +
                            $"{RuntimeInformation.OSDescription};" +
                            $"{RuntimeInformation.OSArchitecture};" +
                            $"{RuntimeInformation.FrameworkDescription};" +
                            $"{RuntimeInformation.ProcessArchitecture};" +
                            $"{CultureInfo.CurrentCulture.Name};" +
                            $"{CultureInfo.CurrentUICulture.Name};" +
                            $"{ServerVersion.FullVersion})";

            Instance.DefaultRequestHeaders.Add("User-Agent", userAgent);
        }
    }
}
