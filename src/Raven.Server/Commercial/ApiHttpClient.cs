using System;
using System.Globalization;
using System.Net.Http;
using System.Runtime.InteropServices;
using Raven.Server.ServerWide;

namespace Raven.Server.Commercial
{
    public static class ApiHttpClient
    {
        private const string ApiRavenDbNet = "https://api.ravendb.net";

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
                           $"{CultureInfo.CurrentUICulture.Name})";

            Instance.DefaultRequestHeaders.Add("User-Agent", userAgent);
        }
    }
}