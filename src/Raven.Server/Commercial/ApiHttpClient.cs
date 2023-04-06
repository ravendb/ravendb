using System;
using System.Globalization;
using System.Net.Http;
using System.Runtime.InteropServices;
using Raven.Server.ServerWide;
using Raven.Server.Utils;

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

        public static RavenHttpClient Instance;

        static ApiHttpClient()
        {
            Instance = new RavenHttpClient
            {
                BaseAddress = new Uri(ApiRavenDbNet)
            };
        }
    }
}
