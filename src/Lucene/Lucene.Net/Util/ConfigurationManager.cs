#if NETSTANDARD2_1
using Microsoft.Extensions.Configuration;

#endif

namespace Lucene.Net.Util
{
    internal static class ConfigurationManager
    {
#if NETSTANDARD2_1
        private static readonly IConfigurationRoot configuration;

        static ConfigurationManager()
        {
            var builder = new ConfigurationBuilder().AddJsonFile("appsettings.json", optional: true);
            configuration = builder.Build();
        }
#endif

        public static string GetAppSetting(string key)
        {
#if NETSTANDARD2_1
            return configuration[key];
#else
            return System.Configuration.ConfigurationManager.AppSettings[key];
#endif
        }
    }
}