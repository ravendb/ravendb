#if DNXCORE50
using Microsoft.Framework.ConfigurationModel;
#endif

namespace Raven.Abstractions.Util
{
    public static class ConfigurationManager
    {
#if DNXCORE50
        private static readonly Configuration configuration;

        static ConfigurationManager()
        {
            configuration = new Configuration();
        }
#endif

        public static string GetAppSetting(string key)
        {
#if DNXCORE50
            return configuration.Get(key);
#else
            return System.Configuration.ConfigurationManager.AppSettings[key];
#endif
        }
    }
}