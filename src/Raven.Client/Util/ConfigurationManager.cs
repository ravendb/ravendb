using Microsoft.Framework.ConfigurationModel;

namespace Raven.Abstractions.Util
{
    public static class ConfigurationManager
    {
        private static readonly Configuration configuration;

        static ConfigurationManager()
        {
            configuration = new Configuration();
        }

        public static string GetAppSetting(string key)
        {
            return configuration.Get(key);
        }
    }
}