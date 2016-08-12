namespace Raven.Abstractions.Util
{
    public static class ConfigurationManager
    {
        public static string GetAppSetting(string key)
        {
            return System.Configuration.ConfigurationManager.AppSettings[key];
        }
    }
}