using System;

namespace Raven.Server.NotificationCenter.Alerts
{
    public static class AlertUtil
    {
        public static string CreateId(DatabaseAlertType type, string key)
        {
            return GetId(type, key);
        }

        public static string CreateId(ServerAlertType type, string key)
        {
            return GetId(type, key);
        }

        private static string GetId(Enum type, string key)
        {
            return string.IsNullOrEmpty(key) ? type.ToString() : $"{type}/{key}";
        }
    }
}