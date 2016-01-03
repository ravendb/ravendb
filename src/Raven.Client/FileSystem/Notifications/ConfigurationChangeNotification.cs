namespace Raven.Abstractions.FileSystem.Notifications
{
    public class ConfigurationChangeNotification : FileSystemNotification
    {
        public string Name { get; set; }

        public ConfigurationChangeAction Action { get; set; }
    }

    public enum ConfigurationChangeAction
    {
        Set,
        Delete,
    }
}
