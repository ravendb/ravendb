namespace Raven.Abstractions.FileSystem.Notifications
{
    public class ConfigurationChange : FileSystemChange
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
