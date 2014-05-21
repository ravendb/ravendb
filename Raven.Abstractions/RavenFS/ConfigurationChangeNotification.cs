namespace Raven.Client.RavenFS
{
	public class ConfigurationChangeNotification : Notification
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